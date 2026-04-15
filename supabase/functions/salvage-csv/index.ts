import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import * as XLSX from "https://esm.sh/xlsx@0.18.5";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

// Master tab placeholder columns (Module 10 will populate the rows).
const MASTER_COLUMNS = [
  "Patient Name", "DOB", "Address", "Phone", "Email", "First Visit", "Last Visit", "Total Visits",
  "SOAP Notes", "SOAP PDF Link", "Total Charged", "Total Paid", "Total Adjustments", "Current Balance",
  "Top CPT Codes", "Top ICD Codes", "All CPT Codes",
];

// Hard-coded canonical Financials column order (matches Module 8 parser output).
// Row ordering is now stable regardless of which row the JSON happens to land first.
const FINANCIALS_COLUMNS = [
  "PatientName","DOB","Date","Type","CPTCode","ICDCodes",
  "Description","Charges","Payments","Adjustments","Balance","Notes","Status",
];

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const serviceClient = createClient(supabaseUrl, serviceKey);
    const { jobId } = await req.json();
    if (!jobId) throw new Error("jobId required");

    const { data: jobData } = await serviceClient.from("scrape_jobs").select("user_id").eq("id", jobId).single();
    if (!jobData) throw new Error("Job not found");
    const userId = jobData.user_id;

    const bucket = "scraped-data";
    const tmpPrefix = `_batch_tmp/${jobId}`;
    let totalRows = 0;

    async function downloadJson(path: string): Promise<any> {
      const { data, error } = await serviceClient.storage.from(bucket).download(path);
      if (error || !data) return null;
      const text = await data.text();
      return JSON.parse(text);
    }

    async function uploadWorkbook(wb: any, xlsxPath: string, rowCount: number, dataType: string): Promise<void> {
      if (wb.SheetNames.length === 0) return;
      const xlsxBuffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
      const { error: uploadError } = await serviceClient.storage
        .from(bucket)
        .upload(xlsxPath, new Blob([xlsxBuffer], {
          type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }), {
          contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          upsert: true,
        });
      if (uploadError) throw new Error(`Upload failed: ${uploadError.message}`);

      // Best-effort cleanup of any prior result rows for this (job, data_type) so
      // re-runs of salvage-csv don't accumulate duplicate dashboard entries.
      await serviceClient.from("scraped_data_results")
        .delete()
        .eq("scrape_job_id", jobId)
        .eq("data_type", dataType);

      await serviceClient.from("scraped_data_results").insert({
        user_id: userId, scrape_job_id: jobId,
        data_type: dataType, file_path: xlsxPath, row_count: rowCount,
      });
    }

    // ==================== UNIFIED WORKBOOK ====================
    const wb = XLSX.utils.book_new();

    // ---- Tab 1: Master (placeholder header row only — Module 10 fills rows) ----
    const masterWs = XLSX.utils.aoa_to_sheet([MASTER_COLUMNS]);
    masterWs["!cols"] = [
      { wch: 30 }, { wch: 12 }, { wch: 40 }, { wch: 15 }, { wch: 30 },
      { wch: 12 }, { wch: 12 }, { wch: 12 },
      { wch: 15 }, { wch: 15 },
      { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 14 },
      { wch: 40 }, { wch: 40 }, { wch: 80 },
    ];
    XLSX.utils.book_append_sheet(wb, masterWs, "Master");

    // ---- Tabs 2–4: Patient Summary, SOAP Notes Index, Appointments Index ----
    {
      const patientList = await downloadJson(`${tmpPrefix}/patientList.json`);
      const soapIndex = await downloadJson(`${tmpPrefix}/soapIndex.json`);
      const apptIndex = await downloadJson(`${tmpPrefix}/appointmentsIndex.json`);

      const soapSummary: Record<string, string> = {};
      if (soapIndex) {
        for (const s of soapIndex) {
          const name = (s.PatientName || s.Patient || "").trim();
          if (!soapSummary[name]) soapSummary[name] = "";
          const info = s.Documents ? `${s.Documents} docs` : s.Date || "";
          if (info) soapSummary[name] += (soapSummary[name] ? ", " : "") + info;
        }
      }
      const apptSummary: Record<string, string> = {};
      if (apptIndex) {
        for (const a of apptIndex) {
          const name = (a.PatientName || a.Patient || "").trim();
          if (!apptSummary[name]) apptSummary[name] = "";
          const info = a.Appointments ? `${a.Appointments} appts` : a.Date || "";
          if (info) apptSummary[name] += (apptSummary[name] ? ", " : "") + info;
        }
      }
      const dobByName: Record<string, string> = {};
      const demoByName: Record<string, { address: string; phone: string; dob: string; email: string }> = {};
      if (patientList) {
        for (const p of patientList) {
          const name = p.firstName && p.lastName
            ? `${p.lastName}, ${p.firstName}`.trim()
            : (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
          const dob = p.dob || p.DOB || p.Date_Of_Birth || "";
          if (name) {
            dobByName[name] = dob;
            demoByName[name] = {
              address: p.address || p.Address || "",
              phone: p.phone || p.Phone || p.Phone_Number || "",
              dob,
              email: p.email || p.Email || "",
            };
          }
        }
      }

      // Tab 2: Patient Summary
      if (patientList?.length) {
        const summaryRows = patientList.map((p: any) => {
          const name = p.firstName && p.lastName
            ? `${p.lastName}, ${p.firstName}`.trim()
            : (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
          const demo = demoByName[name] || { address: "", phone: "", dob: "", email: "" };
          return {
            "Patient Name": name,
            Address: demo.address,
            Phone: demo.phone,
            DOB: demo.dob,
            Email: demo.email,
            "SOAP Notes": soapSummary[name] || "None",
            Appointments: apptSummary[name] || "None",
          };
        });
        const ws = XLSX.utils.json_to_sheet(summaryRows);
        ws["!cols"] = [{ wch: 30 }, { wch: 40 }, { wch: 15 }, { wch: 12 }, { wch: 30 }, { wch: 50 }, { wch: 50 }];
        XLSX.utils.book_append_sheet(wb, ws, "Patient Summary");
        totalRows += summaryRows.length;
        summaryRows.length = 0;
      }

      // Tab 3: SOAP Notes Index
      if (soapIndex?.length) {
        const sheetData = soapIndex.map((row: any) => {
          const name = (row.PatientName || row.Patient || "").trim();
          return {
            Patient: name, DOB: dobByName[name] || "",
            Documents: row.Documents ?? "", Status: row.Status || "", PDFLink: row.PDFLink || "",
          };
        });
        const ws = XLSX.utils.json_to_sheet(sheetData);
        for (let r = 0; r < sheetData.length; r++) {
          const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 4 });
          if (sheetData[r].PDFLink) {
            ws[cellRef] = { t: "s", v: "📎 Open PDF", l: { Target: sheetData[r].PDFLink, Tooltip: "Open SOAP Note PDF" } };
          }
        }
        ws["!cols"] = [{ wch: 30 }, { wch: 12 }, { wch: 10 }, { wch: 20 }, { wch: 20 }];
        XLSX.utils.book_append_sheet(wb, ws, "SOAP Notes Index");
        totalRows += sheetData.length;
        sheetData.length = 0;
      }

      // Tab 4: Appointments Index
      if (apptIndex?.length) {
        const sheetData = apptIndex.map((row: any) => {
          const name = (row.PatientName || row.Patient || "").trim();
          return {
            Patient: name, DOB: dobByName[name] || "",
            Appointments: row.Appointments ?? "", Status: row.Status || "", PDFLink: row.PDFLink || "",
          };
        });
        const ws = XLSX.utils.json_to_sheet(sheetData);
        for (let r = 0; r < sheetData.length; r++) {
          const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 4 });
          if (sheetData[r].PDFLink) {
            ws[cellRef] = { t: "s", v: "📎 Open PDF", l: { Target: sheetData[r].PDFLink, Tooltip: "Open Appointment PDF" } };
          }
        }
        ws["!cols"] = [{ wch: 30 }, { wch: 12 }, { wch: 10 }, { wch: 20 }, { wch: 20 }];
        XLSX.utils.book_append_sheet(wb, ws, "Appointments Index");
        totalRows += sheetData.length;
        sheetData.length = 0;
      }
      // Block scope ends — patientList, soapIndex, apptIndex, and lookup maps eligible for GC
    }

    // ---- Tab 5: Financials (canonical 13-col schema, AOA for low-memory build) ----
    {
      const ledgerRaw = await downloadJson(`${tmpPrefix}/financialsLedgerRows.json`);
      if (ledgerRaw?.length) {
        // Rebuild DOB lookup from patient list (small CPU cost; big memory savings vs holding state across blocks).
        const patientListForDob = await downloadJson(`${tmpPrefix}/patientList.json`);
        const dobLookup: Record<string, string> = {};
        if (patientListForDob) {
          for (const p of patientListForDob) {
            const name = p.firstName && p.lastName
              ? `${p.lastName}, ${p.firstName}`.trim()
              : (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
            dobLookup[name] = p.dob || p.DOB || p.Date_Of_Birth || "";
          }
          patientListForDob.length = 0;
        }

        // Stamp DOB onto every row before serializing.
        for (const row of ledgerRaw) {
          row.DOB = dobLookup[(row.PatientName || row.Patient || row.patient || "").trim()] || "";
        }

        const finCount = ledgerRaw.length;
        const aoa: (string | number)[][] = [FINANCIALS_COLUMNS];
        for (const row of ledgerRaw) {
          aoa.push(FINANCIALS_COLUMNS.map(k => row[k] ?? ""));
        }
        // Free ledgerRaw before building worksheet (large array).
        ledgerRaw.length = 0;

        const ws = XLSX.utils.aoa_to_sheet(aoa);
        // Free aoa intermediate (sheet already built).
        aoa.length = 0;
        XLSX.utils.book_append_sheet(wb, ws, "Financials");
        totalRows += finCount;
      }
    }

    // ---- Single upload, jobId-based filename (idempotent across re-runs) ----
    const xlsxPath = `${userId}/chirofusion_export_${jobId}.xlsx`;
    await uploadWorkbook(wb, xlsxPath, totalRows, "consolidated_export");

    return new Response(JSON.stringify({
      ok: true,
      xlsxPath,
      message: `Uploaded unified workbook (${wb.SheetNames.length} tabs, ${totalRows} total rows)`,
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (e: any) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
