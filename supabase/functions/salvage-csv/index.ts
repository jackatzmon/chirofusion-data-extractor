import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import * as XLSX from "https://esm.sh/xlsx@0.18.5";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

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
    const uploadedFiles: string[] = [];
    let totalRows = 0;

    async function downloadJson(path: string): Promise<any> {
      const { data, error } = await serviceClient.storage.from(bucket).download(path);
      if (error || !data) return null;
      const text = await data.text();
      return JSON.parse(text);
    }

    async function uploadWorkbook(wb: any, name: string, rowCount: number, dataType: string): Promise<void> {
      if (wb.SheetNames.length === 0) return;
      const xlsxBuffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
      const xlsxPath = `${userId}/${name}_${Date.now()}.xlsx`;
      const { error: uploadError } = await serviceClient.storage
        .from(bucket)
        .upload(xlsxPath, new Blob([xlsxBuffer], {
          type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }), {
          contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          upsert: true,
        });
      if (uploadError) throw new Error(`Upload failed for ${name}: ${uploadError.message}`);

      await serviceClient.from("scraped_data_results").insert({
        user_id: userId, scrape_job_id: jobId,
        data_type: dataType, file_path: xlsxPath, row_count: rowCount,
      });
      uploadedFiles.push(name);
      totalRows += rowCount;
    }

    // ==================== WORKBOOK 1: Patient Summary + SOAP + Appointments ====================
    // These are lightweight index sheets — safe to combine
    {
      const wb = XLSX.utils.book_new();

      // Load patient list for lookups
      const patientList = await downloadJson(`${tmpPrefix}/patientList.json`);
      const soapIndex = await downloadJson(`${tmpPrefix}/soapIndex.json`);
      const apptIndex = await downloadJson(`${tmpPrefix}/appointmentsIndex.json`);

      // Build lookup maps (lightweight — just strings)
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

      // Sheet 1: Patient Summary
      const demoCount = patientList?.length || 0;
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
      }

      // Sheet 2: SOAP Notes Index
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
      }

      // Sheet 3: Appointments Index
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
      }

      if (wb.SheetNames.length > 0) {
        await uploadWorkbook(wb, "patient_data", demoCount + (soapIndex?.length || 0) + (apptIndex?.length || 0), "consolidated_export");
      }
    }
    // Block scope ends — patientList, soapIndex, apptIndex, and all maps are now eligible for GC

    // ==================== WORKBOOK 2: Financials (separate — can be huge) ====================
    {
      const ledgerRaw = await downloadJson(`${tmpPrefix}/financialsLedgerRows.json`);
      if (ledgerRaw?.length) {
        // Build DOB lookup again from patient list (small cost, big memory savings vs keeping it around)
        const patientListForDob = await downloadJson(`${tmpPrefix}/patientList.json`);
        const dobLookup: Record<string, string> = {};
        if (patientListForDob) {
          for (const p of patientListForDob) {
            const name = p.firstName && p.lastName
              ? `${p.lastName}, ${p.firstName}`.trim()
              : (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
            dobLookup[name] = p.dob || p.DOB || p.Date_Of_Birth || "";
          }
        }
        // Free patient list immediately
        if (patientListForDob) patientListForDob.length = 0;

        const finCount = ledgerRaw.length;
        for (const row of ledgerRaw) {
          row.DOB = dobLookup[(row.PatientName || row.Patient || row.patient || "").trim()] || "";
        }

        // Build sheet using AOA (array-of-arrays) for lower memory than json_to_sheet
        const keys = Object.keys(ledgerRaw[0]);
        const aoa: (string | number)[][] = [keys];
        for (const row of ledgerRaw) {
          aoa.push(keys.map(k => row[k] ?? ""));
        }
        // Free ledgerRaw before building worksheet
        ledgerRaw.length = 0;

        const wb = XLSX.utils.book_new();
        const ws = XLSX.utils.aoa_to_sheet(aoa);
        // Free aoa before adding to workbook (sheet already built)
        aoa.length = 0;
        XLSX.utils.book_append_sheet(wb, ws, "Financials");

        await uploadWorkbook(wb, "financials", finCount, "financials_ledger");
      }
    }

    if (uploadedFiles.length === 0) {
      return new Response(JSON.stringify({ ok: false, message: "No data files found" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({
      ok: true,
      xlsxPath: `${userId}/patient_data_latest.xlsx`,
      message: `Uploaded ${uploadedFiles.length} workbook(s): ${uploadedFiles.join(", ")} (${totalRows} total rows)`,
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (e: any) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
