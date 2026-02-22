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

    async function downloadJson(path: string): Promise<any> {
      const { data, error } = await serviceClient.storage.from(bucket).download(path);
      if (error || !data) return null;
      const text = await data.text();
      return JSON.parse(text);
    }

    // ── Phase 1: Build XLSX with Patient Summary + SOAP + Appointments ──
    const wb = XLSX.utils.book_new();

    const patientList = await downloadJson(`${tmpPrefix}/patientList.json`);
    const soapIndex = await downloadJson(`${tmpPrefix}/soapIndex.json`);
    const apptIndex = await downloadJson(`${tmpPrefix}/appointmentsIndex.json`);

    // Build summary maps
    const soapSummary: Record<string, string> = {};
    if (soapIndex) {
      for (const s of soapIndex) {
        const name = (s.Patient || "").trim();
        if (!soapSummary[name]) soapSummary[name] = "";
        if (s.Date) soapSummary[name] += (soapSummary[name] ? ", " : "") + s.Date;
      }
    }
    const apptSummary: Record<string, string> = {};
    if (apptIndex) {
      for (const a of apptIndex) {
        const name = (a.Patient || "").trim();
        if (!apptSummary[name]) apptSummary[name] = "";
        if (a.Date) apptSummary[name] += (apptSummary[name] ? ", " : "") + a.Date;
      }
    }

    // DOB lookup
    const dobByName: Record<string, string> = {};
    if (patientList) {
      for (const p of patientList) {
        const name = (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
        const dob = p.DOB || p.Date_Of_Birth || p.dob || "";
        if (name && dob) dobByName[name] = dob;
      }
    }

    // Patient Summary sheet
    if (patientList?.length) {
      const summaryRows = patientList.map((p: any) => {
        const name = (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
        return {
          "Patient Name": name,
          Address: p.Address || p.address || "",
          Phone: p.Phone || p.phone || p.Phone_Number || "",
          DOB: p.DOB || p.Date_Of_Birth || p.dob || "",
          Email: p.Email || p.email || "",
          "SOAP Notes": soapSummary[name] || "None",
          Appointments: apptSummary[name] || "None",
        };
      });
      const ws = XLSX.utils.json_to_sheet(summaryRows);
      ws["!cols"] = [{ wch: 30 }, { wch: 40 }, { wch: 15 }, { wch: 12 }, { wch: 30 }, { wch: 50 }, { wch: 50 }];
      XLSX.utils.book_append_sheet(wb, ws, "Patient Summary");
    }
    const demoCount = patientList?.length || 0;

    // SOAP Notes Index
    if (soapIndex?.length) {
      const sheetData = soapIndex.map((row: any) => ({
        Patient: row.Patient || "", DOB: dobByName[(row.Patient || "").trim()] || "",
        Date: row.Date || "", Status: row.Status || "", PDFLink: row.PDFLink || "",
      }));
      const ws = XLSX.utils.json_to_sheet(sheetData);
      for (let r = 0; r < soapIndex.length; r++) {
        const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 4 });
        if (soapIndex[r].PDFLink) {
          ws[cellRef] = { t: "s", v: "📎 Open PDF", l: { Target: soapIndex[r].PDFLink, Tooltip: "Open SOAP Note PDF" } };
        }
      }
      ws["!cols"] = [{ wch: 30 }, { wch: 12 }, { wch: 12 }, { wch: 15 }, { wch: 20 }];
      XLSX.utils.book_append_sheet(wb, ws, "SOAP Notes Index");
    }

    // Appointments Index
    if (apptIndex?.length) {
      const sheetData = apptIndex.map((row: any) => ({
        Patient: row.Patient || "", DOB: dobByName[(row.Patient || "").trim()] || "",
        Date: row.Date || "", Status: row.Status || "", PDFLink: row.PDFLink || "",
      }));
      const ws = XLSX.utils.json_to_sheet(sheetData);
      for (let r = 0; r < apptIndex.length; r++) {
        const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 4 });
        if (apptIndex[r].PDFLink) {
          ws[cellRef] = { t: "s", v: "📎 Open PDF", l: { Target: apptIndex[r].PDFLink, Tooltip: "Open Appointment PDF" } };
        }
      }
      ws["!cols"] = [{ wch: 30 }, { wch: 12 }, { wch: 12 }, { wch: 15 }, { wch: 20 }];
      XLSX.utils.book_append_sheet(wb, ws, "Appointments Index");
    }

    // Free index arrays
    const soapLen = soapIndex?.length || 0;
    const apptLen = apptIndex?.length || 0;

    // Upload XLSX (without financials)
    let xlsxPath = "";
    if (wb.SheetNames.length > 0) {
      const xlsxBuffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
      xlsxPath = `${userId}/patient_summary_${Date.now()}.xlsx`;
      const xlsxBlob = new Blob([xlsxBuffer], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });
      const { error: uploadError } = await serviceClient.storage
        .from(bucket)
        .upload(xlsxPath, xlsxBlob, {
          contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          upsert: true,
        });
      if (uploadError) throw new Error(`XLSX upload failed: ${uploadError.message}`);

      await serviceClient.from("scraped_data_results").insert({
        user_id: userId,
        scrape_job_id: jobId,
        data_type: "patient_summary",
        file_path: xlsxPath,
        row_count: demoCount,
      });
    }

    // ── Phase 2: Financials as CSV (memory-safe for 47k+ rows) ──
    const ledgerRows = await downloadJson(`${tmpPrefix}/financialsLedgerRows.json`);
    let csvPath = "";
    let finCount = 0;
    if (ledgerRows?.length) {
      finCount = ledgerRows.length;
      // Add DOB in-place
      for (const row of ledgerRows) {
        const name = (row.Patient || row.patient || "").trim();
        row.DOB = dobByName[name] || "";
      }
      // Build CSV string
      const keys = Object.keys(ledgerRows[0]);
      const csvLines = [keys.join(",")];
      for (const row of ledgerRows) {
        csvLines.push(keys.map(k => {
          const v = (row[k] ?? "").toString().replace(/"/g, '""');
          return v.includes(",") || v.includes('"') || v.includes("\n") ? `"${v}"` : v;
        }).join(","));
      }
      const csvContent = csvLines.join("\n");
      csvPath = `${userId}/financials_ledger_${Date.now()}.csv`;
      const { error: csvErr } = await serviceClient.storage
        .from(bucket)
        .upload(csvPath, new Blob([csvContent], { type: "text/csv" }), {
          contentType: "text/csv",
          upsert: true,
        });
      if (csvErr) throw new Error(`CSV upload failed: ${csvErr.message}`);

      await serviceClient.from("scraped_data_results").insert({
        user_id: userId,
        scrape_job_id: jobId,
        data_type: "financials_ledger",
        file_path: csvPath,
        row_count: finCount,
      });
    }

    if (!xlsxPath && !csvPath) {
      return new Response(JSON.stringify({ ok: false, message: "No data files found" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({
      ok: true, xlsxPath, csvPath,
      message: `Patient Summary XLSX (${demoCount} patients) + Financials CSV (${finCount} rows)`,
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (e: any) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
