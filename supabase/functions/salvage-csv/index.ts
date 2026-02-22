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

    const wb = XLSX.utils.book_new();

    // 1. Patient Summary — load patients + build SOAP/Appt summaries
    const patientList = await downloadJson(`${tmpPrefix}/patientList.json`);
    const soapIndex = await downloadJson(`${tmpPrefix}/soapIndex.json`);
    const apptIndex = await downloadJson(`${tmpPrefix}/appointmentsIndex.json`);

    // Build summary maps (tiny memory footprint)
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

    // Build Patient Summary sheet
    if (patientList?.length) {
      const summaryRows = patientList.map((p: any) => {
        const name = (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
        return {
          Patient: name,
          Address: p.Address || p.address || "",
          Phone: p.Phone || p.phone || p.Phone_Number || "",
          DOB: p.DOB || p.Date_Of_Birth || p.dob || "",
          Email: p.Email || p.email || "",
          SOAP_Notes: soapSummary[name] || "None",
          Appointments: apptSummary[name] || "None",
        };
      });
      const ws = XLSX.utils.json_to_sheet(summaryRows);
      ws["!cols"] = [{ wch: 30 }, { wch: 40 }, { wch: 15 }, { wch: 12 }, { wch: 30 }, { wch: 50 }, { wch: 50 }];
      XLSX.utils.book_append_sheet(wb, ws, "Patient Summary");
    }
    const demoCount = patientList?.length || 0;

    // 2. SOAP Notes Index (with PDF links)
    if (soapIndex?.length) {
      const sheetData = soapIndex.map((row: any) => ({
        Patient: row.Patient || "", Date: row.Date || "",
        Status: row.Status || "", PDFLink: row.PDFLink || "",
      }));
      const ws = XLSX.utils.json_to_sheet(sheetData);
      for (let r = 0; r < soapIndex.length; r++) {
        const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 3 });
        if (soapIndex[r].PDFLink) {
          ws[cellRef] = { t: "s", v: "📎 Open PDF", l: { Target: soapIndex[r].PDFLink, Tooltip: "Open SOAP Note PDF" } };
        }
      }
      ws["!cols"] = [{ wch: 30 }, { wch: 12 }, { wch: 15 }, { wch: 20 }];
      XLSX.utils.book_append_sheet(wb, ws, "SOAP Notes Index");
    }

    // 3. Appointments Index (with PDF links)
    if (apptIndex?.length) {
      const sheetData = apptIndex.map((row: any) => ({
        Patient: row.Patient || "", Date: row.Date || "",
        Status: row.Status || "", PDFLink: row.PDFLink || "",
      }));
      const ws = XLSX.utils.json_to_sheet(sheetData);
      for (let r = 0; r < apptIndex.length; r++) {
        const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 3 });
        if (apptIndex[r].PDFLink) {
          ws[cellRef] = { t: "s", v: "📎 Open PDF", l: { Target: apptIndex[r].PDFLink, Tooltip: "Open Appointment PDF" } };
        }
      }
      ws["!cols"] = [{ wch: 30 }, { wch: 12 }, { wch: 15 }, { wch: 20 }];
      XLSX.utils.book_append_sheet(wb, ws, "Appointments Index");
    }

    // 4. Financials — load last, keep as-is (no modifications to save memory)
    const ledgerRows = await downloadJson(`${tmpPrefix}/financialsLedgerRows.json`);
    if (ledgerRows?.length) {
      const ws = XLSX.utils.json_to_sheet(ledgerRows);
      XLSX.utils.book_append_sheet(wb, ws, "Financials");
    }
    const finCount = ledgerRows?.length || 0;

    if (wb.SheetNames.length === 0) {
      return new Response(JSON.stringify({ ok: false, message: "No data files found" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Write workbook
    const xlsxBuffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
    const filePath = `${userId}/consolidated_export_${Date.now()}.xlsx`;
    const xlsxBlob = new Blob([xlsxBuffer], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
    const { error: uploadError } = await serviceClient.storage
      .from(bucket)
      .upload(filePath, xlsxBlob, {
        contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        upsert: true,
      });
    if (uploadError) throw new Error(`Upload failed: ${uploadError.message}`);

    await serviceClient.from("scraped_data_results").insert({
      user_id: userId,
      scrape_job_id: jobId,
      data_type: "consolidated_export",
      file_path: filePath,
      row_count: demoCount + finCount,
    });

    return new Response(JSON.stringify({
      ok: true, filePath, sheets: wb.SheetNames,
      message: `Created workbook with ${wb.SheetNames.length} sheets`,
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (e: any) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
