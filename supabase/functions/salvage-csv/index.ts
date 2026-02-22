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
    const { jobId, phase } = await req.json();
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

    async function uploadWorkbook(wb: any, filePath: string) {
      const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
      const blob = new Blob([buf], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
      const { error } = await serviceClient.storage.from(bucket).upload(filePath, blob, {
        contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        upsert: true,
      });
      if (error) throw new Error(`Upload failed: ${error.message}`);
    }

    // ═══════════════════════════════════════════════════════════
    // PHASE 1: Patient Summary + SOAP Index + Appt Index
    // ═══════════════════════════════════════════════════════════
    if (!phase || phase === 1) {
      const wb = XLSX.utils.book_new();

      const patientList: any[] = (await downloadJson(`${tmpPrefix}/patientList.json`)) || [];
      const soapIndex: any[] = (await downloadJson(`${tmpPrefix}/soapIndex.json`)) || [];
      const apptIndex: any[] = (await downloadJson(`${tmpPrefix}/appointmentsIndex.json`)) || [];

      // Build summaries
      const soapSummary: Record<string, string> = {};
      for (const s of soapIndex) {
        const name = (s.Patient || "").trim();
        if (!soapSummary[name]) soapSummary[name] = "";
        if (s.Date) soapSummary[name] += (soapSummary[name] ? ", " : "") + s.Date;
      }
      const apptSummary: Record<string, string> = {};
      for (const a of apptIndex) {
        const name = (a.Patient || "").trim();
        if (!apptSummary[name]) apptSummary[name] = "";
        if (a.Date) apptSummary[name] += (apptSummary[name] ? ", " : "") + a.Date;
      }

      // Patient Summary sheet
      const summaryRows: any[] = [];
      const seenNames = new Set<string>();
      for (const p of patientList) {
        const name = (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
        seenNames.add(name);
        summaryRows.push({
          Patient: name,
          Address: p.Address || p.address || "",
          Phone: p.Phone || p.phone || p.Phone_Number || "",
          DOB: p.DOB || p.Date_Of_Birth || p.dob || "",
          Email: p.Email || p.email || "",
          SOAP_Notes: soapSummary[name] || "None",
          Appointments: apptSummary[name] || "None",
        });
      }
      // Add patients only in SOAP/Appt but not demographics
      for (const name of [...Object.keys(soapSummary), ...Object.keys(apptSummary)]) {
        if (seenNames.has(name)) continue;
        seenNames.add(name);
        summaryRows.push({
          Patient: name, Address: "", Phone: "", DOB: "", Email: "",
          SOAP_Notes: soapSummary[name] || "None",
          Appointments: apptSummary[name] || "None",
        });
      }

      if (summaryRows.length) {
        const ws = XLSX.utils.json_to_sheet(summaryRows);
        ws["!cols"] = [{ wch: 30 }, { wch: 40 }, { wch: 15 }, { wch: 12 }, { wch: 30 }, { wch: 50 }, { wch: 50 }];
        XLSX.utils.book_append_sheet(wb, ws, "Patient Summary");
      }

      // SOAP Notes Index sheet
      if (soapIndex.length) {
        const ws = XLSX.utils.json_to_sheet(soapIndex.map((r: any) => ({
          Patient: r.Patient || "", Date: r.Date || "", Status: r.Status || "", PDFLink: r.PDFLink || "",
        })));
        for (let r = 0; r < soapIndex.length; r++) {
          if (soapIndex[r].PDFLink) {
            const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 3 });
            ws[cellRef] = { t: "s", v: "📎 Open PDF", l: { Target: soapIndex[r].PDFLink, Tooltip: "Open SOAP Note PDF" } };
          }
        }
        ws["!cols"] = [{ wch: 30 }, { wch: 12 }, { wch: 15 }, { wch: 20 }];
        XLSX.utils.book_append_sheet(wb, ws, "SOAP Notes Index");
      }

      // Appointments Index sheet
      if (apptIndex.length) {
        const ws = XLSX.utils.json_to_sheet(apptIndex.map((r: any) => ({
          Patient: r.Patient || "", Date: r.Date || "", Status: r.Status || "", PDFLink: r.PDFLink || "",
        })));
        for (let r = 0; r < apptIndex.length; r++) {
          if (apptIndex[r].PDFLink) {
            const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 3 });
            ws[cellRef] = { t: "s", v: "📎 Open PDF", l: { Target: apptIndex[r].PDFLink, Tooltip: "Open Appointment PDF" } };
          }
        }
        ws["!cols"] = [{ wch: 30 }, { wch: 12 }, { wch: 15 }, { wch: 20 }];
        XLSX.utils.book_append_sheet(wb, ws, "Appointments Index");
      }

      const filePath = `${userId}/patient_summary_${Date.now()}.xlsx`;
      await uploadWorkbook(wb, filePath);

      await serviceClient.from("scraped_data_results").insert({
        user_id: userId, scrape_job_id: jobId,
        data_type: "patient_summary",
        file_path: filePath,
        row_count: summaryRows.length,
      });

      // Save demo lookup for phase 2
      const demoByName: Record<string, [string, string, string, string]> = {};
      for (const p of patientList) {
        const name = (p.Patient_Name || p.Name || p.Patient || Object.values(p)[0] || "").toString().trim();
        demoByName[name] = [
          p.Address || p.address || "",
          p.Phone || p.phone || p.Phone_Number || "",
          p.DOB || p.Date_Of_Birth || p.dob || "",
          p.Email || p.email || "",
        ];
      }
      await serviceClient.storage.from(bucket).upload(
        `${tmpPrefix}/_lookups.json`,
        new Blob([JSON.stringify({ demoByName, soapSummary, apptSummary })], { type: "application/json" }),
        { upsert: true }
      );

      // Fire phase 2
      fetch(`${supabaseUrl}/functions/v1/salvage-csv`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${serviceKey}` },
        body: JSON.stringify({ jobId, phase: 2 }),
      }).catch(() => {});

      return new Response(JSON.stringify({
        ok: true, phase: 1, message: "Patient summary created. Building financials workbook...",
      }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ═══════════════════════════════════════════════════════════
    // PHASE 2: Financials as CSV (XLSX too memory-heavy for 47k rows)
    // ═══════════════════════════════════════════════════════════
    if (phase === 2) {
      // Load slim lookups
      const lookups = await downloadJson(`${tmpPrefix}/_lookups.json`);
      const demoByName = lookups?.demoByName || {};
      const soapSummary = lookups?.soapSummary || {};
      const apptSummary = lookups?.apptSummary || {};

      // Load financials
      const ledgerRows: any[] = (await downloadJson(`${tmpPrefix}/financialsLedgerRows.json`)) || [];
      if (!ledgerRows.length) {
        return new Response(JSON.stringify({ ok: true, phase: 2, message: "No financial data" }), {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }

      // Build CSV manually - much lighter than XLSX
      function csvEscape(val: any): string {
        const s = String(val ?? "");
        if (s.includes(",") || s.includes('"') || s.includes("\n")) return '"' + s.replace(/"/g, '""') + '"';
        return s;
      }

      // Determine all column keys from first row + added columns
      const extraCols = ["Address", "Phone", "DOB", "Email", "SOAP_Notes", "Appointments"];
      const origKeys = Object.keys(ledgerRows[0]).filter(k => k.toLowerCase() !== "patient" && k.toLowerCase() !== "name");
      const allCols = ["Patient", ...extraCols, ...origKeys];

      const csvLines: string[] = [allCols.map(csvEscape).join(",")];
      for (const row of ledgerRows) {
        const name = (row.Patient || row.patient || "").trim();
        const demo = demoByName[name] || ["", "", "", ""];
        const vals = [
          name, demo[0], demo[1], demo[2], demo[3],
          soapSummary[name] || "", apptSummary[name] || "",
          ...origKeys.map(k => row[k] ?? ""),
        ];
        csvLines.push(vals.map(csvEscape).join(","));
      }

      const csvContent = csvLines.join("\n");
      const filePath = `${userId}/financials_ledger_${Date.now()}.csv`;
      const { error: uploadError } = await serviceClient.storage.from(bucket).upload(
        filePath,
        new Blob([csvContent], { type: "text/csv" }),
        { contentType: "text/csv", upsert: true }
      );
      if (uploadError) throw new Error(`Upload failed: ${uploadError.message}`);

      await serviceClient.storage.from(bucket).remove([`${tmpPrefix}/_lookups.json`]).catch(() => {});

      await serviceClient.from("scraped_data_results").insert({
        user_id: userId, scrape_job_id: jobId,
        data_type: "financials_ledger",
        file_path: filePath,
        row_count: ledgerRows.length,
      });

      return new Response(JSON.stringify({
        ok: true, phase: 2, filePath, message: `Financials CSV created with ${ledgerRows.length} rows`,
      }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    throw new Error("Invalid phase");
  } catch (e: any) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
