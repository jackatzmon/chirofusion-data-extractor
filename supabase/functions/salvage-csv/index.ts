import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

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
    const results: string[] = [];

    async function downloadJson(path: string): Promise<any> {
      const { data, error } = await serviceClient.storage.from(bucket).download(path);
      if (error || !data) return null;
      const text = await data.text();
      return JSON.parse(text);
    }

    function csvEscape(val: any): string {
      if (val === null || val === undefined) return "";
      const s = String(val);
      if (s.includes(",") || s.includes('"') || s.includes("\n")) {
        return `"${s.replace(/"/g, '""')}"`;
      }
      return s;
    }

    async function convertAndUpload(name: string, dataType: string, data: any[]) {
      const headers = Object.keys(data[0]);
      const csvLines = [headers.map(csvEscape).join(",")];
      for (const row of data) {
        csvLines.push(headers.map(h => csvEscape(row[h])).join(","));
      }
      const csvContent = csvLines.join("\n");
      const filePath = `${userId}/salvaged_${name}_${jobId.slice(0,8)}.csv`;
      await serviceClient.storage.from(bucket).upload(filePath, new Blob([csvContent], { type: "text/csv" }), { upsert: true });
      results.push(`${name}: ${data.length} rows → ${filePath}`);
      await serviceClient.from("scraped_data_results").insert({
        user_id: userId, scrape_job_id: jobId, data_type: dataType, file_path: filePath, row_count: data.length,
      });
    }

    const patientList = await downloadJson(`${tmpPrefix}/patientList.json`);
    if (patientList?.length) await convertAndUpload("demographics", "demographics", patientList);

    const ledgerRows = await downloadJson(`${tmpPrefix}/financialsLedgerRows.json`);
    if (ledgerRows?.length) await convertAndUpload("financials", "financials", ledgerRows);

    const soapIndex = await downloadJson(`${tmpPrefix}/soapIndex.json`);
    if (soapIndex?.length) await convertAndUpload("soap_notes", "soap_notes", soapIndex);

    const apptIndex = await downloadJson(`${tmpPrefix}/appointmentsIndex.json`);
    if (apptIndex?.length) await convertAndUpload("appointments", "appointments", apptIndex);

    const apptData = await downloadJson(`${tmpPrefix}/apptData.json`);
    if (apptData?.length) await convertAndUpload("appointment_details", "appointments", apptData);

    if (results.length === 0) {
      return new Response(JSON.stringify({ ok: false, message: "No data files found" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ ok: true, results }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (e: any) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
