import { createClient } from "npm:@supabase/supabase-js@2";
import { corsHeaders } from "npm:@supabase/supabase-js@2/cors";

const USER_ID = "cde8c555-dc87-48db-bf12-a442ae85c7ac";
const BUCKET = "scraped-data";
const EXPIRES_IN = 3153600000;

type Entry = { path: string; url: string };

async function listAll(supabase: any, folder: string): Promise<{ name: string }[]> {
  const all: { name: string }[] = [];
  let offset = 0;
  const limit = 1000;
  while (true) {
    const { data, error } = await supabase.storage.from(BUCKET).list(folder, {
      limit,
      offset,
    });
    if (error) throw error;
    if (!data || data.length === 0) break;
    all.push(...data);
    if (data.length < limit) break;
    offset += limit;
  }
  return all;
}

function dedupeNewest(files: { name: string }[]): { name: string }[] {
  const re = /^(.*)_(\d+)\.pdf$/i;
  const best = new Map<string, { name: string; ts: number }>();
  for (const f of files) {
    if (!f?.name) continue;
    const m = f.name.match(re);
    if (!m) continue;
    const key = m[1];
    const ts = Number(m[2]);
    const cur = best.get(key);
    if (!cur || ts > cur.ts) best.set(key, { name: f.name, ts });
  }
  return Array.from(best.values()).map((v) => ({ name: v.name }));
}

async function signAll(supabase: any, folder: string, files: { name: string }[]): Promise<Entry[]> {
  const paths = files.map((f) => `${folder}/${f.name}`);
  const out: Entry[] = [];
  const chunkSize = 100;
  for (let i = 0; i < paths.length; i += chunkSize) {
    const chunk = paths.slice(i, i + chunkSize);
    const { data, error } = await supabase.storage
      .from(BUCKET)
      .createSignedUrls(chunk, EXPIRES_IN);
    if (error) throw error;
    for (const row of data || []) {
      if (row.signedUrl && row.path) {
        out.push({ path: row.path, url: row.signedUrl });
      }
    }
  }
  return out;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  try {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    const medicalFolder = `${USER_ID}/medical_files`;
    const apptFolder = `${USER_ID}/appointment_summaries`;

    const [medicalRaw, apptRaw] = await Promise.all([
      listAll(supabase, medicalFolder),
      listAll(supabase, apptFolder),
    ]);

    const medicalDedup = dedupeNewest(medicalRaw);
    const apptDedup = dedupeNewest(apptRaw);

    const [medical, appointments] = await Promise.all([
      signAll(supabase, medicalFolder, medicalDedup),
      signAll(supabase, apptFolder, apptDedup),
    ]);

    return new Response(
      JSON.stringify({
        userId: USER_ID,
        counts: { medical: medical.length, appointments: appointments.length },
        medical,
        appointments,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" }, status: 200 }
    );
  } catch (error) {
    return new Response(
      JSON.stringify({ error: (error as any).message || String(error) }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" }, status: 500 }
    );
  }
});
