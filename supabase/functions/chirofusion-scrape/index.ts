import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

/** Extract forms, selects, inputs, and links from HTML for discovery */
function extractPageStructure(html: string): string {
  const lines: string[] = [];

  // Extract form tags with action/method
  const formRegex = /<form[^>]*>/gi;
  let match;
  while ((match = formRegex.exec(html)) !== null) {
    lines.push(`FORM: ${match[0]}`);
  }

  // Extract select elements with their options
  const selectRegex = /<select[^>]*id="([^"]*)"[^>]*>[\s\S]*?<\/select>/gi;
  while ((match = selectRegex.exec(html)) !== null) {
    const optionRegex = /<option[^>]*value="([^"]*)"[^>]*>([^<]*)<\/option>/gi;
    let optMatch;
    const opts: string[] = [];
    while ((optMatch = optionRegex.exec(match[0])) !== null) {
      opts.push(`${optMatch[1]}=${optMatch[2]}`);
    }
    lines.push(`SELECT#${match[1]}: ${opts.slice(0, 20).join(" | ")}`);
  }

  // Extract input fields
  const inputRegex = /<input[^>]*(name|id)="([^"]*)"[^>]*>/gi;
  while ((match = inputRegex.exec(html)) !== null) {
    lines.push(`INPUT: ${match[0].substring(0, 200)}`);
  }

  // Extract script blocks that might contain AJAX URLs
  const ajaxRegex = /\$\.(ajax|post|get)\s*\(\s*["']([^"']+)["']/gi;
  while ((match = ajaxRegex.exec(html)) !== null) {
    lines.push(`AJAX: $.${match[1]}("${match[2]}")`);
  }

  const fetchRegex = /fetch\s*\(\s*["']([^"']+)["']/gi;
  while ((match = fetchRegex.exec(html)) !== null) {
    lines.push(`FETCH: ${match[1]}`);
  }

  // Extract any URL patterns in script tags
  const urlRegex = /["'](\/[A-Za-z]+\/[A-Za-z]+[^"']*?)["']/g;
  const urls = new Set<string>();
  while ((match = urlRegex.exec(html)) !== null) {
    if (!match[1].includes('.css') && !match[1].includes('.js') && !match[1].includes('.png')) {
      urls.add(match[1]);
    }
  }
  if (urls.size > 0) {
    lines.push(`URL_PATTERNS: ${[...urls].slice(0, 30).join(", ")}`);
  }

  return lines.join("\n");
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const authHeader = req.headers.get("Authorization");
    if (!authHeader?.startsWith("Bearer ")) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_ANON_KEY")!,
      { global: { headers: { Authorization: authHeader } } }
    );

    const { data: claimsData, error: claimsError } = await supabase.auth.getClaims(authHeader.replace("Bearer ", ""));
    if (claimsError || !claimsData?.claims) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const userId = claimsData.claims.sub as string;
    const body = await req.json();
    const { dataTypes = [], mode = "scrape" } = body;

    // Get user's ChiroFusion credentials
    const { data: creds, error: credsError } = await supabase
      .from("chirofusion_credentials")
      .select("*")
      .eq("user_id", userId)
      .maybeSingle();

    if (credsError || !creds) {
      return new Response(JSON.stringify({ error: "ChiroFusion credentials not found. Please save them first." }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // Create a scrape job
    const { data: job, error: jobError } = await supabase
      .from("scrape_jobs")
      .insert({ user_id: userId, data_types: mode === "discover" ? ["discovery"] : dataTypes, status: "running", mode })
      .select()
      .single();

    if (jobError) {
      return new Response(JSON.stringify({ error: jobError.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // Use service role for storage and job updates
    const serviceClient = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    // Step 1: Login to ChiroFusion
    console.log("Logging into ChiroFusion...");
    let sessionCookies: string = "";

    try {
      const loginRes = await fetch("https://www.chirofusionlive.com/Account/Login", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          txtLoginUserName: creds.cf_username,
          txtLoginPassword: creds.cf_password,
          chkEULA: "on",
        }),
        redirect: "manual",
      });

      const cookies = loginRes.headers.getSetCookie?.() || [];
      sessionCookies = cookies.map((c: string) => c.split(";")[0]).join("; ");

      if (!sessionCookies) {
        throw new Error("Failed to authenticate with ChiroFusion. Check your credentials.");
      }

      console.log("Login successful, status:", loginRes.status);
    } catch (loginError: any) {
      await serviceClient.from("scrape_jobs").update({ status: "failed", error_message: loginError.message }).eq("id", job.id);
      return new Response(JSON.stringify({ error: loginError.message }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ==================== DISCOVER MODE ====================
    if (mode === "discover") {
      console.log("Running in DISCOVER mode...");
      const logParts: string[] = [];

      const pagesToFetch = [
        { name: "Home", url: "https://www.chirofusionlive.com/" },
        { name: "Scheduler", url: "https://www.chirofusionlive.com/User/Scheduler" },
        { name: "Billing", url: "https://www.chirofusionlive.com/Billing/" },
      ];

      for (const page of pagesToFetch) {
        try {
          console.log(`Fetching ${page.name} page...`);
          const res = await fetch(page.url, {
            headers: { Cookie: sessionCookies },
            redirect: "follow",
          });

          const html = await res.text();
          const structure = extractPageStructure(html);

          const section = `\n===== ${page.name.toUpperCase()} (${page.url}) =====\nStatus: ${res.status}\nFinal URL: ${res.url}\nHTML Length: ${html.length}\n\n${structure}\n\n--- RAW HTML (first 3000 chars) ---\n${html.substring(0, 3000)}\n`;

          logParts.push(section);
          console.log(`${page.name}: ${html.length} chars, extracted ${structure.split("\n").length} structure lines`);
        } catch (err: any) {
          logParts.push(`\n===== ${page.name.toUpperCase()} ERROR =====\n${err.message}\n`);
          console.error(`Error fetching ${page.name}:`, err.message);
        }
      }

      const fullLog = logParts.join("\n");

      // Update job with discovery log
      await serviceClient.from("scrape_jobs").update({
        status: "completed",
        progress: 100,
        log_output: fullLog,
      }).eq("id", job.id);

      console.log("Discovery complete. Total log length:", fullLog.length);

      return new Response(JSON.stringify({ success: true, jobId: job.id, mode: "discover" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // ==================== SCRAPE MODE (placeholder for Phase 2) ====================
    console.log("Running in SCRAPE mode...");
    let progress = 0;
    const totalTypes = dataTypes.length;

    for (const dataType of dataTypes) {
      try {
        console.log(`Scraping ${dataType}...`);

        // Phase 2: Real scraping will be implemented after discovery
        // For now, log that we need discovery data first
        console.log(`${dataType}: Real scraping not yet implemented. Run discovery first.`);

        progress += Math.round(100 / totalTypes);
        await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress, 100) }).eq("id", job.id);
      } catch (scrapeError: any) {
        console.error(`Error scraping ${dataType}:`, scrapeError);
      }
    }

    await serviceClient.from("scrape_jobs").update({
      status: "completed",
      progress: 100,
      log_output: "Scrape mode: Real scraping not yet implemented. Run discovery first to map ChiroFusion's endpoints.",
    }).eq("id", job.id);

    return new Response(JSON.stringify({ success: true, jobId: job.id }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (error: any) {
    console.error("Error:", error);
    return new Response(JSON.stringify({ error: error.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
