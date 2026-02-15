import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

function extractPageStructure(html: string): string {
  const lines: string[] = [];
  let match;

  const formRegex = /<form[^>]*>/gi;
  while ((match = formRegex.exec(html)) !== null) lines.push(`FORM: ${match[0]}`);

  const selectRegex = /<select[^>]*id="([^"]*)"[^>]*>[\s\S]*?<\/select>/gi;
  while ((match = selectRegex.exec(html)) !== null) {
    const optionRegex = /<option[^>]*value="([^"]*)"[^>]*>([^<]*)<\/option>/gi;
    let optMatch;
    const opts: string[] = [];
    while ((optMatch = optionRegex.exec(match[0])) !== null) opts.push(`${optMatch[1]}=${optMatch[2]}`);
    lines.push(`SELECT#${match[1]}: ${opts.slice(0, 20).join(" | ")}`);
  }

  const inputRegex = /<input[^>]*(name|id)="([^"]*)"[^>]*>/gi;
  while ((match = inputRegex.exec(html)) !== null) lines.push(`INPUT: ${match[0].substring(0, 200)}`);

  const ajaxRegex = /\$\.(ajax|post|get)\s*\(\s*\{[^}]*url\s*:\s*["']([^"']+)["']/gi;
  while ((match = ajaxRegex.exec(html)) !== null) lines.push(`AJAX: $.${match[1]}("${match[2]}")`);

  const urlRegex = /["'](\/[A-Za-z]+\/[A-Za-z]+[^"']*?)["']/g;
  const urls = new Set<string>();
  while ((match = urlRegex.exec(html)) !== null) {
    if (!match[1].match(/\.(css|js|png|gif|ico|jpg)/)) urls.add(match[1]);
  }
  if (urls.size > 0) lines.push(`URL_PATTERNS: ${[...urls].slice(0, 50).join(", ")}`);

  return lines.join("\n");
}

function mergeCookies(existing: string, response: Response): string {
  const newCookies = response.headers.getSetCookie?.() || [];
  const cookieMap = new Map<string, string>();
  if (existing) {
    for (const part of existing.split("; ")) {
      const [name] = part.split("=");
      if (name) cookieMap.set(name, part);
    }
  }
  for (const c of newCookies) {
    const cookiePart = c.split(";")[0];
    const [name] = cookiePart.split("=");
    if (name) cookieMap.set(name, cookiePart);
  }
  return [...cookieMap.values()].join("; ");
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

    const { data: creds, error: credsError } = await supabase
      .from("chirofusion_credentials")
      .select("*")
      .eq("user_id", userId)
      .maybeSingle();

    if (credsError || !creds) {
      return new Response(JSON.stringify({ error: "ChiroFusion credentials not found." }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const { data: job, error: jobError } = await supabase
      .from("scrape_jobs")
      .insert({ user_id: userId, data_types: mode === "discover" ? ["discovery"] : dataTypes, status: "running", mode })
      .select()
      .single();

    if (jobError) {
      return new Response(JSON.stringify({ error: jobError.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const serviceClient = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    const logParts: string[] = [];
    let sessionCookies = "";

    const browserHeaders = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.5",
    };

    /** Follow redirects manually to preserve cookies */
    async function fetchWithCookies(url: string, opts: RequestInit = {}): Promise<{ response: Response; body: string; finalUrl: string }> {
      let currentUrl = url;
      let redirectCount = 0;
      while (redirectCount < 10) {
        const res = await fetch(currentUrl, {
          ...opts,
          headers: { ...browserHeaders, Cookie: sessionCookies, ...(opts.headers || {}) },
          redirect: "manual",
        });
        sessionCookies = mergeCookies(sessionCookies, res);
        const location = res.headers.get("location");
        if (location && (res.status === 301 || res.status === 302 || res.status === 303 || res.status === 307)) {
          await res.text(); // consume body
          currentUrl = location.startsWith("http") ? location : `https://www.chirofusionlive.com${location}`;
          redirectCount++;
          continue;
        }
        const body = await res.text();
        return { response: res, body, finalUrl: currentUrl };
      }
      throw new Error(`Too many redirects for ${url}`);
    }

    // ==================== LOGIN ====================
    try {
      // Step 1: GET login page for session cookie
      console.log("Step 1: Fetching login page...");
      const { body: loginPageHtml } = await fetchWithCookies("https://www.chirofusionlive.com/Account");
      logParts.push(`Session cookie: ${sessionCookies}`);

      // Step 2: POST to /Account/Login/DoLogin (the real AJAX endpoint)
      console.log("Step 2: POST to /Account/Login/DoLogin...");
      const loginRes = await fetch("https://www.chirofusionlive.com/Account/Login/DoLogin", {
        method: "POST",
        headers: {
          ...browserHeaders,
          "Content-Type": "application/x-www-form-urlencoded",
          "Cookie": sessionCookies,
          "X-Requested-With": "XMLHttpRequest",
          "Referer": "https://www.chirofusionlive.com/Account",
          "Origin": "https://www.chirofusionlive.com",
        },
        body: new URLSearchParams({
          userName: creds.cf_username,
          password: creds.cf_password,
        }).toString(),
        redirect: "manual",
      });

      sessionCookies = mergeCookies(sessionCookies, loginRes);
      const loginResponse = await loginRes.text();

      logParts.push(`\n===== DoLogin RESPONSE =====`);
      logParts.push(`Status: ${loginRes.status}`);
      logParts.push(`Response: ${loginResponse.substring(0, 1000)}`);
      logParts.push(`Set-Cookie headers: ${JSON.stringify(loginRes.headers.getSetCookie?.() || [])}`);
      logParts.push(`Cookies after login: ${sessionCookies}`);

      const lowerResponse = loginResponse.toLowerCase().replace(/^"|"$/g, '').trim();
      if (lowerResponse.includes("invalidcredentials")) {
        throw new Error("Invalid credentials.");
      }
      if (lowerResponse === "blocked") {
        throw new Error("Account locked. Try again in 20 minutes.");
      }
      if (lowerResponse === "paused") {
        throw new Error("Account paused.");
      }

      logParts.push(`\n✅ Login response: "${lowerResponse}"`);

      // Skip FirstLook (returns 500) - try directly accessing pages
      // Also try the AJAX content endpoints that the SPA uses
      logParts.push(`\nSkipping FirstLook. Testing direct page access...`);

    } catch (loginError: any) {
      logParts.push(`\n❌ LOGIN ERROR: ${loginError.message}`);
      await serviceClient.from("scrape_jobs").update({
        status: "failed",
        error_message: loginError.message,
        log_output: logParts.join("\n"),
      }).eq("id", job.id);
      return new Response(JSON.stringify({ error: loginError.message }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ==================== FETCH PAGES ====================
    // Try both full page loads AND AJAX content endpoints
    console.log("Fetching authenticated pages...");
    const pagesToFetch = [
      { name: "Scheduler (full page)", url: "https://www.chirofusionlive.com/User/Scheduler" },
      { name: "Scheduler (AJAX)", url: "https://www.chirofusionlive.com/User/Home/GetContent", ajax: true },
      { name: "Patient Search", url: "https://www.chirofusionlive.com/Patient/Patient/GetAllPatientForLocalStorage", ajax: true },
      { name: "Billing Main", url: "https://www.chirofusionlive.com/Billing/Billing/MainDashboard", ajax: true },
      { name: "Billing (full page)", url: "https://www.chirofusionlive.com/Billing/" },
      { name: "Dashboard Providers", url: "https://www.chirofusionlive.com/Dashboard/Dashboard/GetAllProviders", ajax: true },
    ];

    for (const page of pagesToFetch) {
      try {
        console.log(`Fetching ${page.name}...`);

        if (page.ajax) {
          // AJAX endpoints - use XMLHttpRequest headers
          const res = await fetch(page.url, {
            headers: {
              ...browserHeaders,
              "Cookie": sessionCookies,
              "X-Requested-With": "XMLHttpRequest",
              "Accept": "application/json, text/html, */*",
              "Referer": "https://www.chirofusionlive.com/User/Scheduler",
            },
            redirect: "manual",
          });
          sessionCookies = mergeCookies(sessionCookies, res);
          const body = await res.text();
          const contentType = res.headers.get("content-type") || "unknown";
          const isLoginPage = body.includes("txtLoginUserName");

          logParts.push(`\n===== ${page.name.toUpperCase()} =====`);
          logParts.push(`Status: ${res.status} | Content-Type: ${contentType} | Is Login: ${isLoginPage}`);
          logParts.push(`Body length: ${body.length}`);
          logParts.push(`Body (first 3000): ${body.substring(0, 3000)}`);
        } else {
          // Full page - use manual redirect following
          const { response: res, body: html, finalUrl } = await fetchWithCookies(page.url);
          const isLoginPage = html.includes("txtLoginUserName");
          const structure = extractPageStructure(html);

          logParts.push(`\n===== ${page.name.toUpperCase()} =====`);
          logParts.push(`Status: ${res.status} | Final URL: ${finalUrl} | Is Login: ${isLoginPage}`);
          logParts.push(`HTML Length: ${html.length}`);
          logParts.push(structure);
          logParts.push(`\n--- RAW HTML (first 5000 chars) ---\n${html.substring(0, 5000)}`);
        }
      } catch (err: any) {
        logParts.push(`\n===== ${page.name.toUpperCase()} ERROR =====\n${err.message}`);
      }
    }

    const fullLog = logParts.join("\n");

    if (mode === "discover") {
      await serviceClient.from("scrape_jobs").update({
        status: "completed",
        progress: 100,
        log_output: fullLog,
      }).eq("id", job.id);

      return new Response(JSON.stringify({ success: true, jobId: job.id, mode: "discover" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Scrape mode placeholder
    await serviceClient.from("scrape_jobs").update({
      status: "completed",
      progress: 100,
      log_output: fullLog + "\n\nScrape mode: Real scraping not yet implemented.",
    }).eq("id", job.id);

    return new Response(JSON.stringify({ success: true, jobId: job.id }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (error: any) {
    console.error("Error:", error);
    return new Response(JSON.stringify({ error: error.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
