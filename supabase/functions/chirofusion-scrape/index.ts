import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

/** Extract forms, selects, inputs, and links from HTML for discovery */
function extractPageStructure(html: string): string {
  const lines: string[] = [];

  const formRegex = /<form[^>]*>/gi;
  let match;
  while ((match = formRegex.exec(html)) !== null) {
    lines.push(`FORM: ${match[0]}`);
  }

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

  const inputRegex = /<input[^>]*(name|id)="([^"]*)"[^>]*>/gi;
  while ((match = inputRegex.exec(html)) !== null) {
    lines.push(`INPUT: ${match[0].substring(0, 200)}`);
  }

  const ajaxRegex = /\$\.(ajax|post|get)\s*\(\s*["']([^"']+)["']/gi;
  while ((match = ajaxRegex.exec(html)) !== null) {
    lines.push(`AJAX: $.${match[1]}("${match[2]}")`);
  }

  const fetchRegex = /fetch\s*\(\s*["']([^"']+)["']/gi;
  while ((match = fetchRegex.exec(html)) !== null) {
    lines.push(`FETCH: ${match[1]}`);
  }

  const urlRegex = /["'](\/[A-Za-z]+\/[A-Za-z]+[^"']*?)["']/g;
  const urls = new Set<string>();
  while ((match = urlRegex.exec(html)) !== null) {
    if (!match[1].includes('.css') && !match[1].includes('.js') && !match[1].includes('.png') && !match[1].includes('.gif')) {
      urls.add(match[1]);
    }
  }
  if (urls.size > 0) {
    lines.push(`URL_PATTERNS: ${[...urls].slice(0, 50).join(", ")}`);
  }

  return lines.join("\n");
}

/** Extract all cookies from multiple responses, merging them */
function mergeCookies(existing: string, response: Response): string {
  const newCookies = response.headers.getSetCookie?.() || [];
  const cookieMap = new Map<string, string>();

  // Parse existing
  if (existing) {
    for (const part of existing.split("; ")) {
      const [name] = part.split("=");
      if (name) cookieMap.set(name, part);
    }
  }

  // Parse new
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

    // ==================== LOGIN ====================
    console.log("Step 1: Fetching login page to get initial cookies...");
    let sessionCookies = "";
    const logParts: string[] = [];

    try {
      // Step 1: GET the login page to collect any initial cookies / tokens
      const loginPageRes = await fetch("https://www.chirofusionlive.com/Account", {
        redirect: "follow",
      });
      sessionCookies = mergeCookies(sessionCookies, loginPageRes);
      const loginPageHtml = await loginPageRes.text();

      // Extract the RequestVerificationToken if present
      const tokenMatch = loginPageHtml.match(/name="__RequestVerificationToken"[^>]*value="([^"]*)"/);
      const verificationToken = tokenMatch ? tokenMatch[1] : null;

      logParts.push(`===== LOGIN PAGE =====`);
      logParts.push(`Initial cookies: ${sessionCookies}`);
      logParts.push(`Verification token found: ${!!verificationToken}`);

      // Step 2: Also fetch the LoginEssentials JS bundle to find the Login() function
      const scriptMatch = loginPageHtml.match(/src="(\/bundles\/LoginEssentials[^"]*)"/);
      if (scriptMatch) {
        console.log("Fetching LoginEssentials bundle...");
        const jsRes = await fetch(`https://www.chirofusionlive.com${scriptMatch[1]}`, {
          headers: { Cookie: sessionCookies },
        });
        const jsContent = await jsRes.text();

        // Extract the Login function and any AJAX calls
        const loginFuncMatch = jsContent.match(/function\s+Login\s*\([^)]*\)\s*\{[\s\S]*?\n\}/);
        if (loginFuncMatch) {
          logParts.push(`\n===== Login() FUNCTION =====\n${loginFuncMatch[0]}`);
        }

        // Find all AJAX/POST URLs in the bundle
        const ajaxUrls: string[] = [];
        const ajaxRegex = /\$\.(ajax|post|get)\s*\(\s*\{[^}]*url\s*:\s*["']([^"']+)["']/gi;
        let m;
        while ((m = ajaxRegex.exec(jsContent)) !== null) {
          ajaxUrls.push(`$.${m[1]} -> ${m[2]}`);
        }
        const simplePostRegex = /\$\.(post|get)\s*\(\s*["']([^"']+)["']/gi;
        while ((m = simplePostRegex.exec(jsContent)) !== null) {
          ajaxUrls.push(`$.${m[1]} -> ${m[2]}`);
        }
        if (ajaxUrls.length > 0) {
          logParts.push(`\n===== AJAX ENDPOINTS IN LoginEssentials =====\n${ajaxUrls.join("\n")}`);
        }

        // Also dump a broader search for URL patterns
        const urlPatterns = new Set<string>();
        const urlRe = /["'](\/[A-Za-z]+\/[A-Za-z]+[^"'\s,)]*?)["']/g;
        while ((m = urlRe.exec(jsContent)) !== null) {
          if (!m[1].includes('.css') && !m[1].includes('.js') && !m[1].includes('.png') && !m[1].includes('.gif')) {
            urlPatterns.add(m[1]);
          }
        }
        logParts.push(`\n===== URL PATTERNS IN LoginEssentials =====\n${[...urlPatterns].join("\n")}`);
      }

      // Step 3: Try the AJAX login approach (Login() likely uses $.post or $.ajax)
      // Try POST to /Account/LoginUser or /Account/Login with JSON
      const loginAttempts = [
        {
          name: "POST /Account/Login (form)",
          url: "https://www.chirofusionlive.com/Account/Login",
          contentType: "application/x-www-form-urlencoded",
          body: new URLSearchParams({
            txtLoginUserName: creds.cf_username,
            txtLoginPassword: creds.cf_password,
            chkEULA: "on",
            ...(verificationToken ? { __RequestVerificationToken: verificationToken } : {}),
          }).toString(),
        },
        {
          name: "POST /Account/Login (JSON)",
          url: "https://www.chirofusionlive.com/Account/Login",
          contentType: "application/json",
          body: JSON.stringify({
            txtLoginUserName: creds.cf_username,
            txtLoginPassword: creds.cf_password,
            chkEULA: true,
          }),
        },
        {
          name: "POST /Account/LoginUser (JSON)",
          url: "https://www.chirofusionlive.com/Account/LoginUser",
          contentType: "application/json",
          body: JSON.stringify({
            UserName: creds.cf_username,
            Password: creds.cf_password,
          }),
        },
      ];

      for (const attempt of loginAttempts) {
        console.log(`Trying: ${attempt.name}...`);
        const res = await fetch(attempt.url, {
          method: "POST",
          headers: {
            "Content-Type": attempt.contentType,
            Cookie: sessionCookies,
            "X-Requested-With": "XMLHttpRequest",
          },
          body: attempt.body,
          redirect: "manual",
        });

        sessionCookies = mergeCookies(sessionCookies, res);
        const resText = await res.text();

        logParts.push(`\n===== ${attempt.name} =====`);
        logParts.push(`Status: ${res.status}`);
        logParts.push(`Location: ${res.headers.get("location") || "none"}`);
        logParts.push(`Set-Cookie count: ${(res.headers.getSetCookie?.() || []).length}`);
        logParts.push(`Response (first 500): ${resText.substring(0, 500)}`);

        // If we got a redirect to a non-login page, login worked
        const location = res.headers.get("location");
        if (location && !location.includes("Account") && !location.includes("Login")) {
          logParts.push(`✅ LOGIN SUCCESS via ${attempt.name} -> redirect to ${location}`);
          console.log(`Login succeeded via ${attempt.name}`);
          break;
        }
        if (res.status === 200 && !resText.includes("Login") && resText.length < 100) {
          logParts.push(`✅ Possible AJAX login success`);
          break;
        }
      }

      // Step 4: After login attempts, try fetching the actual pages
      console.log("Testing authenticated access...");
      logParts.push(`\nFinal cookies: ${sessionCookies}`);

      const pagesToFetch = [
        { name: "Home", url: "https://www.chirofusionlive.com/" },
        { name: "Scheduler", url: "https://www.chirofusionlive.com/User/Scheduler" },
        { name: "Billing", url: "https://www.chirofusionlive.com/Billing/" },
      ];

      for (const page of pagesToFetch) {
        try {
          console.log(`Fetching ${page.name}...`);
          const res = await fetch(page.url, {
            headers: { Cookie: sessionCookies },
            redirect: "follow",
          });

          const html = await res.text();
          const isLoginPage = html.includes("txtLoginUserName") || html.includes("Login()");
          const structure = extractPageStructure(html);

          logParts.push(`\n===== ${page.name.toUpperCase()} (${page.url}) =====`);
          logParts.push(`Status: ${res.status} | Final URL: ${res.url}`);
          logParts.push(`HTML Length: ${html.length} | Is Login Page: ${isLoginPage}`);
          logParts.push(structure);
          logParts.push(`\n--- RAW HTML (first 3000 chars) ---\n${html.substring(0, 3000)}`);
        } catch (err: any) {
          logParts.push(`\n===== ${page.name.toUpperCase()} ERROR =====\n${err.message}`);
        }
      }

    } catch (loginError: any) {
      logParts.push(`\n===== LOGIN ERROR =====\n${loginError.message}\n${loginError.stack}`);
      await serviceClient.from("scrape_jobs").update({ status: "failed", error_message: loginError.message, log_output: logParts.join("\n") }).eq("id", job.id);
      return new Response(JSON.stringify({ error: loginError.message }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const fullLog = logParts.join("\n");

    // ==================== DISCOVER MODE ====================
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

    // ==================== SCRAPE MODE (placeholder) ====================
    await serviceClient.from("scrape_jobs").update({
      status: "completed",
      progress: 100,
      log_output: fullLog + "\n\nScrape mode: Real scraping not yet implemented. Run discovery first.",
    }).eq("id", job.id);

    return new Response(JSON.stringify({ success: true, jobId: job.id }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (error: any) {
    console.error("Error:", error);
    return new Response(JSON.stringify({ error: error.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
