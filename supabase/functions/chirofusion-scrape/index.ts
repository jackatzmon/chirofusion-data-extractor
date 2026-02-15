import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

const BASE_URL = "https://www.chirofusionlive.com";

const browserHeaders = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.5",
};

function mergeCookies(existing: string, response: Response): string {
  // Try getSetCookie() first (Deno 1.34+), fall back to parsing set-cookie header
  let newCookies: string[] = [];
  try {
    newCookies = response.headers.getSetCookie?.() || [];
  } catch {
    // Fallback: manually get set-cookie header(s)
    const raw = response.headers.get("set-cookie");
    if (raw) newCookies = [raw];
  }

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
  return lines.join("\n");
}

/** Convert JSON array to CSV string */
function jsonToCsv(data: Record<string, unknown>[]): string {
  if (!data.length) return "";
  const headers = Object.keys(data[0]);
  const rows = data.map(row =>
    headers.map(h => `"${String(row[h] ?? "").replace(/"/g, '""')}"`).join(",")
  );
  return headers.join(",") + "\n" + rows.join("\n");
}

/** Parse .NET date format /Date(timestamp)/ */
function parseNetDate(dateStr: string | null): string {
  if (!dateStr) return "";
  const match = dateStr.match(/\/Date\((-?\d+)\)\//);
  if (match) {
    const d = new Date(parseInt(match[1]));
    return d.toLocaleDateString("en-US");
  }
  return dateStr;
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

    const { data: { user }, error: userError } = await supabase.auth.getUser();
    if (userError || !user) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const userId = user.id;
    const body = await req.json();
    const { dataTypes = [], mode = "scrape", dateFrom, dateTo } = body;

    const { data: creds, error: credsError } = await supabase
      .from("chirofusion_credentials")
      .select("*")
      .eq("user_id", userId)
      .maybeSingle();

    if (credsError || !creds) {
      return new Response(JSON.stringify({ error: "ChiroFusion credentials not found." }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const jobDataTypes = mode === "discover" ? ["discovery"] : dataTypes;
    const { data: job, error: jobError } = await supabase
      .from("scrape_jobs")
      .insert({ user_id: userId, data_types: jobDataTypes, status: "running", mode })
      .select()
      .single();

    if (jobError) {
      return new Response(JSON.stringify({ error: jobError.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const serviceClient = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    let sessionCookies = "";
    const logParts: string[] = [];
    const startTime = Date.now();
    const MAX_RUNTIME_MS = 140_000; // 140s safety margin (Supabase limit ~150s)

    function isTimingOut(): boolean {
      return (Date.now() - startTime) > MAX_RUNTIME_MS;
    }

    // Helper: follow redirects manually preserving cookies
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
        if (location && [301, 302, 303, 307].includes(res.status)) {
          await res.text();
          currentUrl = location.startsWith("http") ? location : `${BASE_URL}${location}`;
          redirectCount++;
          continue;
        }
        const responseBody = await res.text();
        return { response: res, body: responseBody, finalUrl: currentUrl };
      }
      throw new Error(`Too many redirects for ${url}`);
    }

    // Helper: AJAX request
    async function ajaxFetch(url: string, opts: RequestInit = {}): Promise<{ status: number; body: string; contentType: string }> {
      const res = await fetch(url.startsWith("http") ? url : `${BASE_URL}${url}`, {
        ...opts,
        headers: {
          ...browserHeaders,
          "Cookie": sessionCookies,
          "X-Requested-With": "XMLHttpRequest",
          "Accept": "application/json, text/html, */*;q=0.01",
          "Referer": `${BASE_URL}/User/Scheduler`,
          ...(opts.headers || {}),
        },
        redirect: "manual",
      });
      sessionCookies = mergeCookies(sessionCookies, res);
      const responseBody = await res.text();
      return { status: res.status, body: responseBody, contentType: res.headers.get("content-type") || "" };
    }

    // ==================== LOGIN ====================
    try {
      console.log("Logging in...");
      await fetchWithCookies(`${BASE_URL}/Account`);

      const loginRes = await fetch(`${BASE_URL}/Account/Login/DoLogin`, {
        method: "POST",
        headers: {
          ...browserHeaders,
          "Content-Type": "application/x-www-form-urlencoded",
          "Cookie": sessionCookies,
          "X-Requested-With": "XMLHttpRequest",
          "Referer": `${BASE_URL}/Account`,
          "Origin": BASE_URL,
        },
        body: new URLSearchParams({
          userName: creds.cf_username,
          password: creds.cf_password,
        }).toString(),
        redirect: "manual",
      });

      sessionCookies = mergeCookies(sessionCookies, loginRes);
      const loginResponse = await loginRes.text();
      const lowerResponse = loginResponse.toLowerCase().replace(/^"|"$/g, "").trim();

      if (lowerResponse.includes("invalidcredentials")) throw new Error("Invalid credentials.");
      if (lowerResponse === "blocked") throw new Error("Account locked. Try again in 20 minutes.");
      if (lowerResponse === "paused") throw new Error("Account paused.");

      logParts.push(`✅ Login: "${lowerResponse}"`);
      console.log("Login successful:", lowerResponse);
    } catch (loginError: any) {
      logParts.push(`❌ LOGIN ERROR: ${loginError.message}`);
      await serviceClient.from("scrape_jobs").update({ status: "failed", error_message: loginError.message, log_output: logParts.join("\n") }).eq("id", job.id);
      return new Response(JSON.stringify({ error: loginError.message }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ==================== DISCOVER MODE ====================
    if (mode === "discover") {
      // Load Scheduler page and extract ALL URLs, JS function calls, and AJAX patterns
      try {
        const { body: html, finalUrl } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
        logParts.push(`===== SCHEDULER PAGE =====`);
        logParts.push(`URL: ${finalUrl} | Length: ${html.length}`);

        // Extract ALL URL patterns from JavaScript (ajax calls, fetch, $.get, $.post, etc.)
        const urlPatterns: string[] = [];
        const urlRegex = /(?:url|href|src|action|ajax|get|post|fetch)\s*[:(\s'"]+\s*['"]([^'"]+?)['"]/gi;
        let urlMatch;
        while ((urlMatch = urlRegex.exec(html)) !== null) {
          const url = urlMatch[1];
          if (url.startsWith("/") && !url.includes(".css") && !url.includes(".js") && !url.includes(".png") && !url.includes(".svg") && !url.includes(".ico")) {
            urlPatterns.push(url);
          }
        }
        // Dedupe
        const uniqueUrls = [...new Set(urlPatterns)].sort();
        logParts.push(`\n===== ALL AJAX/URL PATTERNS (${uniqueUrls.length}) =====`);
        for (const u of uniqueUrls) {
          logParts.push(u);
        }

        // Find all function definitions related to "Patient", "Report", "Export"
        const fnRegex = /function\s+(\w*(?:Patient|Report|Export|Excel|Run|Print)\w*)\s*\(/gi;
        let fnMatch;
        const functions: string[] = [];
        while ((fnMatch = fnRegex.exec(html)) !== null) {
          functions.push(fnMatch[1]);
        }
        logParts.push(`\n===== RELEVANT JS FUNCTIONS (${functions.length}) =====`);
        for (const fn of functions) {
          logParts.push(fn);
        }

        // Extract onclick handlers related to export/report
        const onclickRegex = /onclick\s*=\s*"([^"]*(?:Export|Report|Patient|Excel|Print|Run)[^"]*)"/gi;
        let onclickMatch;
        const handlers: string[] = [];
        while ((onclickMatch = onclickRegex.exec(html)) !== null) {
          handlers.push(onclickMatch[1]);
        }
        logParts.push(`\n===== ONCLICK HANDLERS (${handlers.length}) =====`);
        for (const h of handlers) {
          logParts.push(h);
        }

        // Search for the "Export To Excel" and "Run Report" button HTML
        const exportBtnRegex = /(?:<[^>]*(?:Export|Run Report|Print Report)[^>]*>[\s\S]{0,200})/gi;
        let btnMatch;
        logParts.push(`\n===== EXPORT/REPORT BUTTONS =====`);
        while ((btnMatch = exportBtnRegex.exec(html)) !== null) {
          logParts.push(btnMatch[0].substring(0, 300));
        }

        // Also search for "PatientReport" in script blocks
        const scriptRegex = /<script[^>]*>([\s\S]*?)<\/script>/gi;
        let scriptMatch;
        logParts.push(`\n===== SCRIPT BLOCKS WITH 'PatientReport' or 'ExportPatient' =====`);
        while ((scriptMatch = scriptRegex.exec(html)) !== null) {
          const scriptContent = scriptMatch[1];
          if (scriptContent.includes("PatientReport") || scriptContent.includes("ExportPatient") || scriptContent.includes("ExportToExcel") || scriptContent.includes("RunReport") || scriptContent.includes("btnRunReport") || scriptContent.includes("btnExport")) {
            // Extract just the relevant lines
            const lines = scriptContent.split("\n");
            for (const line of lines) {
              if (line.match(/patient|report|export|excel|print|run/i) && line.trim().length > 5) {
                logParts.push(line.trim().substring(0, 300));
              }
            }
          }
        }

      } catch (err: any) {
        logParts.push(`SCHEDULER ERROR: ${err.message}`);
      }

      // Also test the JSON patient endpoint
      try {
        const { status, body, contentType } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
        logParts.push(`\nJSON Patient endpoint: status=${status} type=${contentType} length=${body.length}`);
      } catch (err: any) {
        logParts.push(`JSON Patient endpoint ERROR: ${err.message}`);
      }

      await serviceClient.from("scrape_jobs").update({ status: "completed", progress: 100, log_output: logParts.join("\n") }).eq("id", job.id);
      return new Response(JSON.stringify({ success: true, jobId: job.id, mode: "discover" }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ==================== SCRAPE MODE ====================
    console.log("Starting scrape mode...");
    let progress = 0;
    const totalTypes = dataTypes.length;
    let hasAnyData = false;

    // Shared: get full patient list with IDs for per-patient scraping
    // Caches result so it's only fetched once even if multiple data types need it
    let _cachedPatients: { id: number; firstName: string; lastName: string }[] | null = null;
    async function getAllPatientIds(): Promise<{ id: number; firstName: string; lastName: string }[]> {
      if (_cachedPatients) return _cachedPatients;

      // Method 1: JSON endpoint (fast, but may return subset)
      const { body, contentType } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
      if (contentType.includes("application/json")) {
        try {
          const parsed = JSON.parse(body);
          const patients = parsed.PatientData || parsed;
          if (Array.isArray(patients) && patients.length > 0) {
            _cachedPatients = patients.map((p: any) => ({
              id: p.I,
              firstName: p.F || "",
              lastName: p.L || "",
            }));
            logParts.push(`Patient list: ${_cachedPatients!.length} patients from JSON endpoint`);

            // Method 2: If JSON only returned a few, try paginating the Patient Report HTML
            if (_cachedPatients!.length < 50) {
              logParts.push(`Only ${_cachedPatients!.length} from JSON, trying Patient Report pagination...`);
              const allFromReport: { id: number; firstName: string; lastName: string }[] = [];
              let page = 1;
              let hasMore = true;

              while (hasMore && page <= 200) {
                const { body: reportHtml, status } = await ajaxFetch(
                  `/User/Scheduler/GetPatientReportData?reportType=PatientList&patientStatus=All&page=${page}&pageSize=100`,
                );
                logParts.push(`Report page ${page}: status=${status} length=${reportHtml.length}`);

                if (status !== 200 || reportHtml.length < 50) {
                  hasMore = false;
                  break;
                }

                // Try to parse as JSON (may return patient array with IDs)
                try {
                  const reportData = JSON.parse(reportHtml);
                  const items = reportData.Data || reportData.data || reportData;
                  if (Array.isArray(items) && items.length > 0) {
                    for (const item of items) {
                      const pid = item.PatientId || item.PkPatientId || item.Id || item.I;
                      if (pid) {
                        allFromReport.push({
                          id: pid,
                          firstName: item.FirstName || item.F || "",
                          lastName: item.LastName || item.L || "",
                        });
                      }
                    }
                    if (items.length < 100) hasMore = false;
                  } else {
                    hasMore = false;
                  }
                } catch {
                  // If HTML, try extracting patient IDs from links
                  const linkRegex = /patientId[=:](\d+)/gi;
                  let linkMatch;
                  const foundIds = new Set<number>();
                  while ((linkMatch = linkRegex.exec(reportHtml)) !== null) {
                    const pid = parseInt(linkMatch[1]);
                    if (pid && !foundIds.has(pid)) {
                      foundIds.add(pid);
                      allFromReport.push({ id: pid, firstName: "", lastName: "" });
                    }
                  }
                  if (foundIds.size === 0) hasMore = false;
                }
                page++;
                await new Promise(r => setTimeout(r, 300));
              }

              if (allFromReport.length > _cachedPatients!.length) {
                _cachedPatients = allFromReport;
                logParts.push(`✅ Patient list expanded to ${_cachedPatients!.length} via report pagination`);
              }
            }

            return _cachedPatients!;
          }
        } catch { /* ignore */ }
      }

      _cachedPatients = [];
      return _cachedPatients;
    }

    for (const dataType of dataTypes) {
      try {
        console.log(`Scraping ${dataType}...`);
        logParts.push(`\n--- Scraping: ${dataType} ---`);

        let csvContent = "";
        let rowCount = 0;

        switch (dataType) {
          // ==================== DEMOGRAPHICS ====================
          case "demographics": {
            let found = false;

            // Helper: extract __RequestVerificationToken from page HTML
            function extractVerifToken(html: string): string {
              const m = html.match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/);
              return m ? m[1] : "";
            }

            // Helper: extract select option values from HTML
            function extractSelectOptions(html: string, selectId: string): { value: string; text: string }[] {
              const selectRegex = new RegExp(`<select[^>]*id="${selectId}"[^>]*>([\\s\\S]*?)</select>`, "i");
              const selectMatch = html.match(selectRegex);
              if (!selectMatch) return [];
              const opts: { value: string; text: string }[] = [];
              const optRegex = /<option[^>]*value="([^"]*)"[^>]*>([^<]*)<\/option>/gi;
              let om;
              while ((om = optRegex.exec(selectMatch[1])) !== null) {
                opts.push({ value: om[1], text: om[2] });
              }
              return opts;
            }

            // ===== Attempt 1: Load Scheduler, extract JS bundles, find function bodies =====
            logParts.push(`Step 1: Loading Scheduler page...`);
            const { body: schedHtml } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
            const schedToken = extractVerifToken(schedHtml);
            logParts.push(`Scheduler: ${schedHtml.length} chars, token: ${schedToken ? "YES" : "NONE"}`);

            // Find external JS file URLs
            const scriptSrcRegex = /<script[^>]*src\s*=\s*["']([^"']+)["']/gi;
            let scriptMatch;
            const jsFiles: string[] = [];
            while ((scriptMatch = scriptSrcRegex.exec(schedHtml)) !== null) {
              const src = scriptMatch[1];
              if (!src.includes("jquery") && !src.includes("kendo") && !src.includes("bootstrap") && !src.includes("signalr") && !src.includes("moment")) {
                jsFiles.push(src.startsWith("http") ? src : `${BASE_URL}${src}`);
              }
            }
            logParts.push(`Custom JS files: ${jsFiles.length} → ${jsFiles.map(f => f.split("/").pop()).join(", ")}`);

            // Fetch each custom JS file and look for RunPatientReport/ExportPatientReport functions
            for (const jsUrl of jsFiles.slice(0, 5)) {
              if (isTimingOut()) break;
              try {
                const { body: jsBody } = await fetchWithCookies(jsUrl);
                // Search for function definitions
                const fnNames = ["RunPatientReport", "ExportPatientReport", "ExportPatientList", "GetPatientReport"];
                for (const fnName of fnNames) {
                  const idx = jsBody.indexOf(`function ${fnName}`);
                  if (idx >= 0) {
                    // Extract ~600 chars of function body
                    logParts.push(`FOUND ${fnName} in ${jsUrl.split("/").pop()}: ${jsBody.substring(idx, idx + 600).replace(/\s+/g, " ")}`);
                  }
                  // Also check for property-style: fnName: function or fnName = function
                  const propIdx = jsBody.indexOf(`${fnName} =`);
                  if (propIdx >= 0 && propIdx !== idx) {
                    logParts.push(`FOUND ${fnName}= in ${jsUrl.split("/").pop()}: ${jsBody.substring(propIdx, propIdx + 600).replace(/\s+/g, " ")}`);
                  }
                }
                // Also look for any URL containing "PatientReport" or "GetPatient"
                const urlInJs = /["']([^"']*(?:PatientReport|GetPatient|ExportPatient)[^"']*)["']/gi;
                let urlMatch;
                while ((urlMatch = urlInJs.exec(jsBody)) !== null) {
                  logParts.push(`URL in ${jsUrl.split("/").pop()}: ${urlMatch[1]}`);
                }
              } catch (e: any) {
                logParts.push(`JS fetch error ${jsUrl.split("/").pop()}: ${e.message}`);
              }
            }

            // ===== Attempt 1b: Try ExportPatientList (simple form submit, no params) =====
            // The form has NO hidden fields - just <input type="submit" />
            logParts.push(`Step 1b: ExportPatientList (no params, like browser form submit)...`);
            try {
              const { response: expRes, body: expBody } = await fetchWithCookies(
                `${BASE_URL}/Patient/Patient/ExportPatientList`,
                {
                  method: "POST",
                  headers: { "Content-Type": "application/x-www-form-urlencoded" },
                  body: "",
                }
              );
              const ct = expRes.headers.get("content-type") || "unknown";
              const cd = expRes.headers.get("content-disposition") || "";
              logParts.push(`ExportPatientList: status=${expRes.status} length=${expBody.length} type=${ct} disposition=${cd}`);
              if (expBody.length > 50) {
                logParts.push(`Preview: ${expBody.substring(0, 500)}`);
                if (!expBody.includes("<!DOCTYPE") && !expBody.includes("<html")) {
                  csvContent = expBody;
                  rowCount = csvContent.split("\n").filter(l => l.trim()).length - 1;
                  logParts.push(`✅ Demographics (ExportPatientList): ${rowCount} rows`);
                  found = true;
                }
              }
            } catch (err: any) {
              logParts.push(`ExportPatientList error: ${err.message}`);
            }

            // ===== Attempt 2: Run Report first, wait, then Export from Scheduler =====
            if (!found) {
              logParts.push(`Step 2: Run Report → Export flow...`);
              try {
                // Reuse schedHtml and schedToken from Attempt 1

                // Build report parameters
                const reportParams: Record<string, string> = {
                  PatientReportType: "1",
                  ReportType: "1",
                  PatientStatus: "Active",
                  PatientStatusString: "Active",
                  IsAllPatientValue: "true",
                  IsAllPatient: "true",
                  BirthMonths: "",
                  BirthMonthString: "",
                  PatientInsurance: "",
                  PatientInsuranceString: "",
                  isOverrideDate: "false",
                };

                // Step 2a: TRIGGER report generation
                const endpoint = "/User/Scheduler/GetPatientReports";
                logParts.push(`Step 2a: Triggering report generation with corrected params...`);
                
                // Try with form data (include both field name variants)
                try {
                  const triggerParams = new URLSearchParams({
                    ...reportParams,
                    __RequestVerificationToken: schedToken,
                    take: "5000", skip: "0", page: "1", pageSize: "5000",
                  });
                  const runRes = await ajaxFetch(endpoint, {
                    method: "POST",
                    headers: {
                      "Content-Type": "application/x-www-form-urlencoded",
                      "RequestVerificationToken": schedToken,
                    },
                    body: triggerParams.toString(),
                  });
                  logParts.push(`Trigger (form): status=${runRes.status} length=${runRes.body.length}`);
                  if (runRes.body.length > 10) {
                    logParts.push(`  Preview: ${runRes.body.substring(0, 400)}`);
                    if (runRes.status === 200 && runRes.body.length > 50 && runRes.contentType.includes("json")) {
                      try {
                        const data = JSON.parse(runRes.body);
                        const items = data.Data || data.data || data.Items || data.Result || (Array.isArray(data) ? data : null);
                        if (items && Array.isArray(items) && items.length > 0) {
                          csvContent = jsonToCsv(items);
                          rowCount = items.length;
                          logParts.push(`  ✅ Demographics direct: ${rowCount} patients`);
                          found = true;
                        } else {
                          logParts.push(`  Response keys: ${Object.keys(data).join(",")}, Total: ${data.Total || "N/A"}`);
                        }
                      } catch { /* not JSON array */ }
                    }
                  }
                } catch (e: any) { logParts.push(`Trigger error: ${e.message}`); }

                // (removed uniqueDiscovered loop - variable was never defined in scrape mode)

                // Step 2b: ALWAYS wait for report generation, even if trigger returned empty
                if (!found) {
                  logParts.push(`Step 2b: Waiting 20 seconds for server-side report generation...`);
                  await new Promise(r => setTimeout(r, 20000));

                  // Step 2c: Now try Export — the report should be ready
                  logParts.push(`Step 2c: Attempting export after wait...`);
                  const exportParams = new URLSearchParams({
                    ...reportParams,
                    __RequestVerificationToken: schedToken,
                  });

                  // Try form POST (browser-style form submit)
                  const { response: rptRes, body: rptBody } = await fetchWithCookies(
                    `${BASE_URL}/User/Scheduler/ExportPatientReports`,
                    {
                      method: "POST",
                      headers: { "Content-Type": "application/x-www-form-urlencoded" },
                      body: exportParams.toString(),
                    }
                  );
                  const rptCt = rptRes.headers.get("content-type") || "unknown";
                  const rptCd = rptRes.headers.get("content-disposition") || "";
                  logParts.push(`ExportPatientReports: status=${rptRes.status} length=${rptBody.length} type=${rptCt} disposition=${rptCd}`);

                  if (rptBody.length > 50) {
                    logParts.push(`Preview: ${rptBody.substring(0, 500)}`);
                    if (!rptBody.includes("<!DOCTYPE") && !rptBody.includes("<html")) {
                      try {
                        const data = JSON.parse(rptBody);
                        const items = data.Data || data.data || data.Items || (Array.isArray(data) ? data : null);
                        if (items && Array.isArray(items) && items.length > 0) {
                          csvContent = jsonToCsv(items);
                          rowCount = items.length;
                          logParts.push(`✅ Demographics: ${rowCount} patients from export`);
                          found = true;
                        }
                      } catch {
                        if (rptBody.includes(",") && rptBody.includes("\n")) {
                          csvContent = rptBody;
                          rowCount = rptBody.split("\n").filter(l => l.trim()).length - 1;
                          logParts.push(`✅ Demographics CSV: ${rowCount} rows`);
                          found = true;
                        }
                      }
                    }
                  }

                  // Also try AJAX version
                  if (!found) {
                    const ajaxRes = await ajaxFetch("/User/Scheduler/ExportPatientReports", {
                      method: "POST",
                      headers: {
                        "Content-Type": "application/x-www-form-urlencoded",
                        "RequestVerificationToken": schedToken,
                      },
                      body: exportParams.toString(),
                    });
                    logParts.push(`AJAX Export: status=${ajaxRes.status} length=${ajaxRes.body.length} type=${ajaxRes.contentType}`);
                    if (ajaxRes.body.length > 50 && !ajaxRes.body.includes("<!DOCTYPE")) {
                      logParts.push(`AJAX Preview: ${ajaxRes.body.substring(0, 300)}`);
                      try {
                        const data = JSON.parse(ajaxRes.body);
                        const items = data.Data || data.data || data.Items || data.Result || (Array.isArray(data) ? data : null);
                        if (items && Array.isArray(items) && items.length > 0) {
                          csvContent = jsonToCsv(items);
                          rowCount = items.length;
                          logParts.push(`✅ Demographics (AJAX after wait): ${rowCount} patients`);
                          found = true;
                        }
                      } catch { /* not JSON */ }
                    }
                  }
                }
              } catch (err: any) {
                logParts.push(`ExportPatientReports error: ${err.message}`);
              }
            }

            // ===== Attempt 3: JSON endpoint (partial data but reliable) =====
            if (!found) {
              logParts.push(`Step 3: JSON fallback /Patient/Patient/GetAllPatientForLocalStorage...`);
              try {
                const { body, contentType } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
                logParts.push(`JSON endpoint: type=${contentType} length=${body.length}`);
                if (contentType.includes("application/json") && body.length > 10) {
                  const parsed = JSON.parse(body);
                  const patients = parsed.PatientData || parsed;
                  if (Array.isArray(patients) && patients.length > 0) {
                    // Map all available fields from the JSON response
                    const expanded = patients.map((p: any) => ({
                      PatientID: p.I || p.Id || "",
                      FirstName: p.F || p.FirstName || "",
                      LastName: p.L || p.LastName || "",
                      DateOfBirth: parseNetDate(p.DOB || p.DateOfBirth || ""),
                      HomePhone: p.HP || p.HomePhone || "",
                      MobilePhone: p.MP || p.MobilePhone || "",
                      Email: p.E || p.Email || "",
                      Gender: p.G || p.Gender || "",
                      Address: p.A || p.Address || "",
                      City: p.C || p.City || "",
                      State: p.S || p.State || "",
                      Zip: p.Z || p.Zip || "",
                      Insurance: p.Ins || p.Insurance || "",
                    }));
                    csvContent = jsonToCsv(expanded);
                    rowCount = expanded.length;
                    logParts.push(`⚠️ Demographics (JSON fallback): ${rowCount} patients`);
                    // Log first patient's raw keys so we can map more fields next time
                    logParts.push(`Sample patient keys: ${Object.keys(patients[0]).join(", ")}`);
                    logParts.push(`Sample patient data: ${JSON.stringify(patients[0]).substring(0, 500)}`);
                  }
                }
              } catch (err: any) {
                logParts.push(`JSON fallback error: ${err.message}`);
              }
            }
            break;
          }

          // ==================== APPOINTMENTS ====================
          case "appointments": {
            // Discovery found: form #exportPatientAppntList POSTs to /User/Scheduler/ExportAppointmentReport
            // Hidden fields: ReportType, DateFrom, DateTo, PhysicianId, AppointmentTypeId, LocationId
            const fromDate = dateFrom || "08/10/2021";
            const toDate = dateTo || new Date().toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" });

            logParts.push(`Date range: ${fromDate} to ${toDate}`);

            // Get physicians list to include all providers
            let physicianId = "";
            try {
              const { body: physBody } = await ajaxFetch("/User/Scheduler/GetPhysiciansForRunReport");
              const physicians = JSON.parse(physBody);
              if (Array.isArray(physicians) && physicians.length > 0) {
                physicianId = physicians.map((p: any) => p.Value || p.Id || "").filter(Boolean).join(",");
                logParts.push(`Physicians found: ${physicians.length} (IDs: ${physicianId})`);
              }
            } catch { /* ignore */ }

            // Get appointment types
            let appointmentTypeId = "";
            try {
              const { body: atBody } = await ajaxFetch("/User/Scheduler/GetAppointmentTypes");
              const types = JSON.parse(atBody);
              if (Array.isArray(types) && types.length > 0) {
                logParts.push(`Appointment types: ${types.length}`);
              }
            } catch { /* ignore */ }

            // Attempt 1: POST form to ExportAppointmentReport (matching the discovered form)
            const formParams = new URLSearchParams({
              ReportType: "CompletedVisits",
              DateFrom: fromDate,
              DateTo: toDate,
              PhysicianId: physicianId || "0",
              AppointmentTypeId: appointmentTypeId,
              LocationId: "",
            });

            logParts.push(`Attempt 1: POST /User/Scheduler/ExportAppointmentReport (form submit)...`);
            try {
              const { response: expRes, body: expBody } = await fetchWithCookies(
                `${BASE_URL}/User/Scheduler/ExportAppointmentReport`,
                {
                  method: "POST",
                  headers: { "Content-Type": "application/x-www-form-urlencoded" },
                  body: formParams.toString(),
                }
              );
              logParts.push(`Form POST: status=${expRes.status} length=${expBody.length} type=${expRes.headers.get("content-type") || "unknown"}`);

              if (expBody.length > 50 && !expBody.includes("<!DOCTYPE") && !expBody.includes("<html")) {
                csvContent = expBody;
                rowCount = csvContent.split("\n").length - 1;
                logParts.push(`✅ Appointments: ${rowCount} rows`);
              } else {
                logParts.push(`Preview: ${expBody.substring(0, 500)}`);
              }
            } catch (err: any) {
              logParts.push(`Form POST error: ${err.message}`);
            }

            // Attempt 2: AJAX version
            if (!csvContent) {
              logParts.push(`Attempt 2: AJAX /User/Scheduler/ExportAppointmentReport...`);
              const ajaxRes = await ajaxFetch("/User/Scheduler/ExportAppointmentReport", {
                method: "POST",
                headers: { "Content-Type": "application/x-www-form-urlencoded" },
                body: formParams.toString(),
              });
              logParts.push(`AJAX: status=${ajaxRes.status} type=${ajaxRes.contentType} length=${ajaxRes.body.length}`);

              if (ajaxRes.body.length > 50 && !ajaxRes.body.includes("<!DOCTYPE")) {
                csvContent = ajaxRes.body;
                rowCount = csvContent.split("\n").length - 1;
                logParts.push(`✅ Appointments (AJAX): ${rowCount} rows`);
              } else {
                logParts.push(`Preview: ${ajaxRes.body.substring(0, 500)}`);
                logParts.push(`⚠️ Appointments: Could not get export data.`);
              }
            }
            break;
          }

          // ==================== SOAP NOTES ====================
          case "soap_notes": {
            const patients = await getAllPatientIds();

            if (patients.length === 0) {
              logParts.push(`⚠️ SOAP Notes: No patients found`);
              break;
            }

            logParts.push(`Processing ${patients.length} patients for SOAP notes`);

            const soapResults: string[] = [];
            let processedCount = 0;

            for (const patient of patients) {
              if (!patient.id) continue;
              if (isTimingOut()) {
                logParts.push(`⏱️ SOAP Notes: Stopped at ${processedCount}/${patients.length} (timeout safety)`);
                break;
              }

              try {
                const { status, body: soapBody } = await ajaxFetch(
                  `/User/Scheduler/GetSopaNoteReportDetailsAsync?patientId=${patient.id}`
                );

                if (status === 200 && soapBody.length > 10) {
                  soapResults.push(`Patient ${patient.firstName} ${patient.lastName} (${patient.id}): ${soapBody.substring(0, 200)}`);
                  logParts.push(`SOAP ${patient.firstName} ${patient.lastName}: status=${status} length=${soapBody.length}`);
                }
              } catch (err: any) {
                logParts.push(`SOAP ${patient.firstName} ${patient.lastName} error: ${err.message}`);
              }

              processedCount++;
              if (processedCount % 20 === 0) {
                const newProgress = Math.round((processedCount / patients.length) * (100 / totalTypes));
                await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress + newProgress, 99), log_output: logParts.join("\n") }).eq("id", job.id);
              }

              await new Promise(r => setTimeout(r, 300));
            }

            if (soapResults.length > 0) {
              csvContent = "Patient,Details\n" + soapResults.map(r => `"${r.replace(/"/g, '""')}"`).join("\n");
              rowCount = soapResults.length;
              logParts.push(`✅ SOAP Notes: ${rowCount} patient records`);
            } else {
              logParts.push(`⚠️ SOAP Notes: No data retrieved.`);
            }
            break;
          }

          // ==================== FINANCIALS ====================
          case "financials": {
            const patients = await getAllPatientIds();

            if (patients.length === 0) {
              logParts.push(`⚠️ Financials: No patients found`);
              break;
            }

            logParts.push(`Processing ${patients.length} patients for financials`);

            const financialRows: Record<string, unknown>[] = [];
            let processedCount = 0;

            for (const patient of patients) {
              if (!patient.id) continue;
              if (isTimingOut()) {
                logParts.push(`⏱️ Financials: Stopped at ${processedCount}/${patients.length} (timeout safety)`);
                break;
              }

              try {
                await ajaxFetch(`/Patient/Patient/SetVisitIdInSession?patientId=${patient.id}`, { method: "POST" });

                const { status, body: billingBody } = await ajaxFetch(
                  `/Billing/PatientAccounting?patientId=${patient.id}`
                );

                if (status === 200 && billingBody.length > 100) {
                  const tableRegex = /<table[^>]*>[\s\S]*?<\/table>/gi;
                  const tables = billingBody.match(tableRegex) || [];
                  const cellRegex = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
                  let cellMatch;
                  const rowData: string[] = [];

                  for (const table of tables) {
                    while ((cellMatch = cellRegex.exec(table)) !== null) {
                      const text = cellMatch[1].replace(/<[^>]+>/g, "").trim();
                      if (text) rowData.push(text);
                    }
                  }

                  if (rowData.length > 0) {
                    financialRows.push({
                      PatientID: patient.id,
                      PatientName: `${patient.firstName} ${patient.lastName}`,
                      RawData: rowData.join(" | "),
                    });
                  }

                  logParts.push(`Financial ${patient.firstName} ${patient.lastName}: ${tables.length} tables, ${rowData.length} cells`);
                }
              } catch (err: any) {
                logParts.push(`Financial ${patient.firstName} ${patient.lastName} error: ${err.message}`);
              }

              processedCount++;
              if (processedCount % 20 === 0) {
                const newProgress = Math.round((processedCount / patients.length) * (100 / totalTypes));
                await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress + newProgress, 99), log_output: logParts.join("\n") }).eq("id", job.id);
              }

              await new Promise(r => setTimeout(r, 300));
            }

            if (financialRows.length > 0) {
              csvContent = jsonToCsv(financialRows);
              rowCount = financialRows.length;
              logParts.push(`✅ Financials: ${rowCount} patient records`);
            } else {
              logParts.push(`⚠️ Financials: No billing data retrieved`);
            }
            break;
          }
        }

        // Upload CSV if we have data
        if (csvContent) {
          hasAnyData = true;
          const filePath = `${userId}/${dataType}_${Date.now()}.csv`;
          const blob = new Blob([csvContent], { type: "text/csv" });

          const { error: uploadError } = await serviceClient.storage
            .from("scraped-data")
            .upload(filePath, blob, { contentType: "text/csv" });

          if (uploadError) {
            logParts.push(`❌ Upload error for ${dataType}: ${uploadError.message}`);
          } else {
            await serviceClient.from("scraped_data_results").insert({
              scrape_job_id: job.id,
              user_id: userId,
              data_type: dataType,
              file_path: filePath,
              row_count: rowCount,
            });
            logParts.push(`✅ Uploaded ${dataType}: ${filePath} (${rowCount} rows)`);
          }
        }

        progress += Math.round(100 / totalTypes);
        await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress, 99) }).eq("id", job.id);

      } catch (scrapeError: any) {
        logParts.push(`❌ Error scraping ${dataType}: ${scrapeError.message}`);
        console.error(`Error scraping ${dataType}:`, scrapeError);
      }
    }

    const finalStatus = hasAnyData ? "completed" : "completed";
    const finalMessage = hasAnyData ? null : "No data was retrieved. Check the log for details.";

    await serviceClient.from("scrape_jobs").update({
      status: finalStatus,
      progress: 100,
      log_output: logParts.join("\n"),
      error_message: finalMessage,
    }).eq("id", job.id);

    return new Response(JSON.stringify({ success: true, jobId: job.id, hasData: hasAnyData }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });

  } catch (error: any) {
    console.error("Error:", error);
    return new Response(JSON.stringify({ error: error.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
