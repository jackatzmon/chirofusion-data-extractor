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

    // ==================== DISCOVER MODE (COMPREHENSIVE) ====================
    if (mode === "discover") {
      const schedToken = { value: "" };

      // ===== 1. SCHEDULER PAGE: forms, hidden fields, tokens =====
      try {
        const { body: html } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
        logParts.push(`===== SCHEDULER PAGE: ${html.length} chars =====`);

        // Extract verification token
        const tokenMatch = html.match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/);
        schedToken.value = tokenMatch ? tokenMatch[1] : "";
        logParts.push(`Token: ${schedToken.value ? "YES" : "NONE"}`);

        // Extract ALL <form> elements with their full HTML (action, method, hidden inputs)
        const formRegex = /<form[^>]*id="([^"]*)"[^>]*>([\s\S]*?)<\/form>/gi;
        let formMatch;
        logParts.push(`\n===== ALL FORMS =====`);
        while ((formMatch = formRegex.exec(html)) !== null) {
          const formTag = formMatch[0].substring(0, formMatch[0].indexOf(">") + 1);
          const formBody = formMatch[2];
          // Extract hidden inputs
          const hiddenRegex = /<input[^>]*type="hidden"[^>]*/gi;
          const hiddens: string[] = [];
          let hm;
          while ((hm = hiddenRegex.exec(formBody)) !== null) hiddens.push(hm[0]);
          // Extract submit buttons
          const submitRegex = /<input[^>]*type="submit"[^>]*/gi;
          const submits: string[] = [];
          let sm;
          while ((sm = submitRegex.exec(formBody)) !== null) submits.push(sm[0]);
          logParts.push(`FORM#${formMatch[1]}: ${formTag}`);
          if (hiddens.length) logParts.push(`  Hiddens: ${hiddens.join(" | ")}`);
          if (submits.length) logParts.push(`  Submits: ${submits.join(" | ")}`);
        }

        // Also find forms without id
        const formNoIdRegex = /<form(?![^>]*id=)[^>]*>([\s\S]*?)<\/form>/gi;
        let fniMatch;
        while ((fniMatch = formNoIdRegex.exec(html)) !== null) {
          const tag = fniMatch[0].substring(0, fniMatch[0].indexOf(">") + 1);
          logParts.push(`FORM(no-id): ${tag.substring(0, 300)}`);
        }

        // Extract ALL select elements with their options
        const selectRegex = /<select[^>]*id="([^"]*)"[^>]*>([\s\S]*?)<\/select>/gi;
        let selMatch;
        logParts.push(`\n===== SELECT DROPDOWNS =====`);
        while ((selMatch = selectRegex.exec(html)) !== null) {
          const optRegex = /<option[^>]*value="([^"]*)"[^>]*>([^<]*)<\/option>/gi;
          let om;
          const opts: string[] = [];
          while ((om = optRegex.exec(selMatch[2])) !== null) opts.push(`${om[1]}=${om[2]}`);
          if (opts.length > 0) logParts.push(`SELECT#${selMatch[1]}: ${opts.join(" | ")}`);
        }

        // Fetch AppointmentReport JS and extract FULL function bodies
        const scriptSrcRegex = /<script[^>]*src\s*=\s*["']([^"']+)["']/gi;
        let srcMatch;
        const jsFiles: string[] = [];
        while ((srcMatch = scriptSrcRegex.exec(html)) !== null) {
          const src = srcMatch[1];
          if (src.includes("AppointmentReport") || src.includes("common") || src.includes("Patient") || src.includes("Billing")) {
            jsFiles.push(src.startsWith("http") ? src : `${BASE_URL}${src}`);
          }
        }

        for (const jsUrl of jsFiles) {
          try {
            const { body: jsBody } = await fetchWithCookies(jsUrl);
            const shortName = jsUrl.split("/").pop()?.split("?")[0] || jsUrl;
            logParts.push(`\n===== JS FILE: ${shortName} (${jsBody.length} chars) =====`);

            // Extract FULL function bodies for key functions
            const targetFns = [
              "ExportPatientList", "ExportPatientReport", "RunPatientReport",
              "ExportAppointmentReport", "RunAppointmentReport",
              "ExportToExcel", "PrintPatientReportDetails",
            ];

            for (const fnName of targetFns) {
              const idx = jsBody.indexOf(`function ${fnName}`);
              if (idx < 0) continue;

              // Extract full function body by counting braces
              let braceCount = 0;
              let started = false;
              let endIdx = idx;
              for (let i = idx; i < jsBody.length && i < idx + 5000; i++) {
                if (jsBody[i] === "{") { braceCount++; started = true; }
                if (jsBody[i] === "}") { braceCount--; }
                if (started && braceCount === 0) { endIdx = i + 1; break; }
              }
              const fullFn = jsBody.substring(idx, endIdx).replace(/\s+/g, " ");
              logParts.push(`\nFULL_FN ${fnName}: ${fullFn.substring(0, 2000)}`);
              if (fullFn.length > 2000) logParts.push(`  ...(truncated at 2000/${fullFn.length} chars)`);
            }

            // Find ALL URLs in this JS file
            const allUrlRegex = /["'](\/(Scheduler|Patient|Billing|User|Account|Home)[^"']{3,})["']/gi;
            let um;
            const urls = new Set<string>();
            while ((um = allUrlRegex.exec(jsBody)) !== null) urls.add(um[1]);
            if (urls.size > 0) {
              logParts.push(`\nALL URLs in ${shortName}:`);
              for (const u of [...urls].sort()) logParts.push(`  ${u}`);
            }
          } catch (e: any) {
            logParts.push(`JS fetch error ${jsUrl}: ${e.message}`);
          }
        }
      } catch (err: any) {
        logParts.push(`SCHEDULER ERROR: ${err.message}`);
      }

      // ===== 2. TEST KEY ENDPOINTS WITH DIFFERENT PARAMS =====
      logParts.push(`\n===== ENDPOINT TESTS =====`);

      // 2a: GetPatientReports with ReportType=1 (PatientList)
      const patientReportParams = [
        { ReportType: "1", BirthMonths: "", PatientStatus: "", PatientInsurance: "" },
        { ReportType: "1", BirthMonths: "", PatientStatus: "Active", PatientInsurance: "" },
        { ReportType: "1", BirthMonths: "", PatientStatus: "All", PatientInsurance: "" },
      ];

      for (const params of patientReportParams) {
        try {
          const res = await ajaxFetch("/Scheduler/Scheduler/GetPatientReports", {
            method: "POST",
            headers: {
              "Content-Type": "application/x-www-form-urlencoded",
              "RequestVerificationToken": schedToken.value,
            },
            body: new URLSearchParams(params).toString(),
          });
          logParts.push(`GetPatientReports(Status=${params.PatientStatus || "empty"}): status=${res.status} len=${res.body.length} type=${res.contentType}`);
          if (res.body.length > 0 && res.body.length < 2000) logParts.push(`  Body: ${res.body}`);
          else if (res.body.length >= 2000) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
        } catch (e: any) {
          logParts.push(`GetPatientReports error: ${e.message}`);
        }
      }

      // 2b: Try ExportPatientReports right after (report might be primed from above)
      try {
        const res = await ajaxFetch("/Scheduler/Scheduler/ExportPatientReports", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "RequestVerificationToken": schedToken.value,
          },
          body: new URLSearchParams({
            ReportType: "1", BirthMonths: "", PatientStatus: "All", PatientInsurance: "",
            __RequestVerificationToken: schedToken.value,
          }).toString(),
        });
        logParts.push(`ExportPatientReports: status=${res.status} len=${res.body.length} type=${res.contentType}`);
        if (res.body.length > 0) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
      } catch (e: any) {
        logParts.push(`ExportPatientReports error: ${e.message}`);
      }

      // 2c: Try the 500 error body from ExportPatientReports in detail
      try {
        const { response: rawRes, body: rawBody } = await fetchWithCookies(
          `${BASE_URL}/Scheduler/Scheduler/ExportPatientReports`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/x-www-form-urlencoded",
              "X-Requested-With": "XMLHttpRequest",
              "RequestVerificationToken": schedToken.value,
            },
            body: new URLSearchParams({
              ReportType: "1", BirthMonths: "", PatientStatus: "All", PatientInsurance: "",
            }).toString(),
          }
        );
        logParts.push(`ExportPatientReports(raw): status=${rawRes.status} len=${rawBody.length}`);
        logParts.push(`  Headers: ${JSON.stringify(Object.fromEntries(rawRes.headers.entries()))}`);
        logParts.push(`  Full body: ${rawBody.substring(0, 1000)}`);
      } catch (e: any) {
        logParts.push(`Raw export error: ${e.message}`);
      }

      // 2d: ExportPatientList - try multiple paths
      const exportPaths = [
        "/Patient/Patient/ExportPatientList",
        "/Scheduler/Scheduler/ExportPatientList",
        "/User/Scheduler/ExportPatientList",
        "/User/Patient/ExportPatientList",
      ];
      for (const path of exportPaths) {
        try {
          const res = await ajaxFetch(path, { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body: "" });
          logParts.push(`${path}: status=${res.status} len=${res.body.length} type=${res.contentType}`);
          if (res.status === 200 && res.body.length > 50 && !res.body.includes("<!DOCTYPE")) {
            logParts.push(`  ✅ FOUND DATA: ${res.body.substring(0, 300)}`);
          }
        } catch (e: any) {
          logParts.push(`${path}: error ${e.message}`);
        }
      }

      // 2e: Try GET on export paths too
      for (const path of exportPaths) {
        try {
          const { response: res, body } = await fetchWithCookies(`${BASE_URL}${path}`);
          const ct = res.headers.get("content-type") || "";
          const cd = res.headers.get("content-disposition") || "";
          logParts.push(`GET ${path}: status=${res.status} len=${body.length} type=${ct} disp=${cd}`);
          if (res.status === 200 && body.length > 50 && !body.includes("<!DOCTYPE")) {
            logParts.push(`  ✅ FOUND DATA: ${body.substring(0, 300)}`);
          }
        } catch (e: any) {
          logParts.push(`GET ${path}: error ${e.message}`);
        }
      }

      // ===== 3. APPOINTMENT REPORT ENDPOINTS =====
      logParts.push(`\n===== APPOINTMENT ENDPOINTS =====`);
      const today = new Date().toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" });

      // 3a: GetAppointmentReport
      const apptParams = {
        ReportType: "CompletedVisits",
        DateFrom: "08/10/2021",
        DateTo: today,
        PhysicianId: "0",
        AppointmentTypeId: "",
        LocationId: "",
      };

      const apptPaths = [
        "/Scheduler/Scheduler/GetAppointmentReport",
        "/User/Scheduler/GetAppointmentReport",
      ];
      for (const path of apptPaths) {
        try {
          const res = await ajaxFetch(path, {
            method: "POST",
            headers: {
              "Content-Type": "application/x-www-form-urlencoded",
              "RequestVerificationToken": schedToken.value,
            },
            body: new URLSearchParams(apptParams).toString(),
          });
          logParts.push(`${path}: status=${res.status} len=${res.body.length} type=${res.contentType}`);
          if (res.body.length > 0 && res.body.length < 1000) logParts.push(`  Body: ${res.body}`);
          else if (res.body.length >= 1000) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
        } catch (e: any) {
          logParts.push(`${path}: error ${e.message}`);
        }
      }

      // 3b: ExportAppointmentReport
      const apptExportPaths = [
        "/Scheduler/Scheduler/ExportAppointmentReport",
        "/User/Scheduler/ExportAppointmentReport",
      ];
      for (const path of apptExportPaths) {
        try {
          const res = await ajaxFetch(path, {
            method: "POST",
            headers: {
              "Content-Type": "application/x-www-form-urlencoded",
              "RequestVerificationToken": schedToken.value,
            },
            body: new URLSearchParams(apptParams).toString(),
          });
          logParts.push(`${path}: status=${res.status} len=${res.body.length} type=${res.contentType}`);
          if (res.body.length > 0) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
        } catch (e: any) {
          logParts.push(`${path}: error ${e.message}`);
        }
      }

      // ===== 4. BILLING/SOAP ENDPOINTS =====
      logParts.push(`\n===== BILLING & SOAP ENDPOINTS =====`);

      // 4a: Load billing page
      try {
        const { body: billingHtml } = await fetchWithCookies(`${BASE_URL}/Billing/`);
        logParts.push(`Billing page: ${billingHtml.length} chars`);
        // Extract forms
        const bFormRegex = /<form[^>]*>([\s\S]*?)<\/form>/gi;
        let bfm;
        while ((bfm = bFormRegex.exec(billingHtml)) !== null) {
          const tag = bfm[0].substring(0, bfm[0].indexOf(">") + 1);
          logParts.push(`  Billing form: ${tag.substring(0, 200)}`);
        }
      } catch (e: any) {
        logParts.push(`Billing page error: ${e.message}`);
      }

      // 4b: Home page (SOAP notes search)
      try {
        const { body: homeHtml } = await fetchWithCookies(`${BASE_URL}/`);
        logParts.push(`Home page: ${homeHtml.length} chars`);
      } catch (e: any) {
        logParts.push(`Home page error: ${e.message}`);
      }

      // 4c: Try patient search/SOAP endpoints
      try {
        const { body: patientJson } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
        const parsed = JSON.parse(patientJson);
        const patients = parsed.PatientData || parsed;
        logParts.push(`Patient JSON: ${Array.isArray(patients) ? patients.length : "N/A"} patients`);
        if (Array.isArray(patients) && patients.length > 0) {
          logParts.push(`  All keys: ${Object.keys(patients[0]).join(", ")}`);
          logParts.push(`  Sample: ${JSON.stringify(patients[0])}`);

          // Try SOAP/Medical File endpoints for first patient
          const pid = patients[0].I;
          if (pid) {
            const soapPaths = [
              `/User/Scheduler/GetSopaNoteReportDetailsAsync?patientId=${pid}`,
              `/Patient/Patient/GetMedicalFileList?patientId=${pid}`,
              `/Patient/Patient/GetPatientMedicalFile?patientId=${pid}`,
              `/Patient/Patient/ExportMedicalFile?patientId=${pid}`,
              `/User/Patient/MedicalFile?patientId=${pid}`,
            ];
            for (const sp of soapPaths) {
              try {
                const res = await ajaxFetch(sp);
                logParts.push(`${sp}: status=${res.status} len=${res.body.length} type=${res.contentType}`);
                if (res.status === 200 && res.body.length > 0 && res.body.length < 1000) logParts.push(`  Body: ${res.body.substring(0, 500)}`);
              } catch (e: any) {
                logParts.push(`${sp}: error ${e.message}`);
              }
            }
          }
        }
      } catch (e: any) {
        logParts.push(`Patient JSON error: ${e.message}`);
      }

      // ===== 5. WAIT 20s AND TRY EXPORT AGAIN (report might have primed) =====
      logParts.push(`\n===== WAITING 20s FOR REPORT GENERATION =====`);
      await new Promise(r => setTimeout(r, 20000));

      try {
        const res = await ajaxFetch("/Scheduler/Scheduler/ExportPatientReports", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "RequestVerificationToken": schedToken.value,
          },
          body: new URLSearchParams({
            ReportType: "1", BirthMonths: "", PatientStatus: "", PatientInsurance: "",
          }).toString(),
        });
        logParts.push(`ExportPatientReports(after 20s): status=${res.status} len=${res.body.length} type=${res.contentType}`);
        if (res.body.length > 0) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
      } catch (e: any) {
        logParts.push(`Export after wait error: ${e.message}`);
      }

      // Also try appointment export after the wait
      try {
        const res = await ajaxFetch("/Scheduler/Scheduler/ExportAppointmentReport", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "RequestVerificationToken": schedToken.value,
          },
          body: new URLSearchParams(apptParams).toString(),
        });
        logParts.push(`ExportAppointmentReport(after 20s): status=${res.status} len=${res.body.length} type=${res.contentType}`);
        if (res.body.length > 0) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
      } catch (e: any) {
        logParts.push(`Appt export after wait error: ${e.message}`);
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

    // Helper: extract __RequestVerificationToken from page HTML
    function extractVerifToken(html: string): string {
      const m = html.match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/);
      return m ? m[1] : "";
    }

        switch (dataType) {
          // ==================== DEMOGRAPHICS ====================
          case "demographics": {
            let found = false;

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

            // ===== Step 1: Load Scheduler page, scan INLINE scripts for function bodies =====
            logParts.push(`Step 1: Loading Scheduler page...`);
            const { body: schedHtml } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
            const schedToken = extractVerifToken(schedHtml);
            logParts.push(`Scheduler: ${schedHtml.length} chars, token: ${schedToken ? "YES" : "NONE"}`);

            // Scan INLINE <script> blocks for RunPatientReport, ExportPatientReport, ExportPatientList
            const inlineScriptRegex = /<script(?:\s[^>]*)?>(?!.*src)([\s\S]*?)<\/script>/gi;
            let inlineMatch;
            const targetFns = ["RunPatientReport", "ExportPatientReport", "ExportPatientList", "RunAppointmentReport", "ExportAppointmentReport"];
            while ((inlineMatch = inlineScriptRegex.exec(schedHtml)) !== null) {
              const scriptContent = inlineMatch[1];
              for (const fnName of targetFns) {
                const idx = scriptContent.indexOf(`function ${fnName}`);
                if (idx >= 0) {
                  logParts.push(`INLINE ${fnName}: ${scriptContent.substring(idx, idx + 800).replace(/\s+/g, " ")}`);
                }
              }
              // Also find Kendo grid dataSource read URLs for report grids
              if (scriptContent.includes("patientReportGrid") || scriptContent.includes("PatientReport") || scriptContent.includes("appointmentReportGrid")) {
                const readUrlMatch = scriptContent.match(/["']url["']\s*:\s*["']([^"']*(?:Report|Patient)[^"']*)["']/gi);
                if (readUrlMatch) {
                  for (const m of readUrlMatch) {
                    logParts.push(`INLINE_GRID_URL: ${m}`);
                  }
                }
                // Look for grid initialization with read transport
                const gridInitRegex = /kendoGrid\s*\(\s*\{[\s\S]{0,2000}?read[\s\S]{0,500}?url[^"']*["']([^"']+)["']/gi;
                let gridMatch;
                while ((gridMatch = gridInitRegex.exec(scriptContent)) !== null) {
                  logParts.push(`KENDO_GRID_READ: ${gridMatch[1]}`);
                }
              }
            }

            // Also fetch AppointmentReport JS specifically (contains report functions)
            const scriptSrcRegex = /<script[^>]*src\s*=\s*["']([^"']+)["']/gi;
            let scriptMatch;
            while ((scriptMatch = scriptSrcRegex.exec(schedHtml)) !== null) {
              const src = scriptMatch[1];
              if (src.includes("AppointmentReport") || src.includes("common")) {
                const fullUrl = src.startsWith("http") ? src : `${BASE_URL}${src}`;
                try {
                  const { body: jsBody } = await fetchWithCookies(fullUrl);
                  for (const fnName of targetFns) {
                    const idx = jsBody.indexOf(`function ${fnName}`);
                    if (idx >= 0) {
                      logParts.push(`FOUND ${fnName} in ${src.split("/").pop()}: ${jsBody.substring(idx, idx + 800).replace(/\s+/g, " ")}`);
                    }
                  }
                  // Find Kendo grid read URLs
                  const gridReadRegex = /["']([^"']*(?:GetPatientReport|GetAppointmentReport|PatientReport)[^"']*)["']/gi;
                  let urlMatch;
                  while ((urlMatch = gridReadRegex.exec(jsBody)) !== null) {
                    logParts.push(`URL in ${src.split("/").pop()}: ${urlMatch[1]}`);
                  }
                } catch (e: any) {
                  logParts.push(`JS fetch error: ${e.message}`);
                }
              }
            }

            // ===== Step 2: Extract #exportPatientListCsv form action =====
            const formActionMatch = schedHtml.match(/<form[^>]*id\s*=\s*["']exportPatientListCsv["'][^>]*action\s*=\s*["']([^"']+)["']/i)
              || schedHtml.match(/<form[^>]*action\s*=\s*["']([^"']+)["'][^>]*id\s*=\s*["']exportPatientListCsv["']/i);
            const csvFormAction = formActionMatch ? formActionMatch[1] : null;
            logParts.push(`exportPatientListCsv form action: ${csvFormAction || "NOT FOUND"}`);

            // Also extract hidden inputs from that form
            if (csvFormAction) {
              const formFullMatch = schedHtml.match(/<form[^>]*id\s*=\s*["']exportPatientListCsv["'][^>]*>([\s\S]*?)<\/form>/i);
              if (formFullMatch) {
                const hiddenRegex = /<input[^>]*type\s*=\s*["']hidden["'][^>]*/gi;
                let hm;
                while ((hm = hiddenRegex.exec(formFullMatch[1])) !== null) {
                  logParts.push(`  Form hidden: ${hm[0]}`);
                }
              }
            }

            // ===== Step 2a: Try #exportPatientListCsv form submission first =====
            if (csvFormAction) {
              logParts.push(`Step 2a: Trying exportPatientListCsv form POST to ${csvFormAction}...`);
              try {
                const formRes = await ajaxFetch(csvFormAction, {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "RequestVerificationToken": schedToken,
                  },
                  body: new URLSearchParams({ __RequestVerificationToken: schedToken }).toString(),
                });
                logParts.push(`  Form POST: status=${formRes.status} len=${formRes.body.length} type=${formRes.contentType}`);
                if (formRes.status === 200 && formRes.body.length > 100 && !formRes.body.includes("<!DOCTYPE")) {
                  csvContent = formRes.body;
                  rowCount = csvContent.split("\n").filter((l: string) => l.trim()).length - 1;
                  logParts.push(`✅ Demographics from form: ${rowCount} rows`);
                  found = true;
                } else if (formRes.body.length > 0) {
                  logParts.push(`  Form response preview: ${formRes.body.substring(0, 500)}`);
                }
              } catch (e: any) {
                logParts.push(`  Form POST error: ${e.message}`);
              }
            }

            // ===== Step 2b: Trigger GetPatientReports with EMPTY PatientStatus (= no filter = all) =====
            if (!found) {
              logParts.push(`Step 2b: Trigger GetPatientReports (empty PatientStatus = all)...`);
              const reportData = {
                ReportType: "1",
                BirthMonths: "",
                PatientStatus: "",
                PatientInsurance: "",
              };

              try {
                const triggerRes = await ajaxFetch("/Scheduler/Scheduler/GetPatientReports", {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "RequestVerificationToken": schedToken,
                  },
                  body: new URLSearchParams(reportData).toString(),
                });
                logParts.push(`  Trigger: status=${triggerRes.status} len=${triggerRes.body.length} type=${triggerRes.contentType}`);
                if (triggerRes.body.length > 0 && triggerRes.body.length < 2000) {
                  logParts.push(`  Trigger body: ${triggerRes.body}`);
                } else if (triggerRes.body.length >= 2000) {
                  logParts.push(`  Trigger body preview: ${triggerRes.body.substring(0, 1000)}`);
                  // If trigger itself returns data (Kendo grid JSON), try to use it
                  try {
                    const triggerData = JSON.parse(triggerRes.body);
                    const items = triggerData.Data || triggerData.data || (Array.isArray(triggerData) ? triggerData : null);
                    if (items && Array.isArray(items) && items.length > 0) {
                      csvContent = jsonToCsv(items);
                      rowCount = items.length;
                      logParts.push(`✅ Demographics from trigger response: ${rowCount} patients`);
                      found = true;
                    }
                  } catch { /* not JSON */ }
                }

                // Poll ExportPatientReports
                if (!found) {
                  const MAX_RETRIES = 6;
                  const RETRY_INTERVAL_MS = 5000;

                  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                    if (isTimingOut()) { logParts.push(`⏱️ Timeout at attempt ${attempt}`); break; }
                    logParts.push(`  Export attempt ${attempt}/${MAX_RETRIES} (waiting ${RETRY_INTERVAL_MS / 1000}s)...`);
                    await new Promise(r => setTimeout(r, RETRY_INTERVAL_MS));

                    try {
                      const expRes = await ajaxFetch("/Scheduler/Scheduler/ExportPatientReports", {
                        method: "POST",
                        headers: {
                          "Content-Type": "application/x-www-form-urlencoded",
                          "RequestVerificationToken": schedToken,
                        },
                        body: new URLSearchParams({
                          ...reportData,
                          __RequestVerificationToken: schedToken,
                        }).toString(),
                      });
                      logParts.push(`    status=${expRes.status} len=${expRes.body.length} type=${expRes.contentType}`);

                      // LOG THE ERROR BODY so we can debug
                      if (expRes.status >= 400) {
                        logParts.push(`    ERROR BODY: ${expRes.body.substring(0, 500)}`);
                        continue;
                      }

                      if (expRes.status === 200 && expRes.body.length > 50 && !expRes.body.includes("<!DOCTYPE") && !expRes.body.includes("<html")) {
                        csvContent = expRes.body;
                        rowCount = csvContent.split("\n").filter((l: string) => l.trim()).length - 1;
                        logParts.push(`✅ Demographics export: ${rowCount} rows (attempt ${attempt})`);
                        found = true;
                        break;
                      }
                    } catch (e: any) {
                      logParts.push(`    Attempt ${attempt} error: ${e.message}`);
                    }
                  }
                }
              } catch (e: any) {
                logParts.push(`  Trigger error: ${e.message}`);
              }
            }

            // ===== Step 2c: Try exportPatientListCsv form AFTER priming the report =====
            if (!found && csvFormAction) {
              logParts.push(`Step 2c: Retrying form POST after report primed...`);
              try {
                const formRes = await ajaxFetch(csvFormAction, {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "RequestVerificationToken": schedToken,
                  },
                  body: new URLSearchParams({ __RequestVerificationToken: schedToken }).toString(),
                });
                logParts.push(`  Form POST: status=${formRes.status} len=${formRes.body.length} type=${formRes.contentType}`);
                if (formRes.status === 200 && formRes.body.length > 100 && !formRes.body.includes("<!DOCTYPE")) {
                  csvContent = formRes.body;
                  rowCount = csvContent.split("\n").filter((l: string) => l.trim()).length - 1;
                  logParts.push(`✅ Demographics from form (post-prime): ${rowCount} rows`);
                  found = true;
                } else if (formRes.body.length > 0) {
                  logParts.push(`  Response preview: ${formRes.body.substring(0, 500)}`);
                }
              } catch (e: any) {
                logParts.push(`  Form POST error: ${e.message}`);
              }
            }

            // ===== Step 2d: Try Kendo grid read endpoint directly =====
            if (!found) {
              const kendoPaths = [
                "/Scheduler/Scheduler/GetPatientListReportData",
                "/Scheduler/Scheduler/PatientListReportData",
                "/Scheduler/Scheduler/GetPatientReportData",
              ];
              for (const kp of kendoPaths) {
                if (found || isTimingOut()) break;
                try {
                  const res = await ajaxFetch(kp, {
                    method: "POST",
                    headers: {
                      "Content-Type": "application/x-www-form-urlencoded",
                      "RequestVerificationToken": schedToken,
                    },
                    body: new URLSearchParams({
                      take: "5000", skip: "0", page: "1", pageSize: "5000",
                    }).toString(),
                  });
                  logParts.push(`Kendo ${kp}: status=${res.status} len=${res.body.length} type=${res.contentType}`);
                  if (res.status === 200 && res.contentType.includes("json") && res.body.length > 50) {
                    logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
                    try {
                      const data = JSON.parse(res.body);
                      const items = data.Data || data.data || (Array.isArray(data) ? data : null);
                      if (items && Array.isArray(items) && items.length > 0) {
                        csvContent = jsonToCsv(items);
                        rowCount = items.length;
                        logParts.push(`✅ Demographics from Kendo: ${rowCount} patients`);
                        found = true;
                      }
                    } catch { /* not JSON */ }
                  }
                } catch (e: any) {
                  logParts.push(`Kendo ${kp}: error ${e.message}`);
                }
              }
            }

            // ===== Fallback: JSON endpoint (partial data but reliable) =====
            if (!found) {
              logParts.push(`Step 3: JSON fallback /Patient/Patient/GetAllPatientForLocalStorage...`);
              try {
                const { body, contentType } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
                logParts.push(`JSON endpoint: type=${contentType} length=${body.length}`);
                if (contentType.includes("application/json") && body.length > 10) {
                  const parsed = JSON.parse(body);
                  const patients = parsed.PatientData || parsed;
                  if (Array.isArray(patients) && patients.length > 0) {
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

            // Step 1: Trigger the appointment report via GetAppointmentReport
            const { body: schedHtmlAppt } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
            const apptToken = extractVerifToken(schedHtmlAppt);

            const apptFormData = new URLSearchParams({
              ReportType: "CompletedVisits",
              DateFrom: fromDate,
              DateTo: toDate,
              PhysicianId: physicianId || "0",
              AppointmentTypeId: appointmentTypeId,
              LocationId: "",
            });

            logParts.push(`Step 1: Trigger GetAppointmentReport...`);
            try {
              const triggerRes = await ajaxFetch("/Scheduler/Scheduler/GetAppointmentReport", {
                method: "POST",
                headers: {
                  "Content-Type": "application/x-www-form-urlencoded",
                  "RequestVerificationToken": apptToken,
                },
                body: apptFormData.toString(),
              });
              logParts.push(`Trigger: status=${triggerRes.status} len=${triggerRes.body.length}`);
            } catch (e: any) {
              logParts.push(`Trigger error: ${e.message}`);
            }

            // Step 2: Poll ExportAppointmentReport with retries (don't press OK)
            const MAX_APPT_RETRIES = 8;
            const APPT_RETRY_MS = 4000;

            for (let attempt = 1; attempt <= MAX_APPT_RETRIES; attempt++) {
              if (isTimingOut()) {
                logParts.push(`⏱️ Appt export: timeout at attempt ${attempt}`);
                break;
              }

              logParts.push(`Export attempt ${attempt}/${MAX_APPT_RETRIES} (waiting ${APPT_RETRY_MS / 1000}s)...`);
              await new Promise(r => setTimeout(r, APPT_RETRY_MS));

              try {
                const expRes = await ajaxFetch("/Scheduler/Scheduler/ExportAppointmentReport", {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "RequestVerificationToken": apptToken,
                  },
                  body: new URLSearchParams({
                    ...Object.fromEntries(apptFormData),
                    __RequestVerificationToken: apptToken,
                  }).toString(),
                });
                logParts.push(`  status=${expRes.status} len=${expRes.body.length} type=${expRes.contentType}`);

                if (expRes.status === 200 && expRes.body.length > 50 && !expRes.body.includes("<!DOCTYPE") && !expRes.body.includes("<html")) {
                  csvContent = expRes.body;
                  rowCount = csvContent.split("\n").filter((l: string) => l.trim()).length - 1;
                  logParts.push(`✅ Appointments export: ${rowCount} rows (attempt ${attempt})`);
                  break;
                }

                if (expRes.status >= 400) {
                  logParts.push(`  Not ready yet (${expRes.status}), retrying...`);
                }
              } catch (e: any) {
                logParts.push(`  Attempt ${attempt} error: ${e.message}`);
              }
            }

            if (!csvContent) {
              logParts.push(`⚠️ Appointments: Export polling exhausted after ${MAX_APPT_RETRIES} attempts`);
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
