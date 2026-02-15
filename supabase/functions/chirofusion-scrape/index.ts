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

    const { data: claimsData, error: claimsError } = await supabase.auth.getClaims(authHeader.replace("Bearer ", ""));
    if (claimsError || !claimsData?.claims) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const userId = claimsData.claims.sub as string;
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
      const pages = [
        { name: "Scheduler", url: `${BASE_URL}/User/Scheduler` },
        { name: "Billing", url: `${BASE_URL}/Billing/` },
      ];

      for (const page of pages) {
        try {
          const { body: html, finalUrl } = await fetchWithCookies(page.url);
          const isLogin = html.includes("txtLoginUserName");
          logParts.push(`\n===== ${page.name.toUpperCase()} =====`);
          logParts.push(`Final URL: ${finalUrl} | Is Login: ${isLogin} | Length: ${html.length}`);
          logParts.push(extractPageStructure(html));
          logParts.push(`\n--- HTML (first 5000) ---\n${html.substring(0, 5000)}`);
        } catch (err: any) {
          logParts.push(`\n===== ${page.name.toUpperCase()} ERROR =====\n${err.message}`);
        }
      }

      // Also test key AJAX endpoints
      const ajaxTests = [
        "/Patient/Patient/GetAllPatientForLocalStorage",
        "/User/Scheduler/GetPhysiciansForRunReport",
        "/Dashboard/Dashboard/GetAllProviders",
      ];
      for (const endpoint of ajaxTests) {
        try {
          const { status, body, contentType } = await ajaxFetch(endpoint);
          logParts.push(`\nAJAX ${endpoint}: status=${status} type=${contentType} length=${body.length}`);
          logParts.push(`Body (first 1000): ${body.substring(0, 1000)}`);
        } catch (err: any) {
          logParts.push(`\nAJAX ${endpoint} ERROR: ${err.message}`);
        }
      }

      await serviceClient.from("scrape_jobs").update({ status: "completed", progress: 100, log_output: logParts.join("\n") }).eq("id", job.id);
      return new Response(JSON.stringify({ success: true, jobId: job.id, mode: "discover" }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ==================== SCRAPE MODE ====================
    console.log("Starting scrape mode...");
    let progress = 0;
    const totalTypes = dataTypes.length;
    let hasAnyData = false;

    for (const dataType of dataTypes) {
      try {
        console.log(`Scraping ${dataType}...`);
        logParts.push(`\n--- Scraping: ${dataType} ---`);

        let csvContent = "";
        let rowCount = 0;

        switch (dataType) {
          // ==================== DEMOGRAPHICS ====================
          case "demographics": {
            // Primary: Use the bulk export endpoint which returns ALL patients
            logParts.push(`Trying ExportPatientList (bulk export)...`);
            const exportRes = await ajaxFetch("/Patient/Patient/ExportPatientList", {
              method: "POST",
              headers: { "Content-Type": "application/x-www-form-urlencoded" },
              body: "",
            });
            logParts.push(`ExportPatientList: status=${exportRes.status} type=${exportRes.contentType} length=${exportRes.body.length}`);
            logParts.push(`Export preview (first 500): ${exportRes.body.substring(0, 500)}`);

            if (exportRes.body.length > 100 && !exportRes.body.includes("<!DOCTYPE") && !exportRes.body.includes("<html")) {
              csvContent = exportRes.body;
              rowCount = csvContent.split("\n").length - 1;
              logParts.push(`✅ Demographics (export): ${rowCount} rows`);
            } else {
              // Fallback: JSON endpoint (may only return subset)
              logParts.push(`Export failed, falling back to JSON endpoint...`);
              const { status, body, contentType } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
              logParts.push(`Patient API: status=${status} type=${contentType} length=${body.length}`);

              if (contentType.includes("application/json")) {
                const parsed = JSON.parse(body);
                const patients = parsed.PatientData || parsed;

                if (Array.isArray(patients) && patients.length > 0) {
                  const expanded = patients.map((p: any) => ({
                    PatientID: p.I || "",
                    FirstName: p.F || "",
                    LastName: p.L || "",
                    Nickname: p.N || "",
                    DateOfBirth: p.DOB || parseNetDate(p.D ? `/Date(${p.D})/` : null),
                    HomePhone: p.HP || "",
                    MobilePhone: p.MP || "",
                    WorkPhone: p.WP || "",
                    Email: p.E || "",
                    IsDeactivated: p.ID ? "Yes" : "No",
                  }));

                  csvContent = jsonToCsv(expanded);
                  rowCount = expanded.length;
                  logParts.push(`✅ Demographics (JSON): ${rowCount} patients (may be partial)`);
                }
              }
            }
            break;
          }

          // ==================== APPOINTMENTS ====================
          case "appointments": {
            // Use the export endpoint with date range
            const fromDate = dateFrom || "08/10/2021";
            const toDate = dateTo || new Date().toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" });

            logParts.push(`Date range: ${fromDate} to ${toDate}`);

            // First get physicians for the report
            const { body: physBody } = await ajaxFetch("/User/Scheduler/GetPhysiciansForRunReport");
            let physicianId = "";
            try {
              const physicians = JSON.parse(physBody);
              if (Array.isArray(physicians) && physicians.length > 0) {
                physicianId = physicians[0].Value || physicians[0].Id || physicians[0].value || "";
                logParts.push(`Using physician: ${JSON.stringify(physicians[0])}`);
              }
            } catch { /* ignore parse errors */ }

            // Try ExportAppointmentReport
            const params = new URLSearchParams({
              fromDate,
              toDate,
              physicianId: physicianId || "0",
              reportType: "CompletedVisits",
            });

            const exportRes = await ajaxFetch(`/User/Scheduler/ExportAppointmentReport?${params.toString()}`);
            logParts.push(`ExportAppointmentReport: status=${exportRes.status} type=${exportRes.contentType} length=${exportRes.body.length}`);

            if (exportRes.body.length > 50 && !exportRes.body.includes("<!DOCTYPE") && !exportRes.body.includes("<html")) {
              csvContent = exportRes.body;
              rowCount = csvContent.split("\n").length - 1;
              logParts.push(`✅ Appointments: ${rowCount} rows`);
            } else {
              // Try POST with form data
              const postRes = await ajaxFetch("/User/Scheduler/ExportAppointmentReport", {
                method: "POST",
                headers: { "Content-Type": "application/x-www-form-urlencoded" },
                body: params.toString(),
              });
              logParts.push(`ExportAppointmentReport (POST): status=${postRes.status} type=${postRes.contentType} length=${postRes.body.length}`);
              logParts.push(`Response preview: ${postRes.body.substring(0, 500)}`);

              if (postRes.body.length > 50 && !postRes.body.includes("<!DOCTYPE")) {
                csvContent = postRes.body;
                rowCount = csvContent.split("\n").length - 1;
                logParts.push(`✅ Appointments (POST): ${rowCount} rows`);
              } else {
                logParts.push(`⚠️ Appointments: Could not get export data. Response logged for debugging.`);
              }
            }
            break;
          }

          // ==================== SOAP NOTES ====================
          case "soap_notes": {
            // Get patient list first
            const { body: patBody } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
            let patients: any[] = [];
            try {
              const parsed = JSON.parse(patBody);
              patients = parsed.PatientData || parsed;
            } catch { /* ignore */ }

            if (!Array.isArray(patients) || patients.length === 0) {
              logParts.push(`⚠️ SOAP Notes: No patients found`);
              break;
            }

            logParts.push(`Found ${patients.length} patients for SOAP notes`);

            // For each patient, try to get their SOAP note report
            const soapResults: string[] = [];
            let processedCount = 0;

            for (const patient of patients) {
              const patientId = patient.I;
              if (!patientId) continue;

              try {
                // Try the SOAP note report endpoint
                const { status, body: soapBody, contentType } = await ajaxFetch(
                  `/User/Scheduler/GetSopaNoteReportDetailsAsync?patientId=${patientId}`
                );

                if (status === 200 && soapBody.length > 10) {
                  soapResults.push(`Patient ${patient.F} ${patient.L} (${patientId}): ${soapBody.substring(0, 200)}`);
                  logParts.push(`SOAP ${patient.F} ${patient.L}: status=${status} length=${soapBody.length}`);
                }
              } catch (err: any) {
                logParts.push(`SOAP ${patient.F} ${patient.L} error: ${err.message}`);
              }

              processedCount++;
              const newProgress = Math.round((processedCount / patients.length) * (100 / totalTypes));
              await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress + newProgress, 99) }).eq("id", job.id);

              // Avoid hammering the server
              await new Promise(r => setTimeout(r, 500));
            }

            if (soapResults.length > 0) {
              csvContent = "Patient,Details\n" + soapResults.map(r => `"${r.replace(/"/g, '""')}"`).join("\n");
              rowCount = soapResults.length;
              logParts.push(`✅ SOAP Notes: ${rowCount} patient records`);
            } else {
              logParts.push(`⚠️ SOAP Notes: No data retrieved. May need PDF export approach.`);
            }
            break;
          }

          // ==================== FINANCIALS ====================
          case "financials": {
            // Get patient list first
            const { body: patBody } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
            let patients: any[] = [];
            try {
              const parsed = JSON.parse(patBody);
              patients = parsed.PatientData || parsed;
            } catch { /* ignore */ }

            if (!Array.isArray(patients) || patients.length === 0) {
              logParts.push(`⚠️ Financials: No patients found`);
              break;
            }

            logParts.push(`Found ${patients.length} patients for financials`);

            // Navigate to billing and try patient accounting for each patient
            const financialRows: Record<string, unknown>[] = [];
            let processedCount = 0;

            for (const patient of patients) {
              const patientId = patient.I;
              if (!patientId) continue;

              try {
                // Set patient in session
                await ajaxFetch(`/Patient/Patient/SetVisitIdInSession?patientId=${patientId}`, { method: "POST" });

                // Try to get patient accounting data
                const { status, body: billingBody, contentType } = await ajaxFetch(
                  `/Billing/PatientAccounting?patientId=${patientId}`
                );

                if (status === 200 && billingBody.length > 100) {
                  // Parse HTML tables for financial data
                  const tableRegex = /<table[^>]*>[\s\S]*?<\/table>/gi;
                  const tables = billingBody.match(tableRegex) || [];

                  // Extract text content from table cells
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
                      PatientID: patientId,
                      PatientName: `${patient.F} ${patient.L}`,
                      RawData: rowData.join(" | "),
                    });
                  }

                  logParts.push(`Financial ${patient.F} ${patient.L}: ${tables.length} tables, ${rowData.length} cells`);
                }
              } catch (err: any) {
                logParts.push(`Financial ${patient.F} ${patient.L} error: ${err.message}`);
              }

              processedCount++;
              const newProgress = Math.round((processedCount / patients.length) * (100 / totalTypes));
              await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress + newProgress, 99) }).eq("id", job.id);

              await new Promise(r => setTimeout(r, 500));
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
