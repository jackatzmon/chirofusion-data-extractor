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
            // Primary: Use Patient Reports "Export To Excel" (Schedule > Patient Reports)
            // This returns ALL 2321+ patients with full demographics
            logParts.push(`Trying ExportPatientReports (Patient Reports > Export To Excel)...`);

            // Try the export endpoint that backs the "Export To Excel" button
            const exportRes = await ajaxFetch("/User/Scheduler/ExportPatientReports", {
              method: "POST",
              headers: { "Content-Type": "application/x-www-form-urlencoded" },
              body: new URLSearchParams({
                reportType: "PatientList",
                patientStatus: "All",
              }).toString(),
            });
            logParts.push(`ExportPatientReports POST: status=${exportRes.status} type=${exportRes.contentType} length=${exportRes.body.length}`);
            logParts.push(`Preview: ${exportRes.body.substring(0, 500)}`);

            if (exportRes.body.length > 200 && !exportRes.body.includes("<!DOCTYPE") && !exportRes.body.includes("<html")) {
              csvContent = exportRes.body;
              rowCount = csvContent.split("\n").length - 1;
              logParts.push(`✅ Demographics (ExportPatientReports): ${rowCount} rows`);
            } else {
              // Try GET variant
              const getRes = await ajaxFetch("/User/Scheduler/ExportPatientReports?reportType=PatientList&patientStatus=All");
              logParts.push(`ExportPatientReports GET: status=${getRes.status} type=${getRes.contentType} length=${getRes.body.length}`);
              logParts.push(`GET Preview: ${getRes.body.substring(0, 500)}`);

              if (getRes.body.length > 200 && !getRes.body.includes("<!DOCTYPE") && !getRes.body.includes("<html")) {
                csvContent = getRes.body;
                rowCount = csvContent.split("\n").length - 1;
                logParts.push(`✅ Demographics (GET): ${rowCount} rows`);
              } else {
                // Try ExportPatientList
                const listRes = await ajaxFetch("/Patient/Patient/ExportPatientList", {
                  method: "POST",
                  headers: { "Content-Type": "application/x-www-form-urlencoded" },
                  body: "",
                });
                logParts.push(`ExportPatientList: status=${listRes.status} type=${listRes.contentType} length=${listRes.body.length}`);
                logParts.push(`Preview: ${listRes.body.substring(0, 500)}`);

                if (listRes.body.length > 200 && !listRes.body.includes("<!DOCTYPE") && !listRes.body.includes("<html")) {
                  csvContent = listRes.body;
                  rowCount = csvContent.split("\n").length - 1;
                  logParts.push(`✅ Demographics (ExportPatientList): ${rowCount} rows`);
                } else {
                  // Final fallback: JSON endpoint (partial)
                  const { body, contentType } = await ajaxFetch("/Patient/Patient/GetAllPatientForLocalStorage");
                  if (contentType.includes("application/json")) {
                    const patients = (JSON.parse(body)).PatientData || JSON.parse(body);
                    if (Array.isArray(patients) && patients.length > 0) {
                      const expanded = patients.map((p: any) => ({
                        PatientID: p.I || "", FirstName: p.F || "", LastName: p.L || "",
                        DateOfBirth: p.DOB || "", HomePhone: p.HP || "", MobilePhone: p.MP || "",
                        Email: p.E || "",
                      }));
                      csvContent = jsonToCsv(expanded);
                      rowCount = expanded.length;
                      logParts.push(`⚠️ Demographics (JSON fallback): ${rowCount} patients (partial)`);
                    }
                  }
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
              if (processedCount % 50 === 0) {
                const newProgress = Math.round((processedCount / patients.length) * (100 / totalTypes));
                await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress + newProgress, 99) }).eq("id", job.id);
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
              if (processedCount % 50 === 0) {
                const newProgress = Math.round((processedCount / patients.length) * (100 / totalTypes));
                await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress + newProgress, 99) }).eq("id", job.id);
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
