import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import * as XLSX from "https://esm.sh/xlsx@0.18.5";

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

// Mark stale running jobs as failed (in case previous runs were killed by CPU timeout)
async function cleanupStaleJobs(supabase: any) {
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  await supabase.from("scrape_jobs")
    .update({ status: "failed", error_message: "Job timed out. Please retry." })
    .eq("status", "running")
    .lt("created_at", oneHourAgo);
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

/** Parse CSV string back to JSON array */
function parseCsvLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
      else { inQuotes = !inQuotes; }
    } else if (ch === ',' && !inQuotes) {
      result.push(current); current = "";
    } else { current += ch; }
  }
  result.push(current);
  return result;
}

function csvToJson(csv: string): Record<string, string>[] {
  const lines = csv.split("\n").filter(l => l.trim());
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map(line => {
    const values = parseCsvLine(line);
    const obj: Record<string, string> = {};
    headers.forEach((h, i) => { obj[h] = values[i] || ""; });
    return obj;
  });
}


function parseNetDate(dateStr: string | null): string {
  if (!dateStr) return "";
  const match = dateStr.match(/\/Date\((-?\d+)\)\//);
  if (match) {
    const d = new Date(parseInt(match[1]));
    return d.toLocaleDateString("en-US");
  }
  return dateStr;
}

/** Parse ShowLedger HTML response to extract ledger table rows */
function parseLedgerHtml(html: string): Record<string, string>[] {
  const rows: Record<string, string>[] = [];

  // Extract all <tr> rows from the ledger table
  const trRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let trMatch;

  // First, try to find headers from <th> elements
  const headers: string[] = [];
  const theadMatch = html.match(/<thead[^>]*>([\s\S]*?)<\/thead>/i);
  if (theadMatch) {
    const thRegex = /<th[^>]*>([\s\S]*?)<\/th>/gi;
    let thMatch;
    while ((thMatch = thRegex.exec(theadMatch[1])) !== null) {
      // Strip HTML tags from header text
      const text = thMatch[1].replace(/<[^>]*>/g, "").trim();
      headers.push(text || `Column${headers.length}`);
    }
  }

  // If no headers found, use default ledger columns
  if (headers.length === 0) {
    headers.push("Date", "Description", "CPTCode", "Charges", "Payments", "Adjustments", "Balance");
  }

  // Extract <tbody> content if present, otherwise use full HTML
  const tbodyMatch = html.match(/<tbody[^>]*>([\s\S]*?)<\/tbody>/i);
  const bodyContent = tbodyMatch ? tbodyMatch[1] : html;

  while ((trMatch = trRegex.exec(bodyContent)) !== null) {
    const trContent = trMatch[1];
    // Skip header rows (already parsed above)
    if (trContent.includes("<th")) continue;

    const cells: string[] = [];
    const tdRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    let tdMatch;
    while ((tdMatch = tdRegex.exec(trContent)) !== null) {
      // Strip HTML tags and trim whitespace
      const text = tdMatch[1].replace(/<[^>]*>/g, "").replace(/&nbsp;/gi, " ").replace(/&amp;/gi, "&").trim();
      cells.push(text);
    }

    if (cells.length === 0) continue;

    // Map cells to headers
    const row: Record<string, string> = {};
    for (let c = 0; c < Math.min(cells.length, headers.length); c++) {
      row[headers[c]] = cells[c];
    }
    // If more cells than headers, append extras
    for (let c = headers.length; c < cells.length; c++) {
      row[`Extra${c}`] = cells[c];
    }
    rows.push(row);
  }

  return rows;
}

/** Generate a simple text-based PDF from lines of text. Returns raw PDF bytes. */
function generateTextPdf(title: string, lines: string[]): Uint8Array {
  const escPdf = (s: string) => s.replace(/\\/g, '\\\\').replace(/\(/g, '\\(').replace(/\)/g, '\\)');
  const LINES_PER_PAGE = 48;
  const LINE_HEIGHT = 13;
  const TITLE_HEIGHT = 18;
  const PAGE_WIDTH = 612;
  const PAGE_HEIGHT = 792;
  const MARGIN_LEFT = 50;
  const MARGIN_TOP = 740;

  // Split into pages
  const allLines = [title, "", ...lines];
  const pageGroups: string[][] = [];
  for (let i = 0; i < allLines.length; i += LINES_PER_PAGE) {
    pageGroups.push(allLines.slice(i, i + LINES_PER_PAGE));
  }
  if (pageGroups.length === 0) pageGroups.push([title]);

  // Build PDF content
  const parts: string[] = [];
  const offsets: number[] = [0]; // 0-index unused (PDF objects are 1-based)
  let pos = 0;

  function write(s: string) { parts.push(s); pos += s.length; }
  function startObj(n: number) { offsets[n] = pos; write(`${n} 0 obj\n`); }
  function endObj() { write("endobj\n\n"); }

  write("%PDF-1.4\n");

  // Obj 1: Catalog
  startObj(1);
  write("<< /Type /Catalog /Pages 2 0 R >>\n");
  endObj();

  // Obj 2: Pages (placeholder — we'll reference page objects)
  const numPages = pageGroups.length;
  // Font objects: 3 = Helvetica, 4 = Helvetica-Bold
  // Page objects start at 5, content objects interleaved
  // Page i -> obj (5 + i*2), Content i -> obj (5 + i*2 + 1) ... wait, let me just pre-compute

  // Reserve obj 2 for pages, obj 3 for font, obj 4 for bold font
  // Then pages at 5,7,9,... and contents at 6,8,10,...
  const pageObjNums: number[] = [];
  const contentObjNums: number[] = [];
  for (let p = 0; p < numPages; p++) {
    pageObjNums.push(5 + p * 2);
    contentObjNums.push(6 + p * 2);
  }
  const totalObjs = 5 + numPages * 2;

  // Obj 2: Pages
  startObj(2);
  const kids = pageObjNums.map(n => `${n} 0 R`).join(" ");
  write(`<< /Type /Pages /Kids [${kids}] /Count ${numPages} >>\n`);
  endObj();

  // Obj 3: Font (Helvetica)
  startObj(3);
  write("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\n");
  endObj();

  // Obj 4: Font (Helvetica-Bold)
  startObj(4);
  write("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>\n");
  endObj();

  // Pages and content streams
  for (let p = 0; p < numPages; p++) {
    const pageLines = pageGroups[p];

    // Build content stream
    let stream = "BT\n";
    let y = MARGIN_TOP;
    for (let l = 0; l < pageLines.length; l++) {
      const line = pageLines[l];
      const isTitle = (p === 0 && l === 0);
      if (isTitle) {
        stream += `/F2 14 Tf\n`;
        stream += `${MARGIN_LEFT} ${y} Td\n`;
        stream += `(${escPdf(line)}) Tj\n`;
        y -= TITLE_HEIGHT;
      } else {
        stream += `/F1 9 Tf\n`;
        stream += `${MARGIN_LEFT} ${y} Td\n`;
        stream += `(${escPdf(line)}) Tj\n`;
        y -= LINE_HEIGHT;
      }
    }
    stream += "ET\n";

    // Content object
    startObj(contentObjNums[p]);
    write(`<< /Length ${stream.length} >>\nstream\n${stream}endstream\n`);
    endObj();

    // Page object
    startObj(pageObjNums[p]);
    write(`<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${PAGE_WIDTH} ${PAGE_HEIGHT}] /Contents ${contentObjNums[p]} 0 R /Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> >>\n`);
    endObj();
  }

  // Cross-reference table
  const xrefPos = pos;
  write("xref\n");
  write(`0 ${totalObjs}\n`);
  write("0000000000 65535 f \n");
  for (let i = 1; i < totalObjs; i++) {
    write(`${String(offsets[i] || 0).padStart(10, '0')} 00000 n \n`);
  }

  write("trailer\n");
  write(`<< /Size ${totalObjs} /Root 1 0 R >>\n`);
  write("startxref\n");
  write(`${xrefPos}\n`);
  write("%%EOF\n");

  return new TextEncoder().encode(parts.join(""));
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

    // Cleanup any stale running jobs from previous crashed runs
    await cleanupStaleJobs(supabase);

    const body = await req.json();
    const { dataTypes = [], mode = "scrape", dateFrom, dateTo,
      // Batch continuation fields (set automatically by self-invocation)
      _batchJobId, _batchState
    } = body;

    const { data: creds, error: credsError } = await supabase
      .from("chirofusion_credentials")
      .select("*")
      .eq("user_id", userId)
      .maybeSingle();

    if (credsError || !creds) {
      return new Response(JSON.stringify({ error: "ChiroFusion credentials not found." }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const serviceClient = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    // If continuing a batch, reuse existing job; otherwise create new one
    let job: any;
    if (_batchJobId) {
      const { data: existingJob } = await serviceClient.from("scrape_jobs").select("*").eq("id", _batchJobId).single();
      if (!existingJob || existingJob.status !== "running") {
        return new Response(JSON.stringify({ error: "Batch job not found or already finished." }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }
      job = existingJob;
    } else {
      const jobDataTypes = mode === "discover" ? ["discovery"] : dataTypes;
      const { data: newJob, error: jobError } = await supabase
        .from("scrape_jobs")
        .insert({ user_id: userId, data_types: jobDataTypes, status: "running", mode })
        .select()
        .single();
      if (jobError) {
        return new Response(JSON.stringify({ error: jobError.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }
      job = newJob;

      // For non-discover modes, self-invoke immediately and return fast
      // so the browser doesn't timeout waiting for the scrape to finish
      if (mode === "scrape") {
        const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
        const fnUrl = `${supabaseUrl}/functions/v1/chirofusion-scrape`;
        fetch(fnUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": authHeader!,
          },
          body: JSON.stringify({
            dataTypes, mode, dateFrom, dateTo,
            _batchJobId: job.id,
            _batchState: { resumeIndex: 0, dataTypeIndex: 0 },
          }),
        }).catch(err => console.error("Initial self-invoke error:", err));
        // Wait briefly to ensure request is dispatched
        await new Promise(r => setTimeout(r, 2000));
        return new Response(JSON.stringify({ success: true, jobId: job.id, message: "Scrape started in background" }), {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }
    }

    let sessionCookies = "";
    // On resume, load logParts from DB instead of HTTP body
    const logParts: string[] = [];
    if (_batchJobId && job.log_output) {
      logParts.push(...job.log_output.split("\n"));
    }
    const startTime = Date.now();
    const MAX_RUNTIME_MS = 100_000; // 100s to leave room for cleanup + self-invoke

    function isTimingOut(): boolean {
      return (Date.now() - startTime) > MAX_RUNTIME_MS;
    }

    // Self-invoke to continue processing in a new batch
    // CRITICAL: Large data is stored in Supabase Storage to avoid batch_state JSONB size limits
    // which cause DB statement timeouts with 16MB+ payloads.
    async function saveLargeDataToStorage(key: string, data: any): Promise<void> {
      const path = `_batch_tmp/${job.id}/${key}.json`;
      const blob = new Blob([JSON.stringify(data)], { type: "application/json" });
      // Upsert: remove first then upload (storage doesn't support overwrite by default)
      await serviceClient.storage.from("scraped-data").remove([path]);
      await serviceClient.storage.from("scraped-data").upload(path, blob, { contentType: "application/json" });
    }

    async function loadLargeDataFromStorage(key: string, fallback: any = []): Promise<any> {
      const path = `_batch_tmp/${job.id}/${key}.json`;
      const { data, error } = await serviceClient.storage.from("scraped-data").download(path);
      if (error || !data) return fallback;
      try {
        return JSON.parse(await data.text());
      } catch { return fallback; }
    }

    async function selfInvoke(batchState: any) {
      logParts.push(`🔄 Batch timeout — continuing from patient ${batchState.resumeIndex}...`);
      
      // Save large data arrays to Storage instead of batch_state to avoid statement timeouts
      try {
        await Promise.all([
          saveLargeDataToStorage("collectedSheets", collectedSheets),
          saveLargeDataToStorage("soapIndex", soapIndex),
          saveLargeDataToStorage("financialsLedgerRows", batchState.financialsLedgerRows || []),
          saveLargeDataToStorage("appointmentsIndex", batchState.appointmentsIndex || []),
        ]);
      } catch (storageErr) {
        logParts.push(`⚠️ Storage save error: ${(storageErr as any).message}`);
      }

      // Only save minimal counters in batch_state (small JSONB)
      await serviceClient.from("scrape_jobs").update({
        log_output: logParts.join("\n"),
        batch_state: {
          resumeIndex: batchState.resumeIndex,
          pdfCount: batchState.pdfCount,
          searchFailed: batchState.searchFailed,
          withFiles: batchState.withFiles,
          skippedDefaultCase: batchState.skippedDefaultCase,
          dataTypeIndex: batchState.dataTypeIndex,
          ledgerFetched: batchState.ledgerFetched,
          ledgerEmpty: batchState.ledgerEmpty,
          ledgerSearchFailed: batchState.ledgerSearchFailed,
          apptPdfIndex: batchState.apptPdfIndex,
          apptPdfCount: batchState.apptPdfCount,
          apptSearchFailed: batchState.apptSearchFailed,
          // Flag to indicate large data is in storage
          _dataInStorage: true,
        },
      }).eq("id", job.id);

      const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
      const fnUrl = `${supabaseUrl}/functions/v1/chirofusion-scrape`;
      // Fire the next batch — don't await the response (it takes 100s+)
      // Just ensure the request leaves the network stack with a short delay
      fetch(fnUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": authHeader!,
        },
        body: JSON.stringify({
          dataTypes, mode, dateFrom, dateTo,
          _batchJobId: job.id,
          _batchState: {
            resumeIndex: batchState.resumeIndex,
            pdfCount: batchState.pdfCount,
            searchFailed: batchState.searchFailed,
            withFiles: batchState.withFiles,
            skippedDefaultCase: batchState.skippedDefaultCase,
            dataTypeIndex: batchState.dataTypeIndex,
            ledgerFetched: batchState.ledgerFetched,
            ledgerEmpty: batchState.ledgerEmpty,
            ledgerSearchFailed: batchState.ledgerSearchFailed,
            apptPdfResumeIndex: batchState.apptPdfResumeIndex,
            apptPdfCount: batchState.apptPdfCount,
            apptSearchFailed: batchState.apptSearchFailed,
          },
        }),
      }).then(r => console.log(`Self-invoke response: ${r.status}`))
        .catch(err => console.error("Self-invoke error:", err));
      // Wait briefly to ensure the request is dispatched before this function returns
      await new Promise(r => setTimeout(r, 3000));
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
    // Extract practiceId from session page (cached)
    let _practiceId = "";
    function setPracticeId(html: string) {
      const m = html.match(/SessionInfo\.practiceId\s*=\s*'(\d+)'/);
      if (m) _practiceId = m[1];
    }

    async function ajaxFetch(url: string, opts: RequestInit = {}): Promise<{ status: number; body: string; contentType: string }> {
      const res = await fetch(url.startsWith("http") ? url : `${BASE_URL}${url}`, {
        ...opts,
        headers: {
          ...browserHeaders,
          "Cookie": sessionCookies,
          "X-Requested-With": "XMLHttpRequest",
          "Accept": "*/*",
          "Origin": BASE_URL,
          "Referer": `${BASE_URL}/User/Scheduler`,
          "ClientPatientId": "0",
          ...(_practiceId ? { "practiceId": _practiceId } : {}),
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
    } catch (loginError) {
      logParts.push(`❌ LOGIN ERROR: ${(loginError as any).message}`);
      await serviceClient.from("scrape_jobs").update({ status: "failed", error_message: (loginError as any).message, log_output: logParts.join("\n") }).eq("id", job.id);
      return new Response(JSON.stringify({ error: (loginError as any).message }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ==================== DISCOVER MODE (COMPREHENSIVE) ====================
    if (mode === "discover") {
      const schedToken = { value: "" };

      // ===== 1. SCHEDULER PAGE: forms, hidden fields, tokens =====
      try {
        const { body: html } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
        logParts.push(`===== SCHEDULER PAGE: ${html.length} chars =====`);

        // Extract verification token and practiceId
        const tokenMatch = html.match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/);
        schedToken.value = tokenMatch ? tokenMatch[1] : "";
        setPracticeId(html);
        logParts.push(`Token: ${schedToken.value ? "YES" : "NONE"}, practiceId: ${_practiceId}`);

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
          } catch (e) {
            logParts.push(`JS fetch error ${jsUrl}: ${(e as any).message}`);
          }
        }
      } catch (err) {
        logParts.push(`SCHEDULER ERROR: ${(err as any).message}`);
      }

      // ===== 2. EXTRACT MULTISELECT OPTIONS & TEST ENDPOINTS =====
      logParts.push(`\n===== ENDPOINT TESTS =====`);

      // Extract PatientStatus multiselect values from the Scheduler page
      const schedHtmlForOpts = (await fetchWithCookies(`${BASE_URL}/User/Scheduler`)).body;
      const selectRegex2 = new RegExp(`<select[^>]*id="patientReportPatientStatusMultiSelect"[^>]*>([\\s\\S]*?)</select>`, "i");
      const selectMatch2 = schedHtmlForOpts.match(selectRegex2);
      const statusOpts: string[] = [];
      if (selectMatch2) {
        const optRegex2 = /<option[^>]*value="([^"]*)"[^>]*>([^<]*)<\/option>/gi;
        let om2;
        while ((om2 = optRegex2.exec(selectMatch2[1])) !== null) {
          statusOpts.push(om2[1]);
          logParts.push(`  PatientStatus option: value="${om2[1]}" text="${om2[2]}"`);
        }
      }
      const allStatusJoined = statusOpts.filter(v => v).join("^");
      logParts.push(`PatientStatus all values joined (^): "${allStatusJoined}"`);

      // 2a: GetPatientReports with ^-separated PatientStatus (matching real browser)
      const patientReportParams = [
        { ReportType: "1", BirthMonths: "", PatientStatus: allStatusJoined, PatientInsurance: "" },
        { ReportType: "1", BirthMonths: "", PatientStatus: "1^2^3", PatientInsurance: "" },
        { ReportType: "1", BirthMonths: "", PatientStatus: "1", PatientInsurance: "" },
      ];

      for (const params of patientReportParams) {
        try {
          const res = await ajaxFetch("/Scheduler/Scheduler/GetPatientReports", {
            method: "POST",
            headers: {
              "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            body: new URLSearchParams(params).toString(),
          });
          logParts.push(`GetPatientReports(Status=${params.PatientStatus || "empty"}): status=${res.status} len=${res.body.length} type=${res.contentType}`);
          if (res.body.length > 0 && res.body.length < 2000) logParts.push(`  Body: ${res.body}`);
          else if (res.body.length >= 2000) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
        } catch (e) {
          logParts.push(`GetPatientReports error: ${(e as any).message}`);
        }
      }

      // 2b: Try ExportPatientReports right after (report might be primed from above)
      try {
        const res = await ajaxFetch("/Scheduler/Scheduler/ExportPatientReports", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          },
          body: new URLSearchParams({
            ReportType: "1", BirthMonths: "", PatientStatus: "1^2^3", PatientInsurance: "",
          }).toString(),
        });
        logParts.push(`ExportPatientReports: status=${res.status} len=${res.body.length} type=${res.contentType}`);
        if (res.body.length > 0) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
      } catch (e) {
        logParts.push(`ExportPatientReports error: ${(e as any).message}`);
      }

      // 2c: Try ExportPatientReports with fetchWithCookies (non-AJAX, like browser form submit)
      try {
        const { response: rawRes, body: rawBody } = await fetchWithCookies(
          `${BASE_URL}/Scheduler/Scheduler/ExportPatientReports`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
              "X-Requested-With": "XMLHttpRequest",
              "Origin": BASE_URL,
              "ClientPatientId": "0",
              ...(_practiceId ? { "practiceId": _practiceId } : {}),
            },
            body: new URLSearchParams({
              ReportType: "1", BirthMonths: "", PatientStatus: "1^2^3", PatientInsurance: "",
            }).toString(),
          }
        );
        logParts.push(`ExportPatientReports(raw): status=${rawRes.status} len=${rawBody.length}`);
        logParts.push(`  Headers: ${JSON.stringify(Object.fromEntries(rawRes.headers.entries()))}`);
        logParts.push(`  Full body: ${rawBody.substring(0, 1000)}`);
      } catch (e) {
        logParts.push(`Raw export error: ${(e as any).message}`);
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
        } catch (e) {
          logParts.push(`${path}: error ${(e as any).message}`);
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
        } catch (e) {
          logParts.push(`GET ${path}: error ${(e as any).message}`);
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
              "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            body: new URLSearchParams(apptParams).toString(),
          });
          logParts.push(`${path}: status=${res.status} len=${res.body.length} type=${res.contentType}`);
          if (res.body.length > 0 && res.body.length < 1000) logParts.push(`  Body: ${res.body}`);
          else if (res.body.length >= 1000) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
        } catch (e) {
          logParts.push(`${path}: error ${(e as any).message}`);
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
              "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            body: new URLSearchParams(apptParams).toString(),
          });
          logParts.push(`${path}: status=${res.status} len=${res.body.length} type=${res.contentType}`);
          if (res.body.length > 0) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
        } catch (e) {
          logParts.push(`${path}: error ${(e as any).message}`);
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
      } catch (e) {
        logParts.push(`Billing page error: ${(e as any).message}`);
      }

      // 4b: Home page (SOAP notes search)
      try {
        const { body: homeHtml } = await fetchWithCookies(`${BASE_URL}/`);
        logParts.push(`Home page: ${homeHtml.length} chars`);
      } catch (e) {
        logParts.push(`Home page error: ${(e as any).message}`);
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
              } catch (e) {
                logParts.push(`${sp}: error ${(e as any).message}`);
              }
            }
          }
        }
      } catch (e) {
        logParts.push(`Patient JSON error: ${(e as any).message}`);
      }

      // ===== 5. WAIT 20s AND TRY EXPORT AGAIN (report might have primed) =====
      logParts.push(`\n===== WAITING 20s FOR REPORT GENERATION =====`);
      await new Promise(r => setTimeout(r, 20000));

      try {
        const res = await ajaxFetch("/Scheduler/Scheduler/ExportPatientReports", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          },
          body: new URLSearchParams({
            ReportType: "1", BirthMonths: "", PatientStatus: "", PatientInsurance: "",
          }).toString(),
        });
        logParts.push(`ExportPatientReports(after 20s): status=${res.status} len=${res.body.length} type=${res.contentType}`);
        if (res.body.length > 0) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
      } catch (e) {
        logParts.push(`Export after wait error: ${(e as any).message}`);
      }

      // Also try appointment export after the wait
      try {
        const res = await ajaxFetch("/Scheduler/Scheduler/ExportAppointmentReport", {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          },
          body: new URLSearchParams(apptParams).toString(),
        });
        logParts.push(`ExportAppointmentReport(after 20s): status=${res.status} len=${res.body.length} type=${res.contentType}`);
        if (res.body.length > 0) logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
      } catch (e) {
        logParts.push(`Appt export after wait error: ${(e as any).message}`);
      }

      await serviceClient.from("scrape_jobs").update({ status: "completed", progress: 100, log_output: logParts.join("\n") }).eq("id", job.id);
      return new Response(JSON.stringify({ success: true, jobId: job.id, mode: "discover" }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ==================== SCRAPE MODE ====================
    console.log("Starting scrape mode...");
    let progress = 0;
    const totalTypes = dataTypes.length;
    let hasAnyData = false;
    // Collectors for consolidated workbook — restore from Storage on resume
    const dbBatchState = _batchJobId && job.batch_state ? job.batch_state as any : {};
    const useStorage = dbBatchState._dataInStorage === true;
    const collectedSheets: { type: string; csv: string }[] = useStorage
      ? await loadLargeDataFromStorage("collectedSheets", [])
      : (dbBatchState.collectedSheets || []);
    const soapIndex: { PatientName: string; Documents: number; PDFLink: string; Status: string }[] = useStorage
      ? await loadLargeDataFromStorage("soapIndex", [])
      : (dbBatchState.soapIndex || []);
    const financialsLedgerRows: Record<string, string>[] = useStorage
      ? await loadLargeDataFromStorage("financialsLedgerRows", [])
      : (dbBatchState.financialsLedgerRows || []);
    const appointmentsIndex: { PatientName: string; Appointments: number; PDFLink: string; Status: string }[] = useStorage
      ? await loadLargeDataFromStorage("appointmentsIndex", [])
      : (dbBatchState.appointmentsIndex || []);

    // Shared: get full patient list with IDs for per-patient scraping
    // Caches result so it's only fetched once even if multiple data types need it
    // Get patient names from GetPatientReports (no IDs needed)
    let _cachedPatientNames: { firstName: string; lastName: string }[] | null = null;
    async function getAllPatientNames(): Promise<{ firstName: string; lastName: string }[]> {
      if (_cachedPatientNames) return _cachedPatientNames;

      // Load Scheduler page and run report
      const { body: schedHtml } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
      setPracticeId(schedHtml);

      const statusSelectMatch = schedHtml.match(/<select[^>]*id="patientReportPatientStatusMultiSelect"[^>]*>([\s\S]*?)<\/select>/i);
      const statusOpts: string[] = [];
      if (statusSelectMatch) {
        const optRegex = /<option[^>]*value="([^"]*)"[^>]*>/gi;
        let om;
        while ((om = optRegex.exec(statusSelectMatch[1])) !== null) {
          if (om[1]) statusOpts.push(om[1]);
        }
      }
      const allStatusValues = statusOpts.join("^") || "1^2^3";

      const reportRes = await ajaxFetch("/Scheduler/Scheduler/GetPatientReports", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
        body: new URLSearchParams({
          ReportType: "1",
          BirthMonths: "",
          PatientStatus: allStatusValues,
          PatientInsurance: "",
        }).toString(),
      });

      if (reportRes.status === 200 && reportRes.body.length > 100) {
        try {
          const reportData = JSON.parse(reportRes.body);
          const items = reportData.Data || reportData.data || (Array.isArray(reportData) ? reportData : null);
          if (items && Array.isArray(items)) {
            _cachedPatientNames = items.map((p: any) => ({
              firstName: p.FirstName || "",
              lastName: p.LastName || "",
            }));
            logParts.push(`✅ Patient names: ${_cachedPatientNames.length} from GetPatientReports`);
            return _cachedPatientNames;
          }
        } catch { /* not JSON */ }
      }

      _cachedPatientNames = [];
      logParts.push(`⚠️ Could not retrieve patient names`);
      return _cachedPatientNames;
    }

    // Search for a patient by name using the real GetSearchedPatient endpoint
    // Returns { id, dob } or null
    async function searchPatient(searchText: string, archiveFilter: number, debugLog: boolean): Promise<{ id: number; dob: string } | null> {
      const res = await ajaxFetch("/Patient/Patient/GetSearchedPatient", {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          "ClientPatientId": "0",
          ...(_practiceId ? { "practiceId": _practiceId } : {}),
        },
        body: new URLSearchParams({
          SelectedCriteria: "10",
          SelectedFilter: "2",
          searchText: searchText,
          archiveFilter: String(archiveFilter),
        }).toString(),
      });

      if (debugLog) {
        logParts.push(`GetSearchedPatient(archive=${archiveFilter}, text="${searchText}"): status=${res.status} len=${res.body.length}`);
        logParts.push(`  Preview: ${res.body.substring(0, 500)}`);
      }

      if (res.status === 200 && res.body.length > 10) {
        try {
          const data = JSON.parse(res.body);
          const arr = Array.isArray(data) ? data : (data.Data || data.data || data.Results || []);
          if (Array.isArray(arr) && arr.length > 0) {
            if (debugLog) {
              logParts.push(`  Results: ${arr.length} items, keys: ${Object.keys(arr[0]).join(", ")}`);
              logParts.push(`  Sample: ${JSON.stringify(arr[0]).substring(0, 400)}`);
            }
            const match = arr[0];
            const patientId = match.File_Number || match.PatientId || match.Id || match.PkPatientId ||
                             match.ClientPatientId || match.id || match.Value || match.value;
            const dob = match.Dob || match.dob || match.DateOfBirth || "";
            if (patientId) return { id: Number(patientId), dob: String(dob) };
          }
        } catch { /* not JSON */ }
      }
      return null;
    }

    // Search by name: first try regular search, then archive if not found
    async function findPatientInfo(firstName: string, lastName: string, debugLog: boolean): Promise<{ id: number; dob: string } | null> {
      const searchText = `${lastName}, ${firstName}`.trim();
      
      // Try regular search first
      let result = await searchPatient(searchText, 0, debugLog);
      if (result !== null) return result;

      // Not found — try archive search ("click here to load more")
      if (debugLog) logParts.push(`"${searchText}" not in regular search, trying archive...`);
      result = await searchPatient(searchText, 1, debugLog);
      return result;
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

            // ===== Step 1: Load Scheduler page, extract multiselect values =====
            logParts.push(`Step 1: Loading Scheduler page...`);
            const { body: schedHtml } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
            const schedToken = extractVerifToken(schedHtml);
            setPracticeId(schedHtml);
            logParts.push(`Scheduler: ${schedHtml.length} chars, token: ${schedToken ? "YES" : "NONE"}, practiceId: ${_practiceId}`);

            // Extract PatientStatus multiselect options from HTML (use ^ separator like the real browser)
            const patientStatusOpts = extractSelectOptions(schedHtml, "patientReportPatientStatusMultiSelect");
            const allStatusValues = patientStatusOpts.map(o => o.value).filter(v => v).join("^");
            logParts.push(`PatientStatus options: ${JSON.stringify(patientStatusOpts)}`);
            logParts.push(`PatientStatus all values (^-separated): "${allStatusValues}"`);

            // Extract PatientInsurance multiselect options
            const patientInsuranceOpts = extractSelectOptions(schedHtml, "patientReportPatientInsuranceMultiSelect");
            const allInsuranceValues = patientInsuranceOpts.map(o => o.value).filter(v => v).join("^");
            logParts.push(`PatientInsurance options: ${patientInsuranceOpts.length} values`);

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
                } catch (e) {
                  logParts.push(`JS fetch error: ${(e as any).message}`);
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
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                  },
                  body: new URLSearchParams({}).toString(),
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
              } catch (e) {
                logParts.push(`  Form POST error: ${(e as any).message}`);
              }
            }

            // ===== Step 2b: Trigger GetPatientReports matching exact browser request =====
            if (!found) {
              logParts.push(`Step 2b: Trigger GetPatientReports (browser-matched format)...`);
              // Use ^-separated values matching real browser cURL
              const statusCandidates = [
                allStatusValues,      // All multiselect values ^-separated
                "1^2^3",              // Active statuses
                "1^2^3^4^5",          // More statuses
                "1",                  // Just Active
              ].filter((v, i, a) => a.indexOf(v) === i); // dedupe

              for (const statusVal of statusCandidates) {
                if (found) break;
                const reportData = {
                  ReportType: "1",
                  BirthMonths: "",
                  PatientStatus: statusVal,
                  PatientInsurance: allInsuranceValues,
                };
                logParts.push(`  Trying PatientStatus="${statusVal}" Insurance="${allInsuranceValues.substring(0, 80)}..."...`);

                try {
                  const triggerRes = await ajaxFetch("/Scheduler/Scheduler/GetPatientReports", {
                    method: "POST",
                    headers: {
                      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    },
                    body: new URLSearchParams(reportData).toString(),
                  });
                  logParts.push(`    Trigger: status=${triggerRes.status} len=${triggerRes.body.length} type=${triggerRes.contentType}`);

                  if (triggerRes.body.length > 0 && triggerRes.body.length < 2000) {
                    logParts.push(`    Trigger body: ${triggerRes.body}`);
                  } else if (triggerRes.body.length >= 2000) {
                    logParts.push(`    Trigger body preview: ${triggerRes.body.substring(0, 1000)}`);
                    try {
                      const triggerData = JSON.parse(triggerRes.body);
                      const items = triggerData.Data || triggerData.data || (Array.isArray(triggerData) ? triggerData : null);
                      if (items && Array.isArray(items) && items.length > 0) {
                        csvContent = jsonToCsv(items);
                        rowCount = items.length;
                        logParts.push(`✅ Demographics from trigger response: ${rowCount} patients`);
                        found = true;
                        break;
                      }
                    } catch { /* not JSON */ }
                  }

                  // If trigger returned data (len > 0), try export after waiting
                  if (!found && triggerRes.body.length > 0) {
                    logParts.push(`    Data returned, polling export...`);
                    await new Promise(r => setTimeout(r, 5000));
                    const expRes = await ajaxFetch("/Scheduler/Scheduler/ExportPatientReports", {
                      method: "POST",
                      headers: {
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                      },
                      body: new URLSearchParams(reportData).toString(),
                    });
                    logParts.push(`    Export: status=${expRes.status} len=${expRes.body.length}`);
                    if (expRes.status === 200 && expRes.body.length > 50 && !expRes.body.includes("<!DOCTYPE")) {
                      csvContent = expRes.body;
                      rowCount = csvContent.split("\n").filter((l: string) => l.trim()).length - 1;
                      logParts.push(`✅ Demographics export: ${rowCount} rows`);
                      found = true;
                      break;
                    }
                  }
                } catch (e) {
                  logParts.push(`    Error: ${(e as any).message}`);
                }
              }
            }

            // ===== Step 2c: Try exportPatientListCsv form AFTER priming the report =====
            if (!found && csvFormAction) {
              logParts.push(`Step 2c: Retrying form POST after report primed...`);
              try {
                const formRes = await ajaxFetch(csvFormAction, {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                  },
                  body: new URLSearchParams({}).toString(),
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
              } catch (e) {
                logParts.push(`  Form POST error: ${(e as any).message}`);
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
                      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
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
                } catch (e) {
                  logParts.push(`Kendo ${kp}: error ${(e as any).message}`);
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
              } catch (err) {
                logParts.push(`JSON fallback error: ${(err as any).message}`);
              }
            }
            break;
          }

          // ==================== APPOINTMENTS ====================
          case "appointments": {
            // Based on browser cURL: POST to /Scheduler/Scheduler/GetAppointmentReport
            // ReportType=1204 (Appointment Hx), IsAllPatient=true, with __RequestVerificationToken
            const fromDate = dateFrom || "08/01/2021";
            const toDate = dateTo || new Date().toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" });

            logParts.push(`Date range: ${fromDate} to ${toDate}`);

            // Step 1: Load Scheduler page to get verification token and practiceId
            const { body: schedHtmlAppt } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
            const apptToken = extractVerifToken(schedHtmlAppt);
            setPracticeId(schedHtmlAppt);
            logParts.push(`Scheduler loaded. Token: ${apptToken ? "YES" : "NONE"}, practiceId: ${_practiceId}`);

            // Step 2: Call GetAppointmentReport with exact browser payload
            const apptBody = new URLSearchParams({
              __RequestVerificationToken: apptToken,
              ReportType_input: "Appointment Hx",
              ReportType: "1204",
              DateFrom: fromDate,
              DateTo: toDate,
              IsAllPatient: "true",
              ReportAppointmentTypeId_input: "",
              ReportAppointmentTypeId: "",
              ReportProviderId_input: "",
              ReportProviderId: "",
            }).toString();

            logParts.push(`Step 2: GetAppointmentReport (ReportType=1204, IsAllPatient=true)...`);
            let apptFound = false;
            let apptItems: Record<string, unknown>[] = [];

            try {
              const triggerRes = await ajaxFetch("/Scheduler/Scheduler/GetAppointmentReport", {
                method: "POST",
                headers: {
                  "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                },
                body: apptBody,
              });
              logParts.push(`Trigger: status=${triggerRes.status} len=${triggerRes.body.length} type=${triggerRes.contentType}`);

              if (triggerRes.status === 200 && triggerRes.body.length > 100) {
                if (triggerRes.body.length < 2000) {
                  logParts.push(`  Body: ${triggerRes.body}`);
                } else {
                  logParts.push(`  Preview: ${triggerRes.body.substring(0, 500)}`);
                }

                try {
                  const triggerData = JSON.parse(triggerRes.body);
                  const items = triggerData.Data || triggerData.data || (Array.isArray(triggerData) ? triggerData : null);
                  if (items && Array.isArray(items) && items.length > 0) {
                    apptItems = items;
                    logParts.push(`✅ Appointments from direct response: ${apptItems.length} rows`);
                    apptFound = true;
                  }
                } catch { /* not JSON, try export */ }
              }
            } catch (e) {
              logParts.push(`Trigger error: ${(e as any).message}`);
            }

            // Step 3: Poll ExportAppointmentReport if direct response didn't have data
            if (!apptFound) {
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
                      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    },
                    body: apptBody,
                  });
                  logParts.push(`  status=${expRes.status} len=${expRes.body.length} type=${expRes.contentType}`);

                  if (expRes.status === 200 && expRes.body.length > 50 && !expRes.body.includes("<!DOCTYPE") && !expRes.body.includes("<html")) {
                    // Parse the CSV export into JSON items
                    const exportItems = csvToJson(expRes.body);
                    if (exportItems.length > 0) {
                      apptItems = exportItems;
                      logParts.push(`✅ Appointments export: ${apptItems.length} rows (attempt ${attempt})`);
                      apptFound = true;
                      break;
                    }
                  }

                  if (expRes.body.length > 0 && expRes.body.length < 1000) {
                    logParts.push(`  Body: ${expRes.body}`);
                  }
                } catch (e) {
                  logParts.push(`  Attempt ${attempt} error: ${(e as any).message}`);
                }
              }
            }

            if (!apptFound || apptItems.length === 0) {
              logParts.push(`⚠️ Appointments: No data retrieved after all attempts`);
              break;
            }

            // Step 4: Clean .NET dates in all rows
            for (const row of apptItems) {
              for (const key of Object.keys(row)) {
                if (typeof row[key] === "string" && (row[key] as string).includes("/Date(")) {
                  row[key] = parseNetDate(row[key] as string);
                }
              }
            }

            // Step 5: Detect field names for sorting (handle various naming conventions)
            const sampleKeys = Object.keys(apptItems[0]);
            logParts.push(`Appointment fields: ${sampleKeys.join(", ")}`);

            // Find patient name field(s)
            const patientNameKey = sampleKeys.find(k => /^(PatientName|Patient_Name|Patient|FullName|Name)$/i.test(k));
            const lastNameKey = sampleKeys.find(k => /^(LastName|Last_Name|PatientLastName)$/i.test(k));
            const firstNameKey = sampleKeys.find(k => /^(FirstName|First_Name|PatientFirstName)$/i.test(k));
            // Date field
            const dateKey = sampleKeys.find(k => /^(AppointmentDate|Date|ApptDate|VisitDate|Appointment_Date|ServiceDate)$/i.test(k));
            // Appt type field
            const typeKey = sampleKeys.find(k => /^(AppointmentType|Appt_Type|ApptType|Type|AppointmentTypeName|VisitType)$/i.test(k));
            // Check-in field
            const checkinKey = sampleKeys.find(k => /^(CheckedIn|Checked_In|IsCheckedIn|CheckIn|Status|AppointmentStatus|Appt_Status)$/i.test(k));

            logParts.push(`Field mapping: name=${patientNameKey || `${lastNameKey}+${firstNameKey}`} date=${dateKey} type=${typeKey} checkin=${checkinKey}`);

            // Helper to get patient display name from a row
            function getPatientName(row: Record<string, unknown>): string {
              if (patientNameKey && row[patientNameKey]) return String(row[patientNameKey]);
              const ln = lastNameKey ? String(row[lastNameKey] || "") : "";
              const fn = firstNameKey ? String(row[firstNameKey] || "") : "";
              if (ln || fn) return `${ln}, ${fn}`.replace(/^, |, $/g, "");
              return "Unknown";
            }

            // Step 6: Sort by patient name → date → appointment type
            apptItems.sort((a, b) => {
              const nameA = getPatientName(a).toLowerCase();
              const nameB = getPatientName(b).toLowerCase();
              if (nameA !== nameB) return nameA < nameB ? -1 : 1;

              // Sort by date chronologically
              if (dateKey) {
                const dA = new Date(String(a[dateKey] || "")).getTime() || 0;
                const dB = new Date(String(b[dateKey] || "")).getTime() || 0;
                if (dA !== dB) return dA - dB;
              }

              // Sort by appointment type
              if (typeKey) {
                const tA = String(a[typeKey] || "").toLowerCase();
                const tB = String(b[typeKey] || "").toLowerCase();
                if (tA !== tB) return tA < tB ? -1 : 1;
              }

              return 0;
            });

            logParts.push(`Sorted ${apptItems.length} appointments by patient name → date → type`);

            // Step 7: Save sorted appointments as the main sheet CSV
            csvContent = jsonToCsv(apptItems);
            rowCount = apptItems.length;

            // Step 8: Group by patient and generate per-patient PDF summaries
            const patientGroups = new Map<string, Record<string, unknown>[]>();
            for (const item of apptItems) {
              const name = getPatientName(item);
              if (!patientGroups.has(name)) patientGroups.set(name, []);
              patientGroups.get(name)!.push(item);
            }

            logParts.push(`Generating PDF summaries for ${patientGroups.size} patients...`);

            // Check if we're resuming PDF generation from a previous batch
            const apptBs = _batchState || {};
            let apptPdfIndex = apptBs.dataTypeIndex === dataTypes.indexOf(dataType) ? (apptBs.apptPdfResumeIndex || 0) : 0;
            let apptPdfCount = apptBs.apptPdfCount || 0;
            const patientNames = [...patientGroups.keys()];

            if (apptPdfIndex > 0) {
              logParts.push(`🔄 Resuming PDF generation from patient ${apptPdfIndex}/${patientNames.length}`);
            }

            for (let pi = apptPdfIndex; pi < patientNames.length; pi++) {
              if (isTimingOut()) {
                // Save state and self-invoke
                await selfInvoke({
                  resumeIndex: 0,
                  dataTypeIndex: dataTypes.indexOf(dataType),
                  apptPdfResumeIndex: pi,
                  apptPdfCount,
                  appointmentsIndex,
                });
                return new Response(JSON.stringify({ success: true, jobId: job.id, batching: true }), {
                  headers: { ...corsHeaders, "Content-Type": "application/json" },
                });
              }

              const pName = patientNames[pi];
              const pAppts = patientGroups.get(pName)!;

              try {
                // Build PDF lines for this patient
                const pdfLines: string[] = [];
                pdfLines.push(`Total Appointments: ${pAppts.length}`);
                pdfLines.push(`Date Range: ${fromDate} — ${toDate}`);
                pdfLines.push("");
                pdfLines.push("─".repeat(70));

                for (const appt of pAppts) {
                  const apptDate = dateKey ? String(appt[dateKey] || "") : "";
                  const apptType = typeKey ? String(appt[typeKey] || "") : "";
                  const checkedIn = checkinKey ? String(appt[checkinKey] || "") : "";
                  
                  let line = apptDate;
                  if (apptType) line += `  |  ${apptType}`;
                  if (checkedIn) line += `  |  Check-in: ${checkedIn}`;
                  
                  // Add any other visible fields
                  const providerKey = sampleKeys.find(k => /^(Provider|Doctor|Physician|ProviderName)$/i.test(k));
                  if (providerKey && appt[providerKey]) line += `  |  ${appt[providerKey]}`;
                  
                  const locationKey = sampleKeys.find(k => /^(Location|Office|LocationName)$/i.test(k));
                  if (locationKey && appt[locationKey]) line += `  |  ${appt[locationKey]}`;

                  pdfLines.push(line);
                }

                // Generate PDF
                const safeName = pName.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase();
                const pdfBytes = generateTextPdf(`Appointment Summary — ${pName}`, pdfLines);
                const filePath = `${userId}/appointment_summaries/${safeName}_${Date.now()}.pdf`;

                const pdfBlob = new Blob([pdfBytes], { type: "application/pdf" });
                const { error: uploadError } = await serviceClient.storage
                  .from("scraped-data")
                  .upload(filePath, pdfBlob, { contentType: "application/pdf" });

                if (uploadError) {
                  logParts.push(`❌ PDF upload error ${pName}: ${uploadError.message}`);
                  appointmentsIndex.push({ PatientName: pName, Appointments: pAppts.length, PDFLink: "", Status: "Upload error" });
                } else {
                  const { data: signedUrlData } = await serviceClient.storage
                    .from("scraped-data")
                    .createSignedUrl(filePath, 2592000); // 30 days
                  appointmentsIndex.push({
                    PatientName: pName,
                    Appointments: pAppts.length,
                    PDFLink: signedUrlData?.signedUrl || filePath,
                    Status: "✅ Generated",
                  });
                  apptPdfCount++;
                }
              } catch (err) {
                logParts.push(`❌ PDF ${pName}: ${(err as any).message}`);
                appointmentsIndex.push({ PatientName: pName, Appointments: pAppts.length, PDFLink: "", Status: "Error" });
              }

              if ((pi + 1) % 100 === 0) {
                logParts.push(`Appt PDF progress: ${pi + 1}/${patientNames.length} (${apptPdfCount} PDFs)`);
                await serviceClient.from("scrape_jobs").update({
                  progress: Math.min(progress + Math.round(((pi + 1) / patientNames.length) * (100 / totalTypes)), 99),
                  log_output: logParts.join("\n"),
                }).eq("id", job.id);
              }

              await new Promise(r => setTimeout(r, 50));
            }

            logParts.push(`✅ Appointments complete: ${apptItems.length} rows sorted, ${apptPdfCount} PDF summaries from ${patientGroups.size} patients`);
            break;
          }

          // ==================== SOAP NOTES / MEDICAL FILES ====================
          case "soap_notes": {
            const patients = await getAllPatientNames();

            if (patients.length === 0) {
              logParts.push(`⚠️ Medical Files: No patients found`);
              break;
            }

            // Load Scheduler page to ensure practiceId is set
            if (!_practiceId) {
              const { body: schedHtml } = await fetchWithCookies(`${BASE_URL}/User/Scheduler`);
              setPracticeId(schedHtml);
            }

            // Restore batch state if continuing
            const bs = _batchState || {};
            let processedCount = bs.resumeIndex || 0;
            let pdfCount = bs.pdfCount || 0;
            let searchFailed = bs.searchFailed || 0;
            let withFiles = bs.withFiles || 0;
            let skippedDefaultCase = bs.skippedDefaultCase || 0;

            if (processedCount > 0) {
              logParts.push(`🔄 Resuming from patient ${processedCount}/${patients.length}`);
            } else {
              logParts.push(`Processing ${patients.length} patients for medical file PDFs (using SetVisitIdInSession for context)`);
            }

            for (let i = processedCount; i < patients.length; i++) {
              const patient = patients[i];
              if (isTimingOut()) {
                // Self-invoke to continue in a new batch
                await selfInvoke({
                  resumeIndex: i,
                  pdfCount, searchFailed, withFiles, skippedDefaultCase,
                  dataTypeIndex: dataTypes.indexOf(dataType),
                });
                return new Response(JSON.stringify({ success: true, jobId: job.id, batching: true }), {
                  headers: { ...corsHeaders, "Content-Type": "application/json" },
                });
              }

              try {
                // 1. Search for patient to get File_Number and check CaseName
                const searchText = `${patient.lastName}, ${patient.firstName}`.trim();
                const searchRes = await ajaxFetch("/Patient/Patient/GetSearchedPatient", {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "ClientPatientId": "0",
                    ...(_practiceId ? { "practiceId": _practiceId } : {}),
                  },
                  body: new URLSearchParams({
                    SelectedCriteria: "10",
                    SelectedFilter: "2",
                    searchText,
                    archiveFilter: "1", // search archive directly (covers all patients)
                  }).toString(),
                });

                if (searchRes.status !== 200 || searchRes.body.length < 10) {
                  searchFailed++;
                  soapIndex.push({ PatientName: `${patient.lastName}, ${patient.firstName}`, Documents: 0, PDFLink: "", Status: "Search failed" });
                  processedCount++;
                  continue;
                }

                let searchData: any[];
                try {
                  const parsed = JSON.parse(searchRes.body);
                  searchData = Array.isArray(parsed) ? parsed : (parsed.Data || []);
                } catch {
                  searchFailed++;
                  soapIndex.push({ PatientName: `${patient.lastName}, ${patient.firstName}`, Documents: 0, PDFLink: "", Status: "Search failed" });
                  processedCount++;
                  continue;
                }

                if (searchData.length === 0) {
                  searchFailed++;
                  soapIndex.push({ PatientName: `${patient.lastName}, ${patient.firstName}`, Documents: 0, PDFLink: "", Status: "Not found" });
                  processedCount++;
                  continue;
                }

                // Check if ALL case entries are "Default Case" — if so, skip (no medical files possible)
                const allDefault = searchData.every((entry: any) => {
                  const caseName = (entry.CaseName || "").toLowerCase();
                  return caseName.includes("default case");
                });

                if (allDefault) {
                  skippedDefaultCase++;
                  soapIndex.push({ PatientName: `${patient.lastName}, ${patient.firstName}`, Documents: 0, PDFLink: "", Status: "Default case only" });
                  processedCount++;
                  continue;
                }

                // Use first non-default-case entry to get patientId and caseId
                const match = searchData.find((entry: any) => !(entry.CaseName || "").toLowerCase().includes("default case")) || searchData[0];
                const patientId = match.File_Number || match.PatientId || match.Id;
                const caseId = match.CaseId || match.Case_Id || match.caseId || "";
                if (!patientId) {
                  searchFailed++;
                  processedCount++;
                  continue;
                }

                const fileName = `${patient.lastName}${patient.firstName}`.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
                const isDebugPatient = i < 5;

                if (isDebugPatient) {
                  logParts.push(`🔍 DEBUG ${patient.firstName} ${patient.lastName} (id=${patientId}, caseId=${caseId}):`);
                  logParts.push(`  Match keys: ${Object.keys(match).join(", ")}`);
                }

                // 2. Set patient context via SetVisitIdInSession (critical for GetFilesFromBlob)
                const visitRes = await ajaxFetch("/Patient/Patient/SetVisitIdInSession", {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer": `${BASE_URL}/Patient`,
                    "ClientPatientId": String(patientId),
                    ...(_practiceId ? { "practiceId": _practiceId } : {}),
                  },
                  body: new URLSearchParams({
                    PatientId: String(patientId),
                    CaseId: String(caseId),
                  }).toString(),
                });

                if (isDebugPatient) {
                  logParts.push(`  SetVisitIdInSession: status=${visitRes.status} body=${visitRes.body.substring(0, 200)}`);
                }

                // Also call GetFileCategory to further prime context
                await ajaxFetch("/Patient/Patient/GetFileCategory", {
                  method: "GET",
                  headers: {
                    "Referer": `${BASE_URL}/Patient`,
                    "ClientPatientId": String(patientId),
                    ...(_practiceId ? { "practiceId": _practiceId } : {}),
                  },
                });

                // 3. Get file list
                const filesRes = await ajaxFetch("/Patient/Patient/GetFilesFromBlob", {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer": `${BASE_URL}/Patient`,
                    "ClientPatientId": String(patientId),
                    ...(_practiceId ? { "practiceId": _practiceId } : {}),
                  },
                  body: new URLSearchParams({
                    sort: "", group: "", filter: "",
                    patientId: String(patientId),
                    practiceId: _practiceId,
                  }).toString(),
                });

                if (isDebugPatient) {
                  logParts.push(`  GetFilesFromBlob: status=${filesRes.status} body=${filesRes.body.substring(0, 500)}`);
                }

                if (filesRes.status !== 200 || filesRes.body.length < 10) {
                  soapIndex.push({ PatientName: `${patient.lastName}, ${patient.firstName}`, Documents: 0, PDFLink: "", Status: "No files" });
                  processedCount++;
                  continue;
                }

                let filesData: any;
                try { filesData = JSON.parse(filesRes.body); } catch { soapIndex.push({ PatientName: `${patient.lastName}, ${patient.firstName}`, Documents: 0, PDFLink: "", Status: "Parse error" }); processedCount++; continue; }

                const files = filesData.Data || (Array.isArray(filesData) ? filesData : []);
                if (isDebugPatient) {
                  logParts.push(`  Parsed: ${files.length} files, keys: ${files.length > 0 ? Object.keys(files[0]).join(", ") : "N/A"}`);
                }
                if (files.length === 0) {
                  soapIndex.push({ PatientName: `${patient.lastName}, ${patient.firstName}`, Documents: 0, PDFLink: "", Status: "No files" });
                  processedCount++;
                  continue;
                }

                // 3. Extract blob names
                const blobNames: string[] = [];
                for (const file of files) {
                  const blobName = file.BlobName || file.blobName || file.FileName || file.fileName || file.Name || file.name || file.FileKey || file.fileKey;
                  if (blobName && typeof blobName === "string") blobNames.push(blobName);
                }

                if (blobNames.length === 0) {
                  if (isDebugPatient) logParts.push(`  ⚠️ ${files.length} files but no blob name field. Keys: ${Object.keys(files[0]).join(", ")}. Sample: ${JSON.stringify(files[0]).substring(0, 500)}`);
                  soapIndex.push({ PatientName: `${patient.lastName}, ${patient.firstName}`, Documents: files.length, PDFLink: "", Status: "No exportable files" });
                  processedCount++;
                  continue;
                }

                logParts.push(`${patient.firstName} ${patient.lastName}: ${blobNames.length} documents → ${fileName}.pdf`);
                withFiles++;

                // 4. Export as PDF
                const pdfRes = await fetch(`${BASE_URL}/User/Navigation/ExportToPdf`, {
                  method: "POST",
                  headers: {
                    ...browserHeaders,
                    "Cookie": sessionCookies,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": BASE_URL,
                    "Referer": `${BASE_URL}/Patient`,
                    ...(_practiceId ? { "practiceId": _practiceId } : {}),
                  },
                  redirect: "manual",
                  body: new URLSearchParams({
                    BlobName: blobNames.join("|"),
                    FileName: fileName,
                  }).toString(),
                });

                sessionCookies = mergeCookies(sessionCookies, pdfRes);

                if (pdfRes.status === 200) {
                  const pdfBuffer = await pdfRes.arrayBuffer();
                  if (pdfBuffer.byteLength > 100) {
                    const filePath = `${userId}/medical_files/${fileName}_${Date.now()}.pdf`;
                    const blob = new Blob([pdfBuffer], { type: "application/pdf" });
                    const { error: uploadError } = await serviceClient.storage
                      .from("scraped-data")
                      .upload(filePath, blob, { contentType: "application/pdf" });
                    if (uploadError) {
                      logParts.push(`❌ Upload error ${patient.firstName}: ${uploadError.message}`);
                    } else {
                      // Generate signed URL (30 days) for the workbook link
                      const { data: signedUrlData } = await serviceClient.storage
                        .from("scraped-data")
                        .createSignedUrl(filePath, 2592000);
                      soapIndex.push({
                        PatientName: `${patient.lastName}, ${patient.firstName}`,
                        Documents: blobNames.length,
                        PDFLink: signedUrlData?.signedUrl || filePath,
                        Status: "✅ Downloaded",
                      });
                      pdfCount++;
                    }
                  }
                } else {
                  if (processedCount < 3) logParts.push(`PDF export ${patient.firstName}: status=${pdfRes.status}`);
                }
              } catch (err) {
                logParts.push(`❌ ${patient.firstName} ${patient.lastName}: ${(err as any).message}`);
              }

              processedCount = i + 1;
              if (processedCount % 50 === 0) {
                logParts.push(`Progress: ${processedCount}/${patients.length} (${skippedDefaultCase} default-case skipped, ${withFiles} with files, ${pdfCount} PDFs)`);
                const newProgress = Math.round((processedCount / patients.length) * (100 / totalTypes));
                await serviceClient.from("scrape_jobs").update({ 
                  progress: Math.min(progress + newProgress, 99), 
                  log_output: logParts.join("\n") 
                }).eq("id", job.id);
              }

              await new Promise(r => setTimeout(r, 150));
            }

            logParts.push(`✅ Medical Files complete: ${pdfCount} PDFs from ${processedCount} patients (${withFiles} with files, ${skippedDefaultCase} default-case skipped, ${searchFailed} search failures)`);
            hasAnyData = pdfCount > 0;
            break;
          }

          // ==================== FINANCIALS (Per-Patient Account Ledger) ====================
          case "financials": {
            logParts.push(`Fetching per-patient account ledgers via ShowLedger...`);
            console.log("Financials: fetching patient list...");

            // Get all patient names from demographics report
            let patients: { firstName: string; lastName: string }[];
            try {
              patients = await getAllPatientNames();
              console.log(`Financials: got ${patients.length} patients`);
              logParts.push(`Got ${patients.length} patients for ledger processing`);
            } catch (patientErr) {
              logParts.push(`❌ Failed to get patient list: ${(patientErr as any).message}`);
              console.error("Financials: getAllPatientNames failed:", patientErr);
              break;
            }
            if (patients.length === 0) {
              logParts.push(`⚠️ No patients found — cannot fetch ledgers`);
              break;
            }

            // Batch resume support
            const bs = _batchState || {};
            let processedCount = bs.dataTypeIndex === dataTypes.indexOf(dataType) ? (bs.resumeIndex || 0) : 0;
            let ledgerFetched = bs.ledgerFetched || 0;
            let ledgerEmpty = bs.ledgerEmpty || 0;
            let ledgerSearchFailed = bs.ledgerSearchFailed || 0;

            // Restore previously collected rows from batch_state
            const allLedgerRows: Record<string, string>[] = [...financialsLedgerRows];

            if (processedCount > 0) {
              logParts.push(`🔄 Resuming ledgers from patient ${processedCount}/${patients.length} (${allLedgerRows.length} rows collected)`);
            } else {
              logParts.push(`Processing ${patients.length} patients for account ledgers`);
            }

            for (let i = processedCount; i < patients.length; i++) {
              const patient = patients[i];
              if (isTimingOut()) {
                // Save collected ledger rows to batch_state and self-invoke
                await selfInvoke({
                  resumeIndex: i,
                  ledgerFetched, ledgerEmpty, ledgerSearchFailed,
                  financialsLedgerRows: allLedgerRows,
                  dataTypeIndex: dataTypes.indexOf(dataType),
                });
                return new Response(JSON.stringify({ success: true, jobId: job.id, batching: true }), {
                  headers: { ...corsHeaders, "Content-Type": "application/json" },
                });
              }

              try {
                const patientName = `${patient.lastName}, ${patient.firstName}`;
                const isDebug = i < 3;

                // 1. Search for patient to get patientId
                const info = await findPatientInfo(patient.firstName, patient.lastName, isDebug);
                if (!info) {
                  ledgerSearchFailed++;
                  if (isDebug) logParts.push(`⚠️ "${patientName}" — search failed`);
                  processedCount = i + 1;
                  continue;
                }

                // 2. Set patient context via SetVisitIdInSession
                await ajaxFetch("/Patient/Patient/SetVisitIdInSession", {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer": `${BASE_URL}/Patient`,
                    "ClientPatientId": String(info.id),
                    ...(_practiceId ? { "practiceId": _practiceId } : {}),
                  },
                  body: new URLSearchParams({
                    PatientId: String(info.id),
                    CaseId: "0",
                  }).toString(),
                });

                // 3. Set billing context to Patient Accounting
                await ajaxFetch("/Billing/Billing/SetBillingDefaultPageInSession", {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer": `${BASE_URL}/Billing/`,
                    "ClientPatientId": String(info.id),
                    ...(_practiceId ? { "practiceId": _practiceId } : {}),
                  },
                  body: new URLSearchParams({
                    billingDefaultPage: "PatientAccounting",
                  }).toString(),
                });

                // 4. Fetch the account ledger HTML
                const ledgerRes = await ajaxFetch("/Billing/PatientAccounting/ShowLedger", {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer": `${BASE_URL}/Billing/`,
                    "ClientPatientId": String(info.id),
                    ...(_practiceId ? { "practiceId": _practiceId } : {}),
                  },
                  body: new URLSearchParams({
                    IsLedger: "1",
                    ShowUac: "true",
                  }).toString(),
                });

                if (isDebug) {
                  logParts.push(`🔍 Ledger ${patientName} (id=${info.id}): status=${ledgerRes.status} len=${ledgerRes.body.length}`);
                  logParts.push(`  Preview: ${ledgerRes.body.substring(0, 500)}`);
                }

                if (ledgerRes.status !== 200 || ledgerRes.body.length < 50) {
                  ledgerEmpty++;
                  processedCount = i + 1;
                  continue;
                }

                // 5. Parse the HTML table for transaction rows
                const ledgerRows = parseLedgerHtml(ledgerRes.body);

                if (ledgerRows.length === 0) {
                  ledgerEmpty++;
                  // Add a status row so every patient appears in the output
                  allLedgerRows.push({
                    PatientName: patientName,
                    Date: "",
                    Description: "",
                    CPTCode: "",
                    Charges: "",
                    Payments: "",
                    Adjustments: "",
                    Balance: "",
                    Status: "No transactions",
                  });
                  if (isDebug) logParts.push(`  No ledger rows parsed for ${patientName}`);
                  processedCount = i + 1;
                  continue;
                }

                // Prepend patient name to each row
                for (const row of ledgerRows) {
                  row["PatientName"] = patientName;
                }

                allLedgerRows.push(...ledgerRows);
                ledgerFetched++;
                if (isDebug) logParts.push(`  ✅ ${ledgerRows.length} ledger rows for ${patientName}`);

              } catch (err) {
                logParts.push(`❌ Ledger ${patient.firstName} ${patient.lastName}: ${(err as any).message}`);
              }

              processedCount = i + 1;
              if (processedCount % 50 === 0) {
                logParts.push(`Ledger progress: ${processedCount}/${patients.length} (${ledgerFetched} with data, ${ledgerEmpty} empty, ${ledgerSearchFailed} failed, ${allLedgerRows.length} total rows)`);
                const newProgress = Math.round((processedCount / patients.length) * (100 / totalTypes));
                await serviceClient.from("scrape_jobs").update({
                  progress: Math.min(progress + newProgress, 99),
                  log_output: logParts.join("\n"),
                }).eq("id", job.id);
              }

              await new Promise(r => setTimeout(r, 150));
            }

            logParts.push(`✅ Patient Ledgers complete: ${allLedgerRows.length} rows from ${processedCount} patients (${ledgerFetched} with data, ${ledgerEmpty} empty, ${ledgerSearchFailed} search failures)`);

            if (allLedgerRows.length > 0) {
              // Reorder columns so PatientName is first
              const reordered = allLedgerRows.map(row => {
                const { PatientName, ...rest } = row;
                return { PatientName, ...rest };
              });
              csvContent = jsonToCsv(reordered);
              rowCount = reordered.length;
            }
            break;
          }
        }

        // Collect CSV for consolidated workbook (instead of uploading individually)
        if (csvContent) {
          collectedSheets.push({ type: dataType, csv: csvContent });
          logParts.push(`📊 Collected ${dataType}: ${rowCount} rows for workbook`);
        }

        progress += Math.round(100 / totalTypes);
        await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress, 99) }).eq("id", job.id);

      } catch (scrapeError) {
        logParts.push(`❌ Error scraping ${dataType}: ${(scrapeError as any).message}`);
        console.error(`Error scraping ${dataType}:`, scrapeError);
      }
    }

    // ==================== BUILD CONSOLIDATED XLSX WORKBOOK ====================
    const wb = XLSX.utils.book_new();
    const typeLabels: Record<string, string> = {
      demographics: "Demographics",
      appointments: "Appointments",
      financials: "Patient Ledgers",
    };

    for (const { type, csv } of collectedSheets) {
      const rows = csvToJson(csv);
      if (rows.length > 0) {
        const ws = XLSX.utils.json_to_sheet(rows);
        XLSX.utils.book_append_sheet(wb, ws, typeLabels[type] || type);
      }
    }

    // Add SOAP Notes Index sheet with PDF download links (clickable hyperlinks)
    if (soapIndex.length > 0) {
      // Build sheet data without the raw URL — use "📎 Open PDF" as display text
      const sheetData = soapIndex.map(row => ({
        PatientName: row.PatientName,
        Documents: row.Documents,
        Status: row.Status,
        PDFLink: row.PDFLink || "",
      }));
      const ws = XLSX.utils.json_to_sheet(sheetData);
      
      // Add clickable hyperlinks to PDFLink column (column D, index 3)
      for (let r = 0; r < soapIndex.length; r++) {
        const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 3 }); // +1 for header row
        if (soapIndex[r].PDFLink) {
          ws[cellRef] = {
            t: "s",
            v: "📎 Open PDF",
            l: { Target: soapIndex[r].PDFLink },
          };
        }
      }
      
      // Set column widths
      ws["!cols"] = [
        { wch: 30 }, // PatientName
        { wch: 12 }, // Documents
        { wch: 20 }, // Status
        { wch: 15 }, // PDFLink
      ];
      
      XLSX.utils.book_append_sheet(wb, ws, "SOAP Notes Index");
    }

    // Add Appointments Index sheet with PDF summary links
    if (appointmentsIndex.length > 0) {
      const apptSheetData = appointmentsIndex.map(row => ({
        PatientName: row.PatientName,
        Appointments: row.Appointments,
        Status: row.Status,
        PDFLink: row.PDFLink || "",
      }));
      const apptWs = XLSX.utils.json_to_sheet(apptSheetData);
      
      for (let r = 0; r < appointmentsIndex.length; r++) {
        const cellRef = XLSX.utils.encode_cell({ r: r + 1, c: 3 });
        if (appointmentsIndex[r].PDFLink) {
          apptWs[cellRef] = {
            t: "s",
            v: "📎 Open PDF",
            l: { Target: appointmentsIndex[r].PDFLink },
          };
        }
      }
      
      apptWs["!cols"] = [
        { wch: 30 },
        { wch: 14 },
        { wch: 18 },
        { wch: 15 },
      ];
      
      XLSX.utils.book_append_sheet(wb, apptWs, "Appointments Index");
    }

    if (wb.SheetNames.length > 0) {
      hasAnyData = true;
      const xlsxBuffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
      const filePath = `${userId}/consolidated_export_${Date.now()}.xlsx`;
      const xlsxBlob = new Blob([xlsxBuffer], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
      const { error: uploadError } = await serviceClient.storage
        .from("scraped-data")
        .upload(filePath, xlsxBlob, { contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
      if (uploadError) {
        logParts.push(`❌ Workbook upload error: ${uploadError.message}`);
      } else {
        const totalRows = collectedSheets.reduce((sum, s) => sum + csvToJson(s.csv).length, 0) + soapIndex.length;
        await serviceClient.from("scraped_data_results").insert({
          scrape_job_id: job.id,
          user_id: userId,
          data_type: "consolidated_export",
          file_path: filePath,
          row_count: totalRows,
        });
        logParts.push(`✅ Consolidated workbook: ${filePath} (${wb.SheetNames.join(", ")} — ${totalRows} total rows)`);
      }
    }

    // Cleanup temp batch storage files
    try {
      const tmpFiles = ["collectedSheets", "soapIndex", "financialsLedgerRows", "appointmentsIndex"];
      await serviceClient.storage.from("scraped-data").remove(
        tmpFiles.map(f => `_batch_tmp/${job.id}/${f}.json`)
      );
    } catch { /* ignore cleanup errors */ }

    const finalMessage = hasAnyData ? null : "No data was retrieved. Check the log for details.";
    await serviceClient.from("scrape_jobs").update({
      status: "completed",
      progress: 100,
      log_output: logParts.join("\n"),
      error_message: finalMessage,
    }).eq("id", job.id);

    return new Response(JSON.stringify({ success: true, jobId: job.id, hasData: hasAnyData }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });

  } catch (error) {
    console.error("Error:", error);
    // Try to mark the job as failed so it doesn't stay stuck as "running"
    try {
      const supabase = createClient(
        Deno.env.get("SUPABASE_URL")!,
        Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
      );
      // Find the most recent running job and mark it failed
      const { data: runningJobs } = await supabase.from("scrape_jobs")
        .select("id")
        .eq("status", "running")
        .order("created_at", { ascending: false })
        .limit(5);
      if (runningJobs && runningJobs.length > 0) {
        for (const j of runningJobs) {
          await supabase.from("scrape_jobs").update({
            status: "failed",
            error_message: `Unexpected error: ${(error as any).message}. Please retry.`,
          }).eq("id", j.id);
        }
      }
    } catch { /* best effort */ }
    return new Response(JSON.stringify({ error: (error as any).message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
