

# Rebuild ChiroFusion Scraper with Real Workflows

## The Problem

The current scraper uses fake API endpoints (`/api/Patient/GetAll`, etc.) that don't exist. ChiroFusion is a traditional web app with forms, not a REST API. Every scrape "completes" instantly with zero data.

## What We Know (from your input)

| Data Type | URL | Workflow |
|-----------|-----|----------|
| Patient List | `/User/Scheduler` | Patient Reports > Patient List dropdown > submit |
| Appointments | `/User/Scheduler` | Scheduling Reports > Completed Visits > date range (08/10/2021 to present) > submit |
| Financials | `/Billing/` | Patient Accounts > search patient name > copy data (no export) |
| SOAP Notes | Home (`/`) | Search patient > "click here to load more" if needed > click patient > Medical File > select all > Export as PDF |

## Strategy: Two-Phase Approach

### Phase 1 -- Discovery (first deploy)

Before we can build the real scraper, we need to see what ChiroFusion's pages actually contain. The edge function will:

1. Log in (this part already works)
2. Fetch the Scheduler page (`/User/Scheduler`) and log the HTML
3. Fetch the Billing page (`/Billing/`) and log the HTML
4. Fetch the home page (`/`) and log the HTML

This lets us inspect the actual form fields, dropdown values, AJAX endpoints, and hidden parameters that ChiroFusion uses behind the scenes. We review the logs, then move to Phase 2.

### Phase 2 -- Real Scraping (second deploy, after reviewing logs)

Once we know the real endpoints, rebuild each data type scraper:

- **Patient List**: POST the correct form data to the Scheduler reports endpoint, parse the HTML table or CSV response
- **Appointments**: POST the Completed Visits form with date range, parse results
- **Financials**: For each patient from the list, fetch their `/Billing/` page, extract the account data from HTML
- **SOAP Notes**: For each patient, navigate to their medical file page, trigger the PDF export, download and store the PDF

## Changes

### 1. Edge Function: `supabase/functions/chirofusion-scrape/index.ts`

**Phase 1 changes:**
- Replace all fake `/api/` calls with HTML page fetches
- Add a `mode` parameter: `"discover"` or `"scrape"`
- In discover mode: fetch Scheduler, Billing, and Home pages, log their HTML structure (forms, selects, action URLs)
- In scrape mode: use the discovered endpoints (updated after Phase 1)

**Phase 2 changes (after discovery):**
- Patient List: submit the real report form, parse HTML table into CSV
- Appointments: submit Completed Visits form with date params, parse results
- Financials: iterate through patient list, fetch each patient's billing page, extract table data
- SOAP Notes: iterate through patients, fetch medical file page, find and download PDF export links
- Store all files in storage, track per-patient results

### 2. Dashboard: `src/pages/Dashboard.tsx`

- Add a **"Discover Endpoints"** button for Phase 1 (runs discovery mode)
- Add **date range inputs** (start: 08/10/2021, end: today) for the appointments report
- Add **auto-refresh polling** for running jobs so you can see real progress
- Show **"No data found"** clearly when a scrape returns empty instead of silently completing
- Add a **job log viewer** that shows what the scraper found/attempted

### 3. Database (if needed)

- Add a `mode` column to `scrape_jobs` table to distinguish discovery vs. scrape runs
- Add a `log_output` text column to store discovery HTML snippets for review

## Implementation Order

1. Add database columns for discovery mode and log output
2. Update edge function with discover mode (fetch real pages, log HTML)
3. Update dashboard with discover button and better status feedback
4. Deploy and run discovery to see actual page structure
5. Update edge function with real form submissions based on discovered endpoints
6. Add date range inputs and per-patient iteration to the scraper
7. Test end-to-end

## Important Caveats

- SOAP notes export as PDF per-patient will be slow for many patients (one-by-one)
- Financials have no export button, so we'll need to parse HTML tables
- Edge functions have a timeout limit, so for large patient lists we may need to batch the work across multiple function calls
- The discovery phase is essential -- without seeing the real HTML, we'd be guessing again

