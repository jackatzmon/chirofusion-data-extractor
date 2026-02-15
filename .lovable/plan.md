

# Patient Financials: Per-Patient Account Ledger Approach

## Overview
Replace the current financials scraping (which uses `GetPatientStatementGridData`) with a per-patient account ledger approach that matches the manual browser workflow. This integrates into the existing per-patient loop pattern already used for SOAP notes/medical files.

## How It Works

The new flow for each patient:
1. **Set billing context** -- call `SetBillingDefaultPageInSession` with `billingDefaultPage=PatientAccounting`
2. **Load patient ledger** -- call `ShowLedger` with `IsLedger=1&ShowUac=true` (returns HTML with all visit/transaction rows)
3. **Parse the HTML response** -- extract the ledger table rows (charges, payments, adjustments, dates, CPT codes, balances) into structured data
4. Since the "download" step produces a client-side PDF (Kendo UI), we skip it and instead parse the server-rendered HTML directly for structured data -- this is actually superior because we get machine-readable rows instead of a flat PDF

## Technical Changes

### Edge Function (`supabase/functions/chirofusion-scrape/index.ts`)

**Replace the `financials` case** (currently lines 1553-1658) with a per-patient loop:

1. Reuse the existing `getAllPatientNames()` and `findPatientInfo()` helpers (already built for SOAP notes)
2. For each patient:
   - Search for the patient to get their `patientId`
   - Set patient context via `SetVisitIdInSession` (same as SOAP notes flow)
   - Call `SetBillingDefaultPageInSession` to switch to Patient Accounting
   - Call `ShowLedger` with `IsLedger=1&ShowUac=true` to get the ledger HTML
   - Parse the HTML table for transaction rows (date, description, CPT code, charges, payments, adjustments, balance)
   - Collect all rows with patient name prepended
3. Add batch/timeout support (same pattern as SOAP notes -- call `selfInvoke` when timing out)
4. Output all collected ledger rows as a CSV sheet in the consolidated workbook

**Add HTML table parser** -- a helper function to extract `<tr>` rows from the ShowLedger HTML response, pulling values from `<td>` cells

### Dashboard UI (`src/pages/Dashboard.tsx`)

- Update the `typeLabels` map to show "Patient Ledgers" for the financials sheet in the workbook
- No other UI changes needed -- the existing "Financials" checkbox and download flow remain the same

### Workbook Output
- The "Financials" sheet in the consolidated Excel workbook will now contain per-patient ledger rows with columns like: PatientName, Date, Description, CPTCode, Charges, Payments, Adjustments, Balance

## Batch Processing
- Uses the same timeout/self-invoke pattern as SOAP notes
- Stores resume state (patient index, collected data) in the DB `batch_state` column
- Each batch processes as many patients as possible within the 100s runtime limit

## Risk & Fallback
- If `ShowLedger` returns unexpected HTML or no table data for a patient, that patient is logged and skipped
- The old `GetPatientStatementGridData` approach will be removed since you indicated this new method is better

