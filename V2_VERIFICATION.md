# V2 Deploy Verification

## Pushed to GitHub (Lovable auto-deploy) on 2026-04-15

Modules 8-11 (V2.1-V2.4) pushed to `main` on GitHub. Lovable auto-deploys both edge functions (`chirofusion-scrape` and `salvage-csv`) from the `main` branch.

## Type check
- `supabase/functions/chirofusion-scrape/index.ts` — deno check clean (0 errors)
- `supabase/functions/salvage-csv/index.ts` — deno check clean (0 errors)

## Smoke tests
- Module 8 (canonical parser): 6/6 pass
- Module 9 (unified workbook): 4/4 pass
- Module 10 (master rollup): 4/4 pass
- Module 11 (crash-safe flush): 4/4 pass

## Manual browser acceptance (pending)
- [ ] One-patient test (Romanov, Andrew) — 5-tab workbook, CPTCode populated, Master row with totals
- [ ] 5-patient test — all in Master, no duplicate files, "No visits found" for patients without data
- [ ] Full production run (~2,500 patients) — one file, Financials has CPT codes, Master ~2,300 rows
