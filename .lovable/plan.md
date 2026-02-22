

# Fixes for Batch Resume, Stale Cleanup, and Progress Updates

## Fix 1: Use service role key for self-invoke (prevents 401 after 1 hour)

The self-invoke calls currently pass the user's JWT (`authHeader`), which expires after ~1 hour. Both self-invoke locations will be changed to use the `SUPABASE_SERVICE_ROLE_KEY` instead.

**File:** `supabase/functions/chirofusion-scrape/index.ts`

- **Line 387** (initial self-invoke): Change `"Authorization": authHeader!` to `"Authorization": "Bearer " + Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!`
- **Line 481** (batch continuation self-invoke): Same change

## Fix 2: Stale job cleanup checks `updated_at` instead of `created_at`

Currently `cleanupStaleJobs` kills jobs created more than 1 hour ago, which kills legitimate long-running batch jobs. Change it to check `updated_at` so only jobs that haven't had any DB activity for 1 hour are cleaned up.

**File:** `supabase/functions/chirofusion-scrape/index.ts`

- **Line 65**: Change `.lt("created_at", oneHourAgo)` to `.lt("updated_at", oneHourAgo)`

## Fix 3: More frequent progress updates (every 10 patients instead of 50)

So you can see progress moving in the dashboard sooner rather than waiting for 50 patients to process.

**File:** `supabase/functions/chirofusion-scrape/index.ts`

- **Line 2004**: Change `% 50 === 0` to `% 10 === 0`
- **Line 2309**: Change `% 50 === 0` to `% 10 === 0`

## Fix 4: Add live log viewer with auto-scroll to active job card

Add the running job's log output directly in the active job card so you can see what's happening in real time, with auto-scroll to bottom.

**File:** `src/components/JobProgressCard.tsx`

- Add a scrollable `<pre>` block between `ActiveJobSpinner` and `LivePageViewer` (after line 136)
- Shows `runningJob.log_output` in a max-height container with overflow scroll
- Auto-scrolls to bottom on each update, but stops auto-scrolling if you manually scroll up
- Uses a ref + onScroll handler to detect if you're pinned to the bottom

## Summary of changes

| File | What changes |
|------|-------------|
| `supabase/functions/chirofusion-scrape/index.ts` | Lines 65, 387, 481, 2004, 2309 |
| `src/components/JobProgressCard.tsx` | Add live log viewer component after line 136 |

