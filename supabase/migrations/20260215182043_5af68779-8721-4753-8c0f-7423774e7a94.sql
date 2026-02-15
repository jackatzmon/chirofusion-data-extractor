
ALTER TABLE public.scrape_jobs ADD COLUMN IF NOT EXISTS batch_state jsonb DEFAULT null;
