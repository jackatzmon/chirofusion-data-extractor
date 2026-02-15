
-- Add mode column to distinguish discovery vs scrape runs
ALTER TABLE public.scrape_jobs ADD COLUMN mode text NOT NULL DEFAULT 'scrape';

-- Add log_output column to store discovery HTML snippets
ALTER TABLE public.scrape_jobs ADD COLUMN log_output text;
