-- Add 'aborted' to allowed status values
ALTER TABLE public.scrape_jobs DROP CONSTRAINT scrape_jobs_status_check;
ALTER TABLE public.scrape_jobs ADD CONSTRAINT scrape_jobs_status_check 
  CHECK (status = ANY (ARRAY['pending'::text, 'running'::text, 'completed'::text, 'failed'::text, 'aborted'::text]));

-- Allow users to delete their own results (needed for cleanup)
CREATE POLICY "Users can delete own results"
  ON public.scraped_data_results
  FOR DELETE
  USING (auth.uid() = user_id);
