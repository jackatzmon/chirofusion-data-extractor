import { useState } from "react";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useToast } from "@/hooks/use-toast";
import { Square, ChevronDown, ChevronUp, FileSpreadsheet, Trash2 } from "lucide-react";

type ScrapeJob = {
  id: string;
  status: string;
  progress: number;
  data_types: string[];
  error_message: string | null;
  created_at: string;
  mode: string;
  log_output: string | null;
  batch_state?: any;
};

type ScrapeResult = {
  id: string;
  data_type: string;
  file_path: string;
  row_count: number | null;
  created_at: string;
};

function parseProgressFromLog(log: string | null, batchState: any): { current: number; total: number; completedTypes: string[] } {
  const completedTypes: string[] = [];
  let total = 0;
  let current = 0;

  if (log) {
    // Extract total patients
    const totalMatch = log.match(/Got (\d+) patients/);
    if (totalMatch) total = parseInt(totalMatch[1]);

    // Check completed data types
    if (log.includes("✅ Demographics")) completedTypes.push("demographics");
    if (log.includes("✅ Appointments")) completedTypes.push("appointments");
    if (log.includes("✅ Financials") || log.includes("financials sheet")) completedTypes.push("financials");
    if (log.includes("✅ SOAP") || log.includes("soap_notes sheet")) completedTypes.push("soap_notes");
  }

  if (batchState) {
    current = batchState.resumeIndex || 0;
  }

  return { current, total, completedTypes };
}

const DATA_TYPE_LABELS: Record<string, string> = {
  demographics: "Demographics",
  appointments: "Appointments",
  financials: "Financials",
  soap_notes: "SOAP Notes",
};

export default function JobProgressCard({
  jobs,
  results,
  onAbort,
  onDownload,
  onRefresh,
}: {
  jobs: ScrapeJob[];
  results: ScrapeResult[];
  onAbort: () => void;
  onDownload: (filePath: string) => void;
  onRefresh: () => void;
}) {
  const [expandedJobLog, setExpandedJobLog] = useState<string | null>(null);
  const [aborting, setAborting] = useState(false);
  const { toast } = useToast();

  const handleAbort = async (jobId: string) => {
    setAborting(true);
    try {
      const { error } = await supabase
        .from("scrape_jobs")
        .update({ status: "aborted", error_message: "Aborted by user" })
        .eq("id", jobId);
      if (error) throw error;
      toast({ title: "Job aborted", description: "The scrape job has been stopped." });
      onAbort();
    } catch (e: any) {
      toast({ title: "Error", description: e.message, variant: "destructive" });
    } finally {
      setAborting(false);
    }
  };

  const handleDeleteJob = async (jobId: string) => {
    try {
      // Delete associated results first
      await supabase.from("scraped_data_results").delete().eq("scrape_job_id", jobId);
      await supabase.from("scrape_jobs").delete().eq("id", jobId);
      toast({ title: "Deleted", description: "Job and its data removed." });
      onRefresh();
    } catch (e: any) {
      toast({ title: "Error", description: e.message, variant: "destructive" });
    }
  };

  const handleClearAll = async () => {
    try {
      const nonRunning = jobs.filter((j) => j.status !== "running");
      for (const job of nonRunning) {
        await supabase.from("scraped_data_results").delete().eq("scrape_job_id", job.id);
        await supabase.from("scrape_jobs").delete().eq("id", job.id);
      }
      toast({ title: "Cleared", description: `Removed ${nonRunning.length} jobs.` });
      onRefresh();
    } catch (e: any) {
      toast({ title: "Error", description: e.message, variant: "destructive" });
    }
  };

  if (jobs.length === 0) return null;

  const runningJob = jobs.find((j) => j.status === "running");
  const otherJobs = jobs.filter((j) => j.id !== runningJob?.id);

  return (
    <>
      {/* Active job with spinner */}
      {runningJob && (
        <Card className="border-primary/30">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">Processing Job</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <ActiveJobSpinner job={runningJob} />
            <Button
              variant="destructive"
              size="sm"
              className="w-full"
              onClick={() => handleAbort(runningJob.id)}
              disabled={aborting}
            >
              <Square className="h-4 w-4 mr-2" />
              {aborting ? "Aborting..." : "Abort Job"}
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Completed / failed jobs */}
      {otherJobs.length > 0 && (
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>Recent Jobs</CardTitle>
            <Button variant="ghost" size="sm" onClick={handleClearAll} className="text-destructive hover:text-destructive">
              <Trash2 className="h-4 w-4 mr-1" /> Clear All
            </Button>
          </CardHeader>
          <CardContent className="space-y-3">
            {otherJobs.map((job) => {
              const jobResults = results.filter((r) =>
                r.created_at >= job.created_at
              );
              const consolidatedResult = jobResults.find(
                (r) => r.data_type === "consolidated_export"
              );

              return (
                <div
                  key={job.id}
                  className="rounded-md border border-border p-3 space-y-2"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <span
                        className={`text-sm font-medium ${
                          job.status === "failed" || job.status === "aborted"
                            ? "text-destructive"
                            : "text-foreground"
                        }`}
                      >
                        {job.status === "completed"
                          ? "✅ Complete"
                          : job.status === "aborted"
                          ? "⛔ Aborted"
                          : job.status}
                      </span>
                      <span className="text-xs bg-muted text-muted-foreground px-2 py-0.5 rounded">
                        {job.mode || "scrape"}
                      </span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-muted-foreground">
                        {new Date(job.created_at).toLocaleString()}
                      </span>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6 text-muted-foreground hover:text-destructive"
                        onClick={() => handleDeleteJob(job.id)}
                      >
                        <Trash2 className="h-3 w-3" />
                      </Button>
                    </div>
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {(job.data_types || []).join(", ")}
                  </div>
                  {job.error_message && (
                    <p className="text-xs text-destructive">
                      {job.error_message}
                    </p>
                  )}

                  {/* Download button for completed jobs */}
                  {job.status === "completed" && consolidatedResult && (
                    <Button
                      variant="outline"
                      size="sm"
                      className="w-full"
                      onClick={() => onDownload(consolidatedResult.file_path)}
                    >
                      <FileSpreadsheet className="h-4 w-4 mr-2" />
                      Open Spreadsheet
                    </Button>
                  )}

                  {/* Log toggle */}
                  {job.log_output && (
                    <div>
                      <button
                        onClick={() =>
                          setExpandedJobLog(
                            expandedJobLog === job.id ? null : job.id
                          )
                        }
                        className="flex items-center gap-1 text-xs text-primary hover:underline"
                      >
                        {expandedJobLog === job.id ? (
                          <ChevronUp className="h-3 w-3" />
                        ) : (
                          <ChevronDown className="h-3 w-3" />
                        )}
                        {expandedJobLog === job.id ? "Hide Log" : "View Log"}
                      </button>
                      {expandedJobLog === job.id && (
                        <pre className="mt-2 p-3 bg-muted rounded text-xs text-muted-foreground overflow-x-auto max-h-96 overflow-y-auto whitespace-pre-wrap break-words">
                          {job.log_output}
                        </pre>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </CardContent>
        </Card>
      )}
    </>
  );
}

function ActiveJobSpinner({ job }: { job: ScrapeJob }) {
  const { current, total, completedTypes } = parseProgressFromLog(
    job.log_output,
    job.batch_state
  );
  const activeTypes = (job.data_types || []).filter(
    (t) => !completedTypes.includes(t)
  );
  const currentType = activeTypes[0];
  const pct = total > 0 ? Math.round((current / total) * 100) : 0;

  return (
    <div className="flex flex-col items-center gap-3">
      {/* Circular spinner */}
      <div className="relative h-28 w-28">
        <svg className="h-28 w-28 -rotate-90" viewBox="0 0 112 112">
          {/* Background circle */}
          <circle
            cx="56"
            cy="56"
            r="48"
            fill="none"
            className="stroke-muted"
            strokeWidth="8"
          />
          {/* Progress arc */}
          <circle
            cx="56"
            cy="56"
            r="48"
            fill="none"
            className="stroke-primary transition-all duration-500"
            strokeWidth="8"
            strokeLinecap="round"
            strokeDasharray={`${2 * Math.PI * 48}`}
            strokeDashoffset={`${2 * Math.PI * 48 * (1 - pct / 100)}`}
          />
        </svg>
        {/* Spinning overlay for active feel */}
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="animate-spin h-28 w-28 rounded-full border-2 border-transparent border-t-primary/30" />
        </div>
        {/* Center text */}
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span className="text-lg font-bold text-foreground">{pct}%</span>
          {total > 0 && (
            <span className="text-[10px] text-muted-foreground">
              {current}/{total}
            </span>
          )}
        </div>
      </div>

      {/* Status line */}
      <p className="text-sm text-center text-muted-foreground">
        {total > 0 ? (
          <>
            Patient {current}/{total}
            {currentType && (
              <> — <span className="font-medium text-foreground">{DATA_TYPE_LABELS[currentType] || currentType}</span></>
            )}
          </>
        ) : (
          "Starting up..."
        )}
      </p>

      {/* Completed types badges */}
      {completedTypes.length > 0 && (
        <div className="flex flex-wrap gap-1.5 justify-center">
          {completedTypes.map((t) => (
            <span
              key={t}
              className="text-xs bg-primary/10 text-primary px-2 py-0.5 rounded-full"
            >
              ✓ {DATA_TYPE_LABELS[t] || t}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
