import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { useToast } from "@/hooks/use-toast";
import { Progress } from "@/components/ui/progress";
import { LogOut, Download, Play, Loader2, Search, ChevronDown, ChevronUp } from "lucide-react";

const DATA_TYPES = [
  { id: "demographics", label: "Patient Demographics" },
  { id: "appointments", label: "Appointments & Scheduling" },
  { id: "soap_notes", label: "SOAP Notes & Clinical" },
  { id: "financials", label: "Financial Transactions" },
];

type ScrapeJob = {
  id: string;
  status: string;
  progress: number;
  data_types: string[];
  error_message: string | null;
  created_at: string;
  mode: string;
  log_output: string | null;
};

type ScrapeResult = {
  id: string;
  data_type: string;
  file_path: string;
  row_count: number | null;
  created_at: string;
};

const Dashboard = () => {
  const [cfUsername, setCfUsername] = useState("");
  const [cfPassword, setCfPassword] = useState("");
  const [hasCreds, setHasCreds] = useState(false);
  const [selectedTypes, setSelectedTypes] = useState<string[]>([]);
  const [jobs, setJobs] = useState<ScrapeJob[]>([]);
  const [results, setResults] = useState<ScrapeResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [discovering, setDiscovering] = useState(false);
  const [savingCreds, setSavingCreds] = useState(false);
  const [expandedJobLog, setExpandedJobLog] = useState<string | null>(null);
  const [dateFrom, setDateFrom] = useState("08/10/2021");
  const [dateTo, setDateTo] = useState(
    new Date().toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" })
  );
  const navigate = useNavigate();
  const { toast } = useToast();

  const loadJobs = useCallback(async () => {
    const { data } = await supabase
      .from("scrape_jobs")
      .select("*")
      .order("created_at", { ascending: false })
      .limit(10);
    if (data) setJobs(data as ScrapeJob[]);
  }, []);

  useEffect(() => {
    loadCredentials();
    loadJobs();
    loadResults();
  }, [loadJobs]);

  useEffect(() => {
    const hasRunning = jobs.some((j) => j.status === "running");
    if (!hasRunning) return;
    // Keep polling for up to 30 minutes (batch processing can take a while)
    const startedAt = Date.now();
    const interval = setInterval(() => {
      if (Date.now() - startedAt > 30 * 60 * 1000) {
        clearInterval(interval);
        return;
      }
      loadJobs(); loadResults();
    }, 5000);
    return () => clearInterval(interval);
  }, [jobs, loadJobs]);

  const loadCredentials = async () => {
    const { data } = await supabase.from("chirofusion_credentials").select("*").maybeSingle();
    if (data) { setHasCreds(true); setCfUsername(data.cf_username); }
  };

  const loadResults = async () => {
    const { data } = await supabase.from("scraped_data_results").select("*").order("created_at", { ascending: false });
    if (data) setResults(data);
  };

  const saveCredentials = async () => {
    setSavingCreds(true);
    try {
      const { data: { user } } = await supabase.auth.getUser();
      if (!user) {
        toast({ title: "Error", description: "Not authenticated. Please sign in again.", variant: "destructive" });
        navigate("/auth");
        return;
      }
      const { error } = hasCreds
        ? await supabase.from("chirofusion_credentials").update({ cf_username: cfUsername, cf_password: cfPassword }).eq("user_id", user.id)
        : await supabase.from("chirofusion_credentials").insert({ user_id: user.id, cf_username: cfUsername, cf_password: cfPassword });
      if (error) {
        toast({ title: "Error", description: error.message, variant: "destructive" });
      } else {
        setHasCreds(true); setCfPassword("");
        toast({ title: "Saved", description: "ChiroFusion credentials saved." });
      }
    } catch (error: any) {
      toast({ title: "Error", description: error.message || "Failed to save credentials.", variant: "destructive" });
    } finally {
      setSavingCreds(false);
    }
  };

  const startDiscover = async () => {
    setDiscovering(true);
    try {
      const { error } = await supabase.functions.invoke("chirofusion-scrape", { body: { mode: "discover" } });
      if (error) throw error;
      toast({ title: "Discovery started", description: "Fetching ChiroFusion page structures..." });
      loadJobs();
    } catch (error: any) {
      toast({ title: "Error", description: error.message, variant: "destructive" });
    } finally { setDiscovering(false); }
  };

  const startScrape = async () => {
    if (selectedTypes.length === 0) {
      toast({ title: "Select data types", description: "Choose at least one.", variant: "destructive" });
      return;
    }
    setLoading(true);
    try {
      const { error } = await supabase.functions.invoke("chirofusion-scrape", {
        body: { dataTypes: selectedTypes, mode: "scrape", dateFrom, dateTo },
      });
      if (error) throw error;
      toast({ title: "Processing scrape", description: "Your data download is running." });
      loadJobs();
    } catch (error: any) {
      toast({ title: "Error", description: error.message, variant: "destructive" });
    } finally { setLoading(false); }
  };

  const downloadFile = async (filePath: string) => {
    const { data, error } = await supabase.storage.from("scraped-data").createSignedUrl(filePath, 3600);
    if (error) { toast({ title: "Error", description: "Could not generate download link.", variant: "destructive" }); return; }
    window.open(data.signedUrl, "_blank");
  };

  const handleLogout = async () => { await supabase.auth.signOut(); navigate("/auth"); };

  const toggleType = (id: string) => {
    setSelectedTypes((prev) => prev.includes(id) ? prev.filter((t) => t !== id) : [...prev, id]);
  };

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b border-border px-6 py-4 flex items-center justify-between">
        <h1 className="text-xl font-bold text-foreground">ChiroFusion Data Exporter</h1>
        <Button variant="ghost" size="sm" onClick={handleLogout}>
          <LogOut className="h-4 w-4 mr-2" /> Sign Out
        </Button>
      </header>

      <main className="max-w-3xl mx-auto p-6 space-y-6">
        {/* Credentials Card */}
        <Card>
          <CardHeader>
            <CardTitle>ChiroFusion Credentials</CardTitle>
            <CardDescription>
              {hasCreds ? "Credentials saved. Update below if needed." : "Enter your ChiroFusion login to enable data downloads."}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <Input placeholder="ChiroFusion Username" value={cfUsername} onChange={(e) => setCfUsername(e.target.value)} />
            <Input type="password" placeholder={hasCreds ? "Enter new password to update" : "ChiroFusion Password"} value={cfPassword} onChange={(e) => setCfPassword(e.target.value)} />
            <Button onClick={saveCredentials} disabled={savingCreds || !cfUsername || !cfPassword}>
              {savingCreds ? "Saving..." : hasCreds ? "Update Credentials" : "Save Credentials"}
            </Button>
          </CardContent>
        </Card>

        {/* Discovery Card */}
        <Card>
          <CardHeader>
            <CardTitle>Discover Endpoints</CardTitle>
            <CardDescription>Fetch ChiroFusion pages to inspect forms and AJAX endpoints.</CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={startDiscover} disabled={discovering || !hasCreds} variant="secondary" className="w-full">
              {discovering ? <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Discovering...</> : <><Search className="h-4 w-4 mr-2" /> Discover Endpoints</>}
            </Button>
            {!hasCreds && <p className="text-sm text-muted-foreground mt-2">Save your credentials first.</p>}
          </CardContent>
        </Card>

        {/* Data Selection Card */}
        <Card>
          <CardHeader>
            <CardTitle>Download Data</CardTitle>
            <CardDescription>Select data types and configure options.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {DATA_TYPES.map((type) => (
                <label key={type.id} className="flex items-center gap-3 rounded-md border border-border p-3 cursor-pointer hover:bg-accent transition-colors">
                  <Checkbox checked={selectedTypes.includes(type.id)} onCheckedChange={() => toggleType(type.id)} />
                  <span className="text-sm font-medium text-foreground">{type.label}</span>
                </label>
              ))}
            </div>

            {/* Date Range for Appointments */}
            {selectedTypes.includes("appointments") && (
              <div className="rounded-md border border-border p-4 space-y-3">
                <p className="text-sm font-medium text-foreground">Appointment Date Range</p>
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="text-xs text-muted-foreground">From</label>
                    <Input value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} placeholder="MM/DD/YYYY" />
                  </div>
                  <div>
                    <label className="text-xs text-muted-foreground">To</label>
                    <Input value={dateTo} onChange={(e) => setDateTo(e.target.value)} placeholder="MM/DD/YYYY" />
                  </div>
                </div>
              </div>
            )}

            <Button onClick={startScrape} disabled={loading || !hasCreds || selectedTypes.length === 0} className="w-full">
              {loading ? <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Processing...</> : <><Play className="h-4 w-4 mr-2" /> Start Download</>}
            </Button>
            {!hasCreds && <p className="text-sm text-muted-foreground">Save your credentials first to enable downloads.</p>}
          </CardContent>
        </Card>

        {/* Jobs Card */}
        {jobs.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Recent Jobs</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {jobs.map((job) => (
                <div key={job.id} className="rounded-md border border-border p-3 space-y-2">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <span className={`text-sm font-medium ${job.status === "failed" ? "text-destructive" : "text-foreground"}`}>
                        {job.status === "completed" ? "Scrape Complete" : job.status === "running" ? "Processing Scrape" : job.status}
                      </span>
                      <span className="text-xs bg-muted text-muted-foreground px-2 py-0.5 rounded">{job.mode || "scrape"}</span>
                    </div>
                    <span className="text-xs text-muted-foreground">{new Date(job.created_at).toLocaleString()}</span>
                  </div>
                  <div className="text-xs text-muted-foreground">{(job.data_types || []).join(", ")}</div>
                  {job.status === "running" && <Progress value={job.progress} />}
                  {job.error_message && <p className="text-xs text-destructive">{job.error_message}</p>}
                  {job.log_output && (
                    <div>
                      <button
                        onClick={() => setExpandedJobLog(expandedJobLog === job.id ? null : job.id)}
                        className="flex items-center gap-1 text-xs text-primary hover:underline"
                      >
                        {expandedJobLog === job.id ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
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
              ))}
            </CardContent>
          </Card>
        )}

        {/* Results Card */}
        {results.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Downloaded Files</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              {results.map((result) => (
                <div key={result.id} className="flex items-center justify-between rounded-md border border-border p-3">
                  <div>
                    <span className="text-sm font-medium capitalize text-foreground">{result.data_type === "consolidated_export" ? "📊 Consolidated Workbook" : result.data_type.replace("_", " ")}</span>
                    {result.row_count != null && <span className="ml-2 text-xs text-muted-foreground">({result.row_count} rows)</span>}
                  </div>
                  <Button variant="outline" size="sm" onClick={() => downloadFile(result.file_path)}>
                    <Download className="h-4 w-4 mr-1" /> Download
                  </Button>
                </div>
              ))}
            </CardContent>
          </Card>
        )}
      </main>
    </div>
  );
};

export default Dashboard;
