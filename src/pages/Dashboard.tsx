import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { useToast } from "@/hooks/use-toast";
import { LogOut, Play, Loader2, Search } from "lucide-react";
import JobProgressCard from "@/components/JobProgressCard";

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
  batch_state?: any;
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
  const [testPatientName, setTestPatientName] = useState("");
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
      const { error } = await supabase.from("chirofusion_credentials").upsert(
        { user_id: user.id, cf_username: cfUsername, cf_password: cfPassword },
        { onConflict: "user_id" }
      );
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

  const startScrape = async (limit?: number, patientName?: string) => {
    if (selectedTypes.length === 0) {
      toast({ title: "Select data types", description: "Choose at least one.", variant: "destructive" });
      return;
    }
    setLoading(true);
    try {
      const payload: any = { dataTypes: selectedTypes, mode: "scrape", dateFrom, dateTo };
      if (limit) payload.testLimit = limit;
      if (patientName) payload.testPatientName = patientName;
      const { error } = await supabase.functions.invoke("chirofusion-scrape", { body: payload });
      if (error) throw error;
      const desc = patientName ? `Testing with "${patientName}"` : limit ? `Testing with ${limit} patients.` : "Your data download is running.";
      toast({ title: limit || patientName ? "Test scrape started" : "Processing scrape", description: desc });
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

            {/* Test specific patient */}
            <div className="flex gap-2">
              <Input
                placeholder="Patient name (e.g. Dagostino, Siyka)"
                value={testPatientName}
                onChange={(e) => setTestPatientName(e.target.value)}
                className="flex-1"
              />
              <Button
                onClick={() => startScrape(undefined, testPatientName)}
                disabled={loading || !hasCreds || selectedTypes.length === 0 || !testPatientName.trim()}
                variant="secondary"
                size="sm"
              >
                {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <>🧪 Test Patient</>}
              </Button>
            </div>

            <div className="flex gap-2">
              <Button onClick={() => startScrape(5)} disabled={loading || !hasCreds || selectedTypes.length === 0} variant="secondary" className="flex-1">
                {loading ? <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Processing...</> : <>🧪 Test (5 patients)</>}
              </Button>
              <Button onClick={() => startScrape()} disabled={loading || !hasCreds || selectedTypes.length === 0} className="flex-1">
                {loading ? <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Processing...</> : <><Play className="h-4 w-4 mr-2" /> Start Full Download</>}
              </Button>
            </div>
            {!hasCreds && <p className="text-sm text-muted-foreground">Save your credentials first to enable downloads.</p>}
          </CardContent>
        </Card>

        {/* Jobs & Progress */}
        <JobProgressCard
          jobs={jobs}
          results={results}
          onAbort={() => { loadJobs(); loadResults(); }}
          onDownload={downloadFile}
        />
      </main>
    </div>
  );
};

export default Dashboard;
