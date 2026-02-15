import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const authHeader = req.headers.get("Authorization");
    if (!authHeader?.startsWith("Bearer ")) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_ANON_KEY")!,
      { global: { headers: { Authorization: authHeader } } }
    );

    const { data: claimsData, error: claimsError } = await supabase.auth.getClaims(authHeader.replace("Bearer ", ""));
    if (claimsError || !claimsData?.claims) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const userId = claimsData.claims.sub as string;
    const { dataTypes } = await req.json();

    // Get user's ChiroFusion credentials
    const { data: creds, error: credsError } = await supabase
      .from("chirofusion_credentials")
      .select("*")
      .eq("user_id", userId)
      .maybeSingle();

    if (credsError || !creds) {
      return new Response(JSON.stringify({ error: "ChiroFusion credentials not found. Please save them first." }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // Create a scrape job
    const { data: job, error: jobError } = await supabase
      .from("scrape_jobs")
      .insert({ user_id: userId, data_types: dataTypes, status: "running" })
      .select()
      .single();

    if (jobError) {
      return new Response(JSON.stringify({ error: jobError.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // Use service role for storage and job updates
    const serviceClient = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    // Step 1: Login to ChiroFusion
    console.log("Logging into ChiroFusion...");
    let sessionCookies: string = "";

    try {
      const loginRes = await fetch("https://www.chirofusionlive.com/Account/Login", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          txtLoginUserName: creds.cf_username,
          txtLoginPassword: creds.cf_password,
          chkEULA: "on",
        }),
        redirect: "manual",
      });

      const cookies = loginRes.headers.getSetCookie?.() || [];
      sessionCookies = cookies.map((c: string) => c.split(";")[0]).join("; ");

      if (!sessionCookies) {
        throw new Error("Failed to authenticate with ChiroFusion. Check your credentials.");
      }

      console.log("Login successful, status:", loginRes.status);
    } catch (loginError: any) {
      await serviceClient.from("scrape_jobs").update({ status: "failed", error_message: loginError.message }).eq("id", job.id);
      return new Response(JSON.stringify({ error: loginError.message }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // Step 2: Scrape each data type
    let progress = 0;
    const totalTypes = dataTypes.length;

    for (const dataType of dataTypes) {
      try {
        console.log(`Scraping ${dataType}...`);
        let csvContent = "";

        switch (dataType) {
          case "demographics": {
            const res = await fetch("https://www.chirofusionlive.com/api/Patient/GetAll", {
              headers: { Cookie: sessionCookies, Accept: "application/json" },
            });
            const patients = await res.json();
            if (Array.isArray(patients) && patients.length > 0) {
              const headers = Object.keys(patients[0]);
              csvContent = headers.join(",") + "\n" + patients.map((p: any) => headers.map((h) => `"${String(p[h] ?? "").replace(/"/g, '""')}"`).join(",")).join("\n");
            }
            break;
          }
          case "appointments": {
            const res = await fetch("https://www.chirofusionlive.com/api/Appointment/GetAll", {
              headers: { Cookie: sessionCookies, Accept: "application/json" },
            });
            const appts = await res.json();
            if (Array.isArray(appts) && appts.length > 0) {
              const headers = Object.keys(appts[0]);
              csvContent = headers.join(",") + "\n" + appts.map((a: any) => headers.map((h) => `"${String(a[h] ?? "").replace(/"/g, '""')}"`).join(",")).join("\n");
            }
            break;
          }
          case "soap_notes": {
            const res = await fetch("https://www.chirofusionlive.com/api/SoapNote/GetAll", {
              headers: { Cookie: sessionCookies, Accept: "application/json" },
            });
            const notes = await res.json();
            if (Array.isArray(notes) && notes.length > 0) {
              const headers = Object.keys(notes[0]);
              csvContent = headers.join(",") + "\n" + notes.map((n: any) => headers.map((h) => `"${String(n[h] ?? "").replace(/"/g, '""')}"`).join(",")).join("\n");
            }
            break;
          }
          case "financials": {
            const res = await fetch("https://www.chirofusionlive.com/api/Billing/GetAll", {
              headers: { Cookie: sessionCookies, Accept: "application/json" },
            });
            const bills = await res.json();
            if (Array.isArray(bills) && bills.length > 0) {
              const headers = Object.keys(bills[0]);
              csvContent = headers.join(",") + "\n" + bills.map((b: any) => headers.map((h) => `"${String(b[h] ?? "").replace(/"/g, '""')}"`).join(",")).join("\n");
            }
            break;
          }
        }

        if (csvContent) {
          const filePath = `${userId}/${dataType}_${Date.now()}.csv`;
          const blob = new Blob([csvContent], { type: "text/csv" });

          const { error: uploadError } = await serviceClient.storage
            .from("scraped-data")
            .upload(filePath, blob, { contentType: "text/csv" });

          if (uploadError) {
            console.error(`Upload error for ${dataType}:`, uploadError);
          } else {
            const rowCount = csvContent.split("\n").length - 1;
            await serviceClient.from("scraped_data_results").insert({
              scrape_job_id: job.id,
              user_id: userId,
              data_type: dataType,
              file_path: filePath,
              row_count: rowCount,
            });
          }
        } else {
          console.log(`No data returned for ${dataType}`);
        }

        progress += Math.round(100 / totalTypes);
        await serviceClient.from("scrape_jobs").update({ progress: Math.min(progress, 100) }).eq("id", job.id);
      } catch (scrapeError: any) {
        console.error(`Error scraping ${dataType}:`, scrapeError);
      }
    }

    // Mark job complete
    await serviceClient.from("scrape_jobs").update({ status: "completed", progress: 100 }).eq("id", job.id);

    return new Response(JSON.stringify({ success: true, jobId: job.id }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (error: any) {
    console.error("Error:", error);
    return new Response(JSON.stringify({ error: error.message }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
