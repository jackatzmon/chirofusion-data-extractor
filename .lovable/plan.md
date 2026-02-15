

# Update ChiroFusion Scraper to Use Correct Login URL and Flow

## What's Wrong

The current edge function targets the wrong domain (`app.chirofusion.com`) and doesn't handle the mandatory EULA checkbox that ChiroFusion requires on every login.

## What Changes

### 1. Fix the login URL and flow in the edge function

**File:** `supabase/functions/chirofusion-scrape/index.ts`

- Change base URL from `https://app.chirofusion.com` to `https://www.chirofusionlive.com`
- Update login to POST to `https://www.chirofusionlive.com/Account/Login` with form-encoded data including the EULA agreement field (`chkEULA=true`)
- Update all API data endpoints to use `https://www.chirofusionlive.com/api/...` as the base
- Add proper cookie handling for the `.chirofusionlive.com` domain

### 2. Login approach

The login form uses these fields:
- `txtLoginUserName` (username)
- `txtLoginPassword` (password)  
- `chkEULA` (EULA agreement checkbox -- must be checked)

The edge function will POST these as form-encoded data to the login endpoint, collect session cookies from the response, then use those cookies for subsequent API calls.

### 3. API endpoint updates

All data-fetching endpoints will be updated from `app.chirofusion.com` to `www.chirofusionlive.com`:
- Demographics: `/api/Patient/GetAll`
- Appointments: `/api/Appointment/GetAll`
- SOAP Notes: `/api/SoapNote/GetAll`
- Financials: `/api/Billing/GetAll`

Note: These endpoint paths are still best guesses. After the login is working, we can discover the actual API routes by testing.

## Technical Details

The key change in the login section:

```text
POST https://www.chirofusionlive.com/Account/Login
Content-Type: application/x-www-form-urlencoded

Body:
  txtLoginUserName=<username>
  txtLoginPassword=<password>
  chkEULA=on
```

No frontend changes needed -- only the backend function is updated.

