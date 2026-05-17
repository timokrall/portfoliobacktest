# Portfolio Backtest

Single-file React app, deployable to GitHub Pages, fed by a weekly GitHub Action
that commits fresh price/NAV CSVs into `data/`.

## Setup

1. Push this repo to GitHub.
2. Repo **Settings → Pages → Source: Deploy from a branch → `main` / root**.
3. Repo **Settings → Actions → General → Workflow permissions →** enable
   **"Read and write permissions"** (so the data-bot can commit to `data/`).
4. (Optional) Add a Google OAuth client ID to enable Drive sync — see comment
   block at the top of `index.html`.
5. (Optional but recommended) Add the **`STOOQ_COOKIE`** secret — see below.

The Action runs every Monday 06:00 UTC. Trigger it manually the first time from
**Actions → Weekly data update → Run workflow** to populate `data/`.

## How the Stooq cookie is stored securely

Stooq has two auth modes. The free CSV endpoint usually works **anonymously**
for the weekly 3-symbol pull. If it gets rate-limited, you can pin one of:

- **`STOOQ_APIKEY`** — preferred if you have a paid Stooq subscription. Your
  download URLs contain `&apikey=…` — copy just that value.
- **`STOOQ_COOKIE`** — free-account fallback. A session cookie from a
  logged-in browser session at stooq.com.

Both are stored as **GitHub repository secrets** — GitHub encrypts them at
rest, only exposes them to the running workflow, and never prints them to
logs. They are never in the source code, never on the deployed Pages site,
never visible to anyone without admin access to the repo.

**Forks do not inherit your secret.** GitHub Actions secrets are scoped to the
repo they were created in. If anyone forks this app, the workflow runs in their
fork against *their* (empty) secrets store — they'd have to add their own
cookie. Your value stays in your repo only.

**The committed `data/*.csv` files are public** (the Pages repo is public).
That's fine — they're just price numbers, not credentials. The cookie itself
is never written to disk in the workflow runner, never printed to logs, and
never appears in any commit.

To add the secret:

1. Log in to <https://stooq.com> in any browser.
2. Open **DevTools → Network**, click any request to `stooq.com`, scroll to
   **Request Headers**, copy the *entire* value of the `Cookie:` header.
3. In this repo: **Settings → Secrets and variables → Actions → New repository
   secret**. Name: `STOOQ_COOKIE`. Value: the copied cookie. Save.

The Action reads it automatically every week via `${{ secrets.STOOQ_COOKIE }}`
and passes it to the script as an env variable. If the secret is absent the
script just sends no Cookie header and tries the anonymous endpoint.

**Do not** paste the cookie into `index.html`, `fetch_data.py`, or any commit.

## CoIQ NAV — one-time config

CoIQ (DE000A3C91C5) has no Stooq/Yahoo listing. The Action pulls its NAV from
ariva.de, which uses internal numeric IDs that need to be looked up once. See
the header comment in `scripts/fetch_data.py` — set `ARIVA_SECU` and
`ARIVA_BOERSE_ID` and commit. Until you do, the coIQ asset will fail to update
and the app will fall back to whatever's already in `data/coiq.csv` (or to
manual paste in the UI).

## Files

- `index.html` — the app (open in the browser; or serve via Pages).
- `scripts/fetch_data.py` — weekly fetch + validate + write CSV.
- `.github/workflows/update-data.yml` — schedule + commit.
- `data/*.csv` — committed price/NAV series, written by the Action.
- `data/manifest.json` — per-asset last date, row count, fetch timestamp.

## Manual override

The app still accepts manual CSV upload or paste per asset. Use this if you
want fresher numbers than the last weekly run, or if the auto-fetch failed for
a given asset.
