#!/usr/bin/env python3
"""
Weekly data fetcher for the Portfolio Backtest app.

Each asset is fetched INDEPENDENTLY. One failure must not block the others.
We write data/{asset}.csv with columns `date,value` and a data/manifest.json
that the app reads to show "data as of …" and staleness warnings.

Auth:
- Stooq: optional. The free CSV endpoint usually works unauthenticated. If
  STOOQ_COOKIE is set in the environment (populated by the workflow from the
  GitHub repo secret of the same name), we send it as a Cookie header.
- Ariva (coIQ NAV): no auth. Undocumented endpoint — wrapped in try/except
  and the existing data/coiq.csv is preserved on failure.

Exit code: 1 if any asset failed (so the run is marked red), but every other
asset has already been written and will still be committed by the workflow.
"""
import os
import sys
import csv
import io
import json
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

# ----------------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
MANIFEST_PATH = DATA_DIR / "manifest.json"

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
STOOQ_COOKIE = os.environ.get("STOOQ_COOKIE", "").strip()
STOOQ_APIKEY = os.environ.get("STOOQ_APIKEY", "").strip()
ARIVA_COOKIE = os.environ.get("ARIVA_COOKIE", "").strip()
TIMEOUT = 30

STOOQ_SOURCES = [
    # (output asset name, stooq symbol)
    ("momentum", "is3r.de"),   # iShares Edge MSCI World Momentum Factor (IE00BP3QZ825)
    ("gold",     "4gld.de"),   # EUWAX Gold II (DE000EWG2LD7) — Xetra-Gold proxy / fallback
    ("market",   "spyi.de"),   # SPDR MSCI ACWI IMI UCITS ETF (IE00B3YLTY66)
]

# --- Ariva config for the CoIQ Collective Intelligence Fund (DE000A3C91C5) ---
# Ariva uses internal numeric IDs (`secu` = security id, `boerse_id` = venue).
# Preferred: commit a `data/ariva.json` with {"secu": "...", "boerse_id": "..."}.
# Fallback: set the constants below directly. Either way, to find the IDs once:
# open https://www.ariva.de/ , search the ISIN, open "Historische Kurse",
# switch the board to "Fondsgesellschaft" (NAV), trigger the CSV "Download" —
# the request URL on the Network tab contains both `secu=` and `boerse_id=`.
ARIVA_SECU = ""
ARIVA_BOERSE_ID = ""
ARIVA_MIN_DATE = "2020-01-01"

def load_ariva_config():
    """Returns (secu, boerse_id) from data/ariva.json if present, else from constants."""
    cfg = DATA_DIR / "ariva.json"
    if cfg.exists():
        try:
            j = json.loads(cfg.read_text())
            s = str(j.get("secu", "") or "").strip()
            b = str(j.get("boerse_id", "") or "").strip()
            if s and b:
                return s, b
        except Exception as e:
            log(f"WARN: data/ariva.json unreadable: {e}")
    return ARIVA_SECU.strip(), ARIVA_BOERSE_ID.strip()

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
def log(*a):
    print("[fetch]", *a, file=sys.stderr, flush=True)

# ----------------------------------------------------------------------------
# Sources
# ----------------------------------------------------------------------------
def fetch_stooq(symbol):
    # Prefer API key (paid Stooq subscription) over session cookie when both
    # are present. Either is optional — anonymous works for small pulls.
    qs = f"s={symbol}&i=d"
    if STOOQ_APIKEY:
        qs = f"s={symbol}&i=d&apikey={STOOQ_APIKEY}"
    url = f"https://stooq.com/q/d/l/?{qs}"
    headers = {"User-Agent": UA, "Accept": "text/csv,*/*;q=0.5"}
    if STOOQ_COOKIE and not STOOQ_APIKEY:
        headers["Cookie"] = STOOQ_COOKIE
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    body = r.text.strip()
    head = body[:200].lower()
    if not body or "<html" in head or head.startswith("no data") or "exceeded" in head:
        raise RuntimeError(f"stooq returned no-data / HTML for {symbol} ({head[:80]!r})")
    rows = []
    reader = csv.DictReader(io.StringIO(body))
    for row in reader:
        d = (row.get("Date") or row.get("date") or "").strip()
        c = (row.get("Close") or row.get("close") or "").strip()
        if not d or not c:
            continue
        try:
            v = float(c)
        except ValueError:
            continue
        if v <= 0:
            continue
        rows.append((d, v))
    return rows


def fetch_ariva_coiq():
    secu, boerse_id = load_ariva_config()
    if not secu or not boerse_id:
        raise RuntimeError(
            "ariva secu/boerse_id not configured. Set them via the app's Setup "
            "panel (creates data/ariva.json), or edit scripts/fetch_data.py."
        )
    max_t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = (
        "https://www.ariva.de/quote/historic/historic.csv"
        f"?secu={secu}"
        f"&boerse_id={boerse_id}"
        "&clean_split=1&clean_payout=0&clean_bezug=1"
        f"&min_time={ARIVA_MIN_DATE}&max_time={max_t}"
        "&trenner=%3B&go=Download"
    )
    headers = {
        "User-Agent": UA,
        "Accept": "text/csv,*/*;q=0.5",
        "Referer": "https://www.ariva.de/",
    }
    if ARIVA_COOKIE:
        headers["Cookie"] = ARIVA_COOKIE
    log(f"ariva GET {url}")
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    log(f"ariva HTTP {r.status_code} · content-type={r.headers.get('content-type', '?')} · {len(r.text)} bytes")
    r.raise_for_status()
    body = r.text.strip()
    snippet = body[:300].replace("\n", " ").replace("\r", " ")
    if "login" in body[:500].lower() or "anmelden" in body[:500].lower():
        raise RuntimeError(
            "ariva.de requires login. Create a free account at https://www.ariva.de/registrierung , "
            "copy your session cookie (DevTools → Network → Request Headers → Cookie), "
            "and add it as the ARIVA_COOKIE repo secret. First 200 chars: " + snippet[:200]
        )
    if not body or "<html" in body[:200].lower() or "<!doctype html" in body[:200].lower():
        raise RuntimeError(f"ariva returned HTML — first 300 chars: {snippet!r}")
    reader = csv.DictReader(io.StringIO(body), delimiter=";")
    rows = []
    for row in reader:
        d = (row.get("Datum") or row.get("Date") or row.get("datum") or "").strip()
        c = (row.get("Schluss") or row.get("Close") or row.get("schluss") or "").strip()
        if not d or not c:
            continue
        if "." in d and "-" not in d:
            try:
                d = datetime.strptime(d, "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                continue
        c = c.replace(".", "").replace(",", ".")
        try:
            v = float(c)
        except ValueError:
            continue
        if v <= 0:
            continue
        rows.append((d, v))
    return rows

# ----------------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------------
def validate(rows, name):
    if not rows:
        raise RuntimeError(f"{name}: empty")
    if len(rows) < 100:
        raise RuntimeError(f"{name}: only {len(rows)} rows (need >= 100)")

    parsed = []
    for d, v in rows:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            raise RuntimeError(f"{name}: unparseable date {d!r}")
        parsed.append((dt, d, v))
    parsed.sort(key=lambda x: x[0])

    prev_dt = None
    for dt, d, _ in parsed:
        if prev_dt is not None and dt <= prev_dt:
            raise RuntimeError(f"{name}: dates not strictly increasing at {d}")
        prev_dt = dt

    big_moves = []
    for i in range(1, len(parsed)):
        a = parsed[i - 1][2]
        b = parsed[i][2]
        if a <= 0:
            continue
        chg = abs(b / a - 1)
        if chg > 0.60:
            big_moves.append((parsed[i][1], chg))
    if big_moves:
        log(f"WARN {name}: {len(big_moves)} >60% single-day moves; first {big_moves[0]}")

    last_dt = parsed[-1][0]
    days_old = (datetime.now(timezone.utc).replace(tzinfo=None) - last_dt).days
    if days_old > 10:
        log(f"WARN {name}: last data is {days_old} days old ({parsed[-1][1]})")

    cleaned = [(d, v) for _, d, v in parsed]
    return cleaned, big_moves, days_old

# ----------------------------------------------------------------------------
# IO
# ----------------------------------------------------------------------------
def write_csv(path, rows):
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="") as f:
        f.write("date,value\n")
        for d, v in rows:
            f.write(f"{d},{v}\n")
    tmp.replace(path)


def load_manifest():
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_manifest(m):
    MANIFEST_PATH.write_text(json.dumps(m, indent=2, sort_keys=True))

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    manifest = load_manifest()
    if "assets" not in manifest:
        manifest["assets"] = {}
    manifest["fetchedAt"] = datetime.now(timezone.utc).isoformat()

    errors = {}

    for name, sym in STOOQ_SOURCES:
        try:
            log(f"fetching stooq {sym} -> {name}")
            rows = fetch_stooq(sym)
            rows, bad, days_old = validate(rows, name)
            write_csv(DATA_DIR / f"{name}.csv", rows)
            manifest["assets"][name] = {
                "source": f"stooq:{sym}",
                "lastDate": rows[-1][0],
                "rows": len(rows),
                "fetchedAt": manifest["fetchedAt"],
                "warnings": (
                    ([f"{len(bad)} single-day moves > 60%"] if bad else [])
                    + ([f"{days_old} days stale"] if days_old > 10 else [])
                ),
            }
            log(f"OK {name}: {len(rows)} rows through {rows[-1][0]}")
        except Exception as e:
            errors[name] = str(e)
            log(f"FAIL {name}: {e}")
            traceback.print_exc(file=sys.stderr)
            # Do NOT overwrite existing data/{name}.csv on failure.

    try:
        log("fetching ariva coIQ")
        rows = fetch_ariva_coiq()
        rows, bad, days_old = validate(rows, "coIQ")
        write_csv(DATA_DIR / "coiq.csv", rows)
        manifest["assets"]["coiq"] = {
            "source": "ariva:DE000A3C91C5",
            "lastDate": rows[-1][0],
            "rows": len(rows),
            "fetchedAt": manifest["fetchedAt"],
            "warnings": (
                ([f"{len(bad)} single-day moves > 60%"] if bad else [])
                + ([f"{days_old} days stale"] if days_old > 10 else [])
            ),
        }
        log(f"OK coIQ: {len(rows)} rows through {rows[-1][0]}")
    except Exception as e:
        errors["coiq"] = str(e)
        log(f"FAIL coIQ (keeping previous data/coiq.csv if present): {e}")
        traceback.print_exc(file=sys.stderr)

    save_manifest(manifest)

    if errors:
        log(f"SUMMARY failures: {errors}")
        sys.exit(1)
    log("SUMMARY all OK")


if __name__ == "__main__":
    main()
