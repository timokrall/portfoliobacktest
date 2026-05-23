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


def sanitize_header(name, value):
    """Headers must be latin-1 encodable. If the user pasted a cookie value
    copied from DevTools that contained the literal ellipsis character
    (U+2026 '…'), that means the value was visually truncated in the UI —
    the secret will not authenticate. We strip non-latin-1 chars so the
    request still goes out, and log a loud warning so the user knows."""
    if not value:
        return value
    try:
        value.encode("latin-1")
        return value
    except UnicodeEncodeError:
        log(f"WARN {name}: contains non-ASCII chars (e.g. '\u2026'). "
            f"DevTools truncated the cookie value when you copied it. "
            f"Re-copy from Application \u2192 Storage \u2192 Cookies (not the visible Header text). "
            f"Sending sanitized value; auth may fail.")
        return value.encode("latin-1", errors="ignore").decode("latin-1")

STOOQ_SOURCES = [
    # (output asset name, stooq symbol)
    ("momentum", "is3r.de"),   # iShares Edge MSCI World Momentum Factor (IE00BP3QZ825)
    ("value",    "is3s.de"),   # iShares Edge MSCI World Value Factor (IE00BP3QZB59)
    ("quality",  "is3q.de"),   # iShares Edge MSCI World Quality Factor (IE00BP3QZ601)
    ("minvol",   "iqq0.de"),   # iShares Edge MSCI World Min Volatility (IE00B8FHGS14)
    ("world",    "webn.de"),   # Amundi MSCI World UCITS ETF Acc (LU1681043599)
    ("market",   "spyi.de"),   # SPDR MSCI ACWI IMI UCITS ETF (IE00B3YLTY66)
    ("signal",   "vt.us"),     # Vanguard Total World VT — used ONLY as the 200-SMA signal
    # Best-effort: Amundi 2x leveraged UCITS ETFs. Confirmed on Stooq as
    # the Paris listings (LQQ.FR, LWLD.FR). Manual CSV paste remains the
    # bulletproof fallback if Stooq ever drops them.
    ("lev2x_ndx", "l8i7.de"),  # Amundi Nasdaq-100 Daily 2x Leveraged (FR0010342592, WKN A0LC12) — Xetra listing
    ("lev2x_wld", "lwld.fr"),  # Amundi MSCI World 2x Leveraged (FR0014010HV4, WKN ETF888)
]
STOOQ_GOLD_SYMBOL = "4gld.de"  # Xetra-Gold DE000A0S9GB0 — used as fallback if no ariva-gold.json

# yfinance mapping. Each entry: asset_name -> [list of Yahoo tickers tried in
# order]. Empty list = no yfinance fallback (must come from Stooq or manual
# upload). yfinance is tried AFTER Stooq fails — it sometimes rate-limits and
# is more brittle than Stooq's CSV API, so it's not the primary.
YFINANCE_SYMBOLS = {
    "momentum":  ["IS3R.DE", "IWMO.L"],
    "value":     ["IS3S.DE", "IWVL.L"],
    "quality":   ["IS3Q.DE", "IWQU.L"],
    "minvol":    ["IQQ0.DE", "MVOL.L"],
    "world":     ["WEBN.DE", "MWRD.L"],
    "market":    ["SPYI.DE", "IMIE.DE", "SPYI.L"],
    "gold":      ["4GLD.DE", "4GLD.SG"],
    "signal":    ["VT", "ACWI"],
    "lev3x":     ["3TWL.L"],                  # Leverage Shares 3x Total World — best-effort
    "lev2x_ndx": ["LQQ.PA", "LVNAS.DE"],     # Amundi Nasdaq-100 Daily 2x (A0LC12)
    "lev2x_wld": ["WLDL.PA", "WLDL.DE"],     # Amundi MSCI World 2x (ETF888) — guesswork; ticker unstable
    "bitcoin":   ["BTC-EUR"],                # Bitcoin in EUR (yfinance native)
}
# Leverage Shares 3x Long Total World ETP (XS2399364822, WKN A3GWC0). Not on
# Stooq under a stable symbol — manual CSV upload via the app is the supported
# path. We still register the asset in the manifest so the UI shows status.
LEV3X_ASSET_NAME = "lev3x"

# German CPI via FRED (OECD source). No auth needed.
# DEUCPIALLMINMEI = Consumer Price Index: All Items for Germany, monthly, 2015=100.
FRED_CPI_ID = "DEUCPIALLMINMEI"

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

def load_ariva_config(filename="ariva.json"):
    """Returns (secu, boerse_id) from data/<filename> if present."""
    cfg = DATA_DIR / filename
    if cfg.exists():
        try:
            j = json.loads(cfg.read_text())
            s = str(j.get("secu", "") or "").strip()
            b = str(j.get("boerse_id", "") or "").strip()
            if s and b:
                return s, b
        except Exception as e:
            log(f"WARN: data/{filename} unreadable: {e}")
    if filename == "ariva.json":
        return ARIVA_SECU.strip(), ARIVA_BOERSE_ID.strip()
    return "", ""

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
        headers["Cookie"] = sanitize_header("STOOQ_COOKIE", STOOQ_COOKIE)
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


def fetch_yfinance(symbol):
    """Fetch daily OHLC from Yahoo Finance via the yfinance library.
    Returns [(date_str, close_float)] sorted ascending. Empty on failure.
    Tries adjusted close first, falls back to plain close. yfinance is
    imported lazily so a missing install only breaks this code path."""
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance not installed (pip install yfinance)")
    log(f"yfinance GET {symbol}")
    # period=max grabs everything available. auto_adjust=True returns
    # split/dividend-adjusted close in the "Close" column (which is what we
    # want for total-return-ish series).
    try:
        df = yf.download(
            symbol,
            period="max",
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=False,
            ignore_tz=True,
        )
    except Exception as e:
        raise RuntimeError(f"yfinance download failed: {e}")
    if df is None or df.empty:
        raise RuntimeError(f"yfinance returned empty for {symbol}")
    # When auto_adjust=True, "Close" is the adjusted column.
    if "Close" not in df.columns:
        raise RuntimeError(f"yfinance: no Close column for {symbol} (cols={list(df.columns)})")
    rows = []
    for idx, row in df.iterrows():
        try:
            d = idx.strftime("%Y-%m-%d")
        except AttributeError:
            d = str(idx)[:10]
        v = row["Close"]
        # df might be a multi-index when a single ticker is downloaded.
        if hasattr(v, "item"):
            try:
                v = v.item()
            except Exception:
                pass
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        if not (v > 0):
            continue
        rows.append((d, v))
    if not rows:
        raise RuntimeError(f"yfinance: no parseable rows for {symbol}")
    log(f"yfinance OK {symbol}: {len(rows)} rows through {rows[-1][0]}")
    return rows


def fetch_with_fallback(asset_name, stooq_sym):
    """Try Stooq first (fast, reliable, anonymous-friendly). If it fails AND
    YFINANCE_SYMBOLS has tickers for this asset, try each yfinance ticker in
    turn. Raises the LAST error if every source fails.
    Returns (rows, source_label)."""
    last_err = None
    if stooq_sym:
        try:
            rows = fetch_stooq(stooq_sym)
            return rows, f"stooq:{stooq_sym}"
        except Exception as e:
            log(f"stooq {stooq_sym} failed: {e}")
            last_err = e
    for ysym in YFINANCE_SYMBOLS.get(asset_name, []):
        try:
            rows = fetch_yfinance(ysym)
            return rows, f"yfinance:{ysym}"
        except Exception as e:
            log(f"yfinance {ysym} failed: {e}")
            last_err = e
    raise last_err or RuntimeError(f"no source for {asset_name}")


def fetch_fred_cpi():
    """German CPI from FRED (OECD-sourced, monthly). Returns [(date, value)].
    FRED occasionally times out; retry a few times before giving up."""
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={FRED_CPI_ID}"
    headers = {"User-Agent": UA, "Accept": "text/csv,*/*;q=0.5"}
    last = None
    for attempt in range(4):
        try:
            log(f"fred GET {url} (attempt {attempt + 1})")
            r = requests.get(url, headers=headers, timeout=60)
            log(f"fred HTTP {r.status_code} · {len(r.text)} bytes")
            r.raise_for_status()
            body = r.text.strip()
            if "<html" in body[:200].lower():
                raise RuntimeError("FRED returned HTML — id may be wrong or service down")
            reader = csv.DictReader(io.StringIO(body))
            rows = []
            for row in reader:
                d = (row.get("DATE") or row.get("observation_date") or "").strip()
                val_keys = [k for k in row.keys() if k and k.upper() not in ("DATE", "OBSERVATION_DATE")]
                if not val_keys:
                    continue
                v = (row.get(val_keys[0]) or "").strip()
                if not v or v == ".":
                    continue
                try:
                    vf = float(v)
                except ValueError:
                    continue
                if vf <= 0:
                    continue
                rows.append((d, vf))
            if rows:
                return rows
            raise RuntimeError("FRED returned no parseable rows")
        except Exception as e:
            last = e
            log(f"fred attempt {attempt + 1} failed: {e}")
            import time as _t
            _t.sleep(2 + attempt * 3)
    raise last or RuntimeError("FRED unreachable")


def fetch_destatis_cpi():
    """Fallback CPI from Destatis (German Federal Statistical Office) via their
    public CSV mirror. Less reliable schema, used only if FRED is down."""
    # Eurostat 'prc_hicp_midx' for DE-HICP, monthly, all-items index. Public API.
    url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hicp_midx/M.I15.CP00.DE?format=SDMX-CSV"
    headers = {"User-Agent": UA, "Accept": "text/csv,*/*;q=0.5"}
    log(f"eurostat GET {url}")
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    body = r.text.strip()
    if "<html" in body[:200].lower():
        raise RuntimeError("eurostat returned HTML")
    reader = csv.DictReader(io.StringIO(body))
    rows = []
    for row in reader:
        d = (row.get("TIME_PERIOD") or row.get("time") or "").strip()
        v = (row.get("OBS_VALUE") or row.get("value") or "").strip()
        if not d or not v or v == ":":
            continue
        # Eurostat uses YYYY-MM; turn into YYYY-MM-01 for downstream consistency
        if len(d) == 7 and d[4] == "-":
            d = d + "-01"
        try:
            vf = float(v)
        except ValueError:
            continue
        if vf <= 0:
            continue
        rows.append((d, vf))
    if not rows:
        raise RuntimeError("eurostat returned no parseable rows")
    return rows


def fetch_ariva_coiq():
    secu, boerse_id = load_ariva_config("ariva.json")
    if not secu or not boerse_id:
        raise RuntimeError(
            "ariva secu/boerse_id not configured. Set them via the app's Setup "
            "panel (creates data/ariva.json), or edit scripts/fetch_data.py."
        )
    return _fetch_ariva(secu, boerse_id, "coIQ")


def fetch_ariva_gold():
    secu, boerse_id = load_ariva_config("ariva-gold.json")
    if not secu or not boerse_id:
        return None  # not configured — caller will fall back to stooq
    return _fetch_ariva(secu, boerse_id, "gold")


def _fetch_ariva(secu, boerse_id, label):
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
        headers["Cookie"] = sanitize_header("ARIVA_COOKIE", ARIVA_COOKIE)
    log(f"ariva({label}) GET {url}")
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    log(f"ariva({label}) HTTP {r.status_code} · content-type={r.headers.get('content-type', '?')} · {len(r.text)} bytes")
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
    reader = csv.reader(io.StringIO(body), delimiter=";")
    all_rows = list(reader)
    if not all_rows:
        raise RuntimeError("ariva: empty CSV body")
    header = [h.strip().lower() for h in all_rows[0]]
    log(f"ariva CSV header: {header}")
    log(f"ariva CSV first data row: {all_rows[1] if len(all_rows) > 1 else 'NONE'}")

    # Find date column
    date_idx = None
    for i, h in enumerate(header):
        if h in ("datum", "date") or "datum" in h or "date" in h:
            date_idx = i; break
    # Find close/value column. Ariva typically: "Erster","Hoch","Tief","Schluss","Stuecke","Volumen"
    close_idx = None
    for cand in ("schluss", "close", "schlusskurs", "kurs", "last", "wert", "nav"):
        if cand in header:
            close_idx = header.index(cand); break
    if close_idx is None:
        # Fund NAVs sometimes only have one numeric column; take the last numeric-looking column
        for i in range(len(header) - 1, 0, -1):
            try:
                if len(all_rows) > 1:
                    v = all_rows[1][i].replace(".", "").replace(",", ".")
                    float(v)
                    close_idx = i
                    log(f"ariva: guessing close column at index {i} ('{header[i]}')")
                    break
            except (ValueError, IndexError):
                continue
    if date_idx is None or close_idx is None:
        raise RuntimeError(f"ariva: cannot identify date/close columns in header {header}")

    rows = []
    for r in all_rows[1:]:
        if len(r) <= max(date_idx, close_idx):
            continue
        d = r[date_idx].strip()
        c = r[close_idx].strip()
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
    log(f"ariva({label}): parsed {len(rows)} rows from {len(all_rows) - 1} CSV lines")
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
            log(f"fetching {name} (stooq:{sym} primary, yfinance fallback)")
            rows, source = fetch_with_fallback(name, sym)
            rows, bad, days_old = validate(rows, name)
            write_csv(DATA_DIR / f"{name}.csv", rows)
            manifest["assets"][name] = {
                "source": source,
                "lastDate": rows[-1][0],
                "rows": len(rows),
                "fetchedAt": manifest["fetchedAt"],
                "warnings": (
                    ([f"{len(bad)} single-day moves > 60%"] if bad else [])
                    + ([f"{days_old} days stale"] if days_old > 10 else [])
                ),
            }
            log(f"OK {name}: {len(rows)} rows through {rows[-1][0]} (via {source})")
        except Exception as e:
            errors[name] = str(e)
            log(f"FAIL {name}: {e}")
            traceback.print_exc(file=sys.stderr)
            # Do NOT overwrite existing data/{name}.csv on failure.

    # --- lev3x: no Stooq symbol; try yfinance only ---
    try:
        log("fetching lev3x (yfinance only)")
        rows, source = fetch_with_fallback("lev3x", None)
        rows, bad, days_old = validate(rows, "lev3x")
        write_csv(DATA_DIR / "lev3x.csv", rows)
        manifest["assets"]["lev3x"] = {
            "source": source,
            "lastDate": rows[-1][0],
            "rows": len(rows),
            "fetchedAt": manifest["fetchedAt"],
            "warnings": (
                ([f"{len(bad)} single-day moves > 60%"] if bad else [])
                + ([f"{days_old} days stale"] if days_old > 10 else [])
            ),
        }
        log(f"OK lev3x: {len(rows)} rows through {rows[-1][0]} (via {source})")
    except Exception as e:
        errors["lev3x"] = str(e)
        log(f"FAIL lev3x (manual CSV upload via the app is the supported path): {e}")

    # --- bitcoin: yfinance BTC-EUR; Stooq has BTCUSD but not stable EUR ---
    try:
        log("fetching bitcoin (yfinance BTC-EUR)")
        rows, source = fetch_with_fallback("bitcoin", None)
        # Bitcoin moves are routinely > 60% intraday during crashes/spikes;
        # we relax the spike-warning threshold by *not* failing on big moves.
        # The validator already only logs (doesn't raise) for big moves.
        rows, bad, days_old = validate(rows, "bitcoin")
        write_csv(DATA_DIR / "bitcoin.csv", rows)
        manifest["assets"]["bitcoin"] = {
            "source": source,
            "lastDate": rows[-1][0],
            "rows": len(rows),
            "fetchedAt": manifest["fetchedAt"],
            "warnings": (
                ([f"{len(bad)} single-day moves > 60% (normal for BTC)"] if bad else [])
                + ([f"{days_old} days stale"] if days_old > 10 else [])
            ),
        }
        log(f"OK bitcoin: {len(rows)} rows through {rows[-1][0]} (via {source})")
    except Exception as e:
        errors["bitcoin"] = str(e)
        log(f"FAIL bitcoin: {e}")

    # --- gold: ariva (full history) if configured, else Stooq + yfinance fallback ---
    try:
        log("fetching gold")
        rows = None
        source = None
        ariva_rows = fetch_ariva_gold()
        if ariva_rows is not None:
            rows = ariva_rows
            source = "ariva:DE000A0S9GB0"
        else:
            log("gold: data/ariva-gold.json not configured, trying stooq + yfinance fallback")
            rows, source = fetch_with_fallback("gold", STOOQ_GOLD_SYMBOL)
        rows, bad, days_old = validate(rows, "gold")
        write_csv(DATA_DIR / "gold.csv", rows)
        manifest["assets"]["gold"] = {
            "source": source,
            "lastDate": rows[-1][0],
            "rows": len(rows),
            "fetchedAt": manifest["fetchedAt"],
            "warnings": (
                ([f"{len(bad)} single-day moves > 60%"] if bad else [])
                + ([f"{days_old} days stale"] if days_old > 10 else [])
            ),
        }
        log(f"OK gold: {len(rows)} rows through {rows[-1][0]} (via {source})")
    except Exception as e:
        errors["gold"] = str(e)
        log(f"FAIL gold (keeping previous data/gold.csv if present): {e}")
        traceback.print_exc(file=sys.stderr)

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

    try:
        log("fetching German CPI")
        rows = None; source = None
        try:
            rows = fetch_fred_cpi()
            source = f"fred:{FRED_CPI_ID}"
        except Exception as e_fred:
            log(f"FRED failed: {e_fred} \u2014 trying Eurostat HICP fallback")
            rows = fetch_destatis_cpi()
            source = "eurostat:prc_hicp_midx/DE"
        rows, bad, days_old = validate(rows, "cpi")
        write_csv(DATA_DIR / "cpi.csv", rows)
        manifest["assets"]["cpi"] = {
            "source": source,
            "lastDate": rows[-1][0],
            "rows": len(rows),
            "fetchedAt": manifest["fetchedAt"],
            "warnings": ([f"{days_old} days stale"] if days_old > 90 else []),
        }
        log(f"OK cpi: {len(rows)} rows through {rows[-1][0]} (via {source})")
    except Exception as e:
        errors["cpi"] = str(e)
        log(f"FAIL cpi (keeping previous data/cpi.csv if present): {e}")
        traceback.print_exc(file=sys.stderr)

    save_manifest(manifest)

    if errors:
        log(f"SUMMARY failures: {errors}")
        sys.exit(1)
    log("SUMMARY all OK")


if __name__ == "__main__":
    main()
