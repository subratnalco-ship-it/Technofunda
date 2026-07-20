"""
Daily Stock Recommendation Agent → Google Sheets
-------------------------------------------------
Combines:
  1. Technical signals (RSI, MA crossover, volume spike, 200-DMA gate,
     20-day high breakout, 2x delivery-volume spike vs 20-day average,
     average daily turnover liquidity floor, turnover spike)
  2. Fundamental analysis (P/E, ROE, D/E, margins, revenue growth)
  3. DCF intrinsic value model (skipped for banks/financials, averaged over
     multiple years, with guardrails on the discount/terminal-growth spread)
  4. News headline sentiment (VADER, local — no external API cost)
  5. Analyst consensus + broker recommendation trend (Yahoo aggregated,
     treated as "soft" signals since NSE coverage is sparse)
  6. Sector rotation (RRG-style quadrant vs Nifty 500 benchmark)

Output: writes a dated tab inside a Google Sheet every weekday, after
        market close. Old tabs are kept so you have a rolling history.

SETUP  (full step-by-step guide is in the README — read that first)
-----
  pip install yfinance pandas requests vaderSentiment gspread google-auth tzdata

Environment variables required:
  GOOGLE_CREDENTIALS_JSON   <- contents of your service-account JSON key file
  GOOGLE_SHEET_ID           <- the long ID from your Google Sheet URL
                               (no fallback default anymore — must be set)

RUNNING
-------
  python3 daily_report_sheets.py --now             # run once, write to Sheets
  python3 daily_report_sheets.py --now --dry-run   # run once, print only,
                                                    # no Sheets writes, shows
                                                    # WHY each stock passed/failed
  python3 daily_report_sheets.py --schedule         # persistent after-market
                                                    # weekday loop (see
                                                    # SCHEDULED_HOUR/MINUTE)

CHANGE LOG vs previous version
-------------------------------
- Ticker() and .info fetched once per symbol and reused everywhere
  (was being re-fetched 3+ times per stock).
- Hard hard filters that likely reject everything (analyst rating,
  broker buy ratio) now degrade gracefully when data is missing,
  and every rejection is logged with a reason instead of silently
  disappearing.
- DCF: averages last up-to-3 years of FCF instead of a single year,
  skipped entirely for Financial Services (bank DCF via FCF is not
  meaningful), guards against r <= terminal_growth.
- Sheet cell coloring batched into one API call instead of one call
  per row (avoids hitting Sheets API per-minute write quota as
  TOP_N grows).
- GOOGLE_SHEET_ID has no silent fallback — missing env var now
  raises clearly instead of writing to a hardcoded spreadsheet.
- --dry-run flag: run the full pipeline, print ranked + rejected
  stocks with reasons, skip all Google Sheets calls.
- Scheduler uses a >= minute check + last_date guard so a missed
  exact-minute tick (sleep/throttle) doesn't skip the whole day.
- run_once() wraps the whole pipeline in try/except and writes a
  FAILED row to the Summary tab on error instead of failing silently.

CHANGE LOG (this revision) — fixes "0 stocks returned" problem
----------------------------------------------------------------
The previous version could legitimately return ZERO stocks if no
symbol satisfied every hard+soft gate simultaneously (e.g. during a
choppy market where several names sit just under their 200-DMA).
That's not a bug, but it's not useful for a "give me today's picks"
tool either. This revision changes the philosophy:

- STRICT_MODE (config flag, default False): when False, filters no
  longer hard-drop stocks. Every stock that has usable technical data
  is scored and ranked; the sheet gets the TOP_N best available
  candidates regardless of whether they'd have passed the old gates.
  Two new columns ("Meets All Filters", "Filter Notes") tell you
  which gates each one did/didn't clear, so you keep the filter
  information without losing the recommendation itself. Set
  STRICT_MODE = True to restore the old hard-drop behavior.
- Added retry-with-backoff around every yfinance network call
  (.info, .history, .cashflow, .news, .recommendations). Transient
  "Too Many Requests" / connection errors are a common reason
  yfinance silently returns empty data, which previously looked
  identical to "this stock failed the filters."
- Added a run-level diagnostic summary printed every run (not just
  --dry-run): how many symbols had usable price history, how many
  had usable fundamental data, etc. If you're getting 0 stocks because
  of a network/data problem rather than a filter problem, this tells
  you immediately instead of you having to guess.

CHANGE LOG (this revision) — 20-day high breakout signal
----------------------------------------------------------------
- analyze_technicals() checks whether today's close crosses above the
  highest close of the prior 20 trading days (a classic breakout /
  buying trigger). Exposed as "crosses_20d_high" and "high_20d_prev"
  on the technical result dict.
- New "20D High Breakout" column added to the Google Sheet output,
  right after the "Above 200-DMA" column.
- get_or_create_tab() now syncs the header row on every reused
  worksheet (not just newly created ones), so older tabs (e.g.
  "History") automatically pick up newly added columns like this one
  instead of silently appending them unlabeled.

CHANGE LOG (this revision) — 20-day high breakout is now a hard gate
----------------------------------------------------------------------
- REQUIRE_20D_HIGH_BREAKOUT (default True) added alongside
  REQUIRE_ABOVE_200DMA. Like that flag, it is a "hard" gate that is
  only actually enforced when STRICT_MODE = True — with STRICT_MODE
  False (the default) it still just shows up in "Meets All Filters" /
  "Filter Notes" as informational, same as every other hard gate.
- crosses_20d_high is a pass/fail buying condition now, not a score
  contributor — it was removed from the combined_score sum, mirroring
  how above_200dma was already a gate rather than a scoring input.
  Every other existing condition (fundamental score gate, analyst/
  sentiment/broker soft gates, sector rotation adjustment, etc.) is
  unchanged.

CHANGE LOG (this revision) — watchlist switched to Nifty LargeMidcap 250
----------------------------------------------------------------------
- WATCHLIST (a hardcoded 20-symbol list) is replaced by get_watchlist(),
  which downloads NSE Indices' official Nifty LargeMidcap 250 constituent
  CSV at the start of every run (that index = all Nifty 100 + Nifty
  Midcap 150 stocks, ~250 names, rebalanced semi-annually). This keeps
  the scanned universe in sync with the real index automatically instead
  of a list that silently goes stale as the index is reconstituted.
- FALLBACK_WATCHLIST keeps the original 20-symbol list as a safety net —
  used only if every live download attempt fails (site down, blocked,
  CSV format changed, parsed result looks truncated), so the script can
  still run and produce a report rather than erroring out.
- build_report() now returns the watchlist it actually used as a third
  value, so run_once()'s "Stocks Scanned" figure in the Summary tab
  reflects the real (possibly live-fetched) universe size rather than a
  stale constant.
- TOP_N (still 10) is unchanged, so the sheet still surfaces the best 10
  candidates — just now selected out of ~250 stocks instead of 20.

CHANGE LOG (this revision) — 2x delivery-volume spike signal
----------------------------------------------------------------------
- New signal: a stock qualifies when today's NSE *delivery* volume
  (DELIV_QTY — shares actually taken delivery of, not squared off
  intraday) is >= DELIVERY_SPIKE_MULTIPLIER (default 2x) the average
  delivery volume of the prior DELIVERY_LOOKBACK_TRADING_DAYS (default
  20) sessions. This is a different, and generally stronger, signal
  than the existing plain-volume spike (which uses total traded
  volume from yfinance and already feeds the technical score) — a
  delivery spike specifically flags real accumulation/distribution
  rather than intraday churn.
- yfinance does not expose delivery quantity, so this is sourced from
  NSE's own daily "full bhavcopy" file (sec_bhavdata_full_DDMMYYYY.csv),
  which is published once per day for the whole market and carries a
  DELIV_QTY column per symbol. build_delivery_cache() downloads the
  last ~20 trading days of these files ONCE per run (not once per
  symbol) and builds an in-memory per-symbol delivery-quantity history;
  delivery_volume_signal() just looks a symbol up in that cache. Days
  that fail to download (holiday, not yet published, transient network
  issue) are skipped rather than treated as zero volume.
- New "Delivery Volume", "Avg Delivery Vol 20D" and "Delivery Vol Spike
  (2x)" columns added to the Google Sheet, right after the 20-day-high
  breakout columns.
- REQUIRE_DELIVERY_VOLUME_SPIKE (default True) is wired in as a "hard"
  gate at the same tier as REQUIRE_ABOVE_200DMA / REQUIRE_20D_HIGH_
  BREAKOUT — i.e. it is informational (shows up in "Meets All Filters"/
  "Filter Notes") when STRICT_MODE=False (the default), and only
  actually drops a stock when STRICT_MODE=True. Every other existing
  condition (fundamental gate, analyst/sentiment/broker soft gates,
  sector rotation adjustment, combined_score formula, etc.) is
  unchanged.

CHANGE LOG (this revision) — scheduled run moved to after market close
----------------------------------------------------------------------
- The old default schedule (8:00 AM IST) ran BEFORE the market opens
  (NSE trading hours are 09:15-15:30 IST), which meant every run used
  the PREVIOUS trading day's closing prices/technicals, AND the
  delivery-volume signal was working off whatever bhavcopy history
  happened to already be cached from earlier days — never that day's
  own session. Both the price-based signals (RSI, MAs, 20-day-high
  breakout, plain volume spike) and the delivery-volume spike signal
  are only meaningful once a session's data is actually final.
- SCHEDULED_HOUR/SCHEDULED_MINUTE now default to 18:30 IST (6:30 PM),
  safely after NSE's 15:30 IST close AND after NSE typically publishes
  the day's full bhavcopy (DELIV_QTY) file, which is what feeds the
  delivery-volume-spike signal. This is a config value, not a hardcoded
  time — see SCHEDULED_HOUR / SCHEDULED_MINUTE below if your bhavcopy
  publish times differ or you want more buffer.
- get_watchlist() (the live Nifty LargeMidcap 250 constituent download)
  and build_delivery_cache() are unchanged in behavior — they already
  re-fetch fresh data on every run_once() call — so simply moving WHEN
  run_once() fires each day automatically updates both the stock
  universe and the delivery-volume history to reflect that day's
  after-close data, with no other code changes required.
- run_scheduler()'s startup log line now prints the actual configured
  time instead of a hardcoded "8 AM IST" string, so it can't drift out
  of sync with SCHEDULED_HOUR/SCHEDULED_MINUTE again.

CHANGE LOG (this revision) — turnover (₹ value) metrics added
----------------------------------------------------------------------
- New signal #1: average daily TURNOVER (₹ value = close * volume,
  rolled over TURNOVER_LOOKBACK_TRADING_DAYS sessions), computed in
  analyze_technicals() from the same `hist` DataFrame already fetched
  (no extra network calls). Exposed as "avg_turnover_cr" (₹ crore) and
  "turnover_spike" (today's turnover >= 2x the 20D average) on the
  technical result dict. This is purely a LIQUIDITY filter — can you
  actually enter/exit a swing position without moving the price? —
  distinct from the delivery-volume signal, which measures conviction/
  accumulation rather than tradability.
- REQUIRE_MIN_TURNOVER (default True) / MIN_AVG_TURNOVER_CR (default
  ₹5 crore/day) wired into evaluate_filters() as a "hard" gate at the
  same tier as REQUIRE_ABOVE_200DMA etc. — informational only unless
  STRICT_MODE=True, consistent with every other hard gate in this
  script. Missing turnover data (couldn't compute) is itself treated
  as a hard-gate failure rather than silently passing, since "unknown
  liquidity" should not be treated as "liquid enough."
- New signal #2: DELIVERY TURNOVER — the existing delivery-volume
  signal (DELIV_QTY, share count) converted to ₹ terms by multiplying
  by last_close, so a 2x delivery-volume spike on a low-priced stock
  and a high-priced stock become directly comparable. Implemented by
  passing last_close into delivery_volume_signal(), which now also
  returns "delivery_turnover_cr" and "avg_delivery_turnover_20d_cr".
  This does NOT change delivery_volume_spike's pass/fail logic (still
  share-count based, matching NSE's own convention) — it's an
  additional, better-normalized ₹ figure surfaced alongside it.
- New Sheet columns: "Delivery Turnover ₹cr", "Avg Delivery Turnover
  20D ₹cr", "Avg Turnover ₹cr (20D)", "Turnover Spike (2x)" — added
  after the existing delivery-volume columns.
- Neither turnover metric feeds combined_score directly (same
  "gate/informational, not a scoring input" treatment already used for
  above_200dma, crosses_20d_high, and delivery_volume_spike).

CHANGE LOG (this revision) — volume z-score filter (z > 2)
----------------------------------------------------------------------
- New signal: a statistical (standard-deviation based) volume spike
  check, distinct from the existing fixed-multiplier "volume_spike"
  (1.3x of the 20D average) already feeding combined_score. Computed
  in analyze_technicals() from the same `hist` DataFrame (no extra
  network calls): z = (today's volume - rolling mean) / rolling std,
  over VOLUME_ZSCORE_LOOKBACK (default 20) sessions. A z-score > 2
  means today's volume is more than 2 standard deviations above its
  recent average — a statistically unusual print, not just "somewhat
  higher than normal." Exposed as "volume_zscore" and
  "volume_zscore_spike" on the technical result dict.
- REQUIRE_VOLUME_ZSCORE_SPIKE (default True) / VOLUME_ZSCORE_THRESHOLD
  (default 2.0) wired into evaluate_filters() as a "hard" gate at the
  same tier as REQUIRE_ABOVE_200DMA / REQUIRE_20D_HIGH_BREAKOUT /
  REQUIRE_DELIVERY_VOLUME_SPIKE / REQUIRE_MIN_TURNOVER — informational
  only (shows up in "Meets All Filters" / "Filter Notes") unless
  STRICT_MODE=True, consistent with every other hard gate in this
  script. Missing/undefined z-score (not enough history to compute a
  rolling std, or std is 0/NaN) is treated as a hard-gate failure
  rather than silently passing, same pattern as REQUIRE_MIN_TURNOVER.
- Does NOT feed combined_score directly — same "gate/informational,
  not a scoring input" treatment already used for above_200dma,
  crosses_20d_high, delivery_volume_spike, and turnover_spike.
- New Sheet columns: "Volume Z-Score", "Volume Z-Score Spike (>2)" —
  added after the existing "Turnover Spike (2x)" column.
"""


import os, sys, time, json, io, argparse, traceback
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import gspread
from google.oauth2.service_account import Credentials

_sentiment_analyzer = SentimentIntensityAnalyzer()
IST = ZoneInfo("Asia/Kolkata")


def fetch_with_retry(fn, *args, retries=None, backoff=None, label="", **kwargs):
    """
    Call fn(*args, **kwargs), retrying on exception with exponential backoff.
    yfinance hits Yahoo's unofficial endpoints, which regularly return
    transient failures (rate limiting, connection resets) that look
    identical to "this stock just has no data" if you don't retry.
    Returns fn's result, or None if every attempt fails.
    """
    retries = FETCH_RETRIES if retries is None else retries
    backoff = FETCH_BACKOFF_SEC if backoff is None else backoff
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            if attempt < retries:
                wait = backoff * (2 ** (attempt - 1))
                print(f"[retry] {label or fn.__name__} attempt {attempt}/{retries} "
                      f"failed ({e}); retrying in {wait:.1f}s")
                time.sleep(wait)
    print(f"[retry] {label or fn.__name__} failed after {retries} attempts: {last_err}")
    return None

# ─────────────────────────── CONFIG ───────────────────────────

GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID         = os.environ.get("GOOGLE_SHEET_ID", "")  # no silent fallback

# ── Watchlist: NSE Nifty LargeMidcap 250 (all Nifty 100 + Nifty Midcap 150
#    constituents), pulled live from NSE Indices' published CSV so the
#    scanned universe always tracks the index's semi-annual reconstitution
#    instead of a hand-maintained ticker list going stale. See
#    fetch_nifty_largemidcap250() / get_watchlist() below.
NIFTY_LARGEMIDCAP250_CSV_URLS = [
    "https://niftyindices.com/IndexConstituent/ind_niftylargemidcap250list.csv",
    "https://nsearchives.nseindia.com/content/indices/ind_niftylargemidcap250list.csv",
]
EXPECTED_NIFTY_LARGEMIDCAP250_SIZE = 250
MIN_ACCEPTABLE_INDEX_SIZE          = 200  # sanity floor: reject a CSV that looks truncated/wrong

# Used ONLY if every live download attempt fails (site down, geo-blocked,
# CSV format changed, etc.) so the script can still produce a report
# instead of erroring out with an empty universe.
FALLBACK_WATCHLIST = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
    "LT.NS","SBIN.NS","BHARTIARTL.NS","ITC.NS","AXISBANK.NS",
    "KOTAKBANK.NS","MARUTI.NS","TATAMOTORS.NS","SUNPHARMA.NS","TITAN.NS",
    "BAJFINANCE.NS","ADANIENT.NS","ULTRACEMCO.NS","WIPRO.NS","HCLTECH.NS",
]


def fetch_nifty_largemidcap250(timeout=15):
    """
    Downloads NSE Indices' published constituent CSV for the Nifty
    LargeMidcap 250 (the "Index Constituent" download on the official
    index page — all Nifty 100 + Nifty Midcap 150 stocks). Tries each URL
    in NIFTY_LARGEMIDCAP250_CSV_URLS in order.

    Returns a list of "<SYMBOL>.NS" tickers on success, or None if every
    source fails / returns something that doesn't look like a real
    ~250-row constituent list (so callers can fall back safely instead of
    silently scanning a bad/truncated universe).
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; DailyStockAgent/1.0)"}
    for url in NIFTY_LARGEMIDCAP250_CSV_URLS:
        resp = fetch_with_retry(
            lambda: requests.get(url, headers=headers, timeout=timeout),
            label=f"Nifty LargeMidcap 250 list ({url})",
        )
        if resp is None:
            continue
        try:
            if resp.status_code != 200 or not resp.text.strip():
                print(f"[watchlist] {url}: HTTP {getattr(resp, 'status_code', '?')} or empty body")
                continue
            df = pd.read_csv(io.StringIO(resp.text))
            symbol_col = next((c for c in df.columns if c.strip().lower() == "symbol"), None)
            if not symbol_col:
                print(f"[watchlist] {url}: no 'Symbol' column found (columns: {list(df.columns)})")
                continue
            symbols = [f"{str(s).strip()}.NS" for s in df[symbol_col].dropna().tolist() if str(s).strip()]
            if len(symbols) < MIN_ACCEPTABLE_INDEX_SIZE:
                print(f"[watchlist] {url}: only parsed {len(symbols)} symbols "
                      f"(expected ~{EXPECTED_NIFTY_LARGEMIDCAP250_SIZE}) — treating as unreliable")
                continue
            print(f"[watchlist] Loaded {len(symbols)} symbols from Nifty LargeMidcap 250 ({url})")
            return symbols
        except Exception as e:
            print(f"[watchlist] Failed to parse CSV from {url}: {e}")
    return None


def get_watchlist():
    """
    Returns the symbols to scan: the live Nifty LargeMidcap 250
    constituent list if the download succeeds, otherwise the small
    static FALLBACK_WATCHLIST so the script can still run.

    Called fresh on every run_once() (via build_report()), so an
    after-market scheduled run automatically picks up any index
    reconstitution that happened that day — no separate "update the
    stock list" step is needed.
    """
    live = fetch_nifty_largemidcap250()
    if live:
        return live
    print(f"[watchlist] Live Nifty LargeMidcap 250 download failed — falling back to the "
          f"static {len(FALLBACK_WATCHLIST)}-symbol watchlist (see [watchlist]/[retry] lines above "
          f"for why). Fix network/site access to scan the full index again.")
    return FALLBACK_WATCHLIST

TOP_N                   = 10    # rows written to the sheet
LOOKBACK_DAYS           = 400
REQUEST_PAUSE_SEC       = 0.3   # be polite to Yahoo's unofficial endpoints
FETCH_RETRIES           = 3     # retries for transient yfinance/network failures
FETCH_BACKOFF_SEC       = 1.5   # base backoff, doubles each retry
HIGH_20D_LOOKBACK       = 20    # window for the 20-day-high breakout signal

# ── Delivery volume spike (NSE DELIV_QTY, not yfinance's traded volume) ──
DELIVERY_LOOKBACK_TRADING_DAYS = 20    # sessions used for the average
DELIVERY_SPIKE_MULTIPLIER      = 2.0   # today's delivery qty must be >= this x the average
DELIVERY_CACHE_MAX_CALENDAR_LOOKBACK = 45  # safety ceiling when walking back over holidays/weekends
NSE_BHAVCOPY_URL_TEMPLATE = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"

# ── Turnover (₹ value traded) ──
# "Turnover" here means close * volume — the rupee value traded, not
# share count. Used as a LIQUIDITY / tradability filter (can you enter
# and exit a swing position without moving the price?), distinct from
# the delivery-volume signals above, which measure conviction/
# accumulation rather than tradability. See analyze_technicals() and
# delivery_volume_signal() for where these are actually computed.
TURNOVER_LOOKBACK_TRADING_DAYS = 20     # window for the average, mirrors DELIVERY_LOOKBACK_TRADING_DAYS
TURNOVER_SPIKE_MULTIPLIER      = 2.0    # today's turnover must be >= this x the 20D average to count as a spike
MIN_AVG_TURNOVER_CR            = 5.0    # ₹ crore/day liquidity floor
REQUIRE_MIN_TURNOVER           = True   # hard gate (only enforced if STRICT_MODE)

# ── Volume z-score (statistical spike detection) ──
# z = (today's volume - rolling mean) / rolling std, over
# VOLUME_ZSCORE_LOOKBACK sessions. A z-score above VOLUME_ZSCORE_THRESHOLD
# flags a statistically unusual volume print (>2 standard deviations above
# recent norm), which is a stricter/different test than the fixed 1.3x
# "volume_spike" multiplier already feeding combined_score. See
# analyze_technicals() for where this is computed.
VOLUME_ZSCORE_LOOKBACK          = 20    # window for the rolling mean/std
VOLUME_ZSCORE_THRESHOLD         = 2.0   # z-score must be > this to count as a spike
REQUIRE_VOLUME_ZSCORE_SPIKE     = True  # hard gate (only enforced if STRICT_MODE)

# ── Shortlist gate ──
# STRICT_MODE = False (default): filters never hard-drop a stock. Every
#   symbol with usable price history gets scored and ranked; the sheet
#   always gets the TOP_N best candidates available that day. Each row
#   is labeled with which filters it did/didn't clear via the
#   "Meets All Filters" / "Filter Notes" columns, so you keep the
#   filter information without ever silently getting zero rows.
# STRICT_MODE = True: restores the old behavior — any stock failing a
#   "hard" gate below is dropped entirely, and only stocks passing every
#   gate are eligible. Can legitimately produce 0 results on a given day.
STRICT_MODE               = False

REQUIRE_ABOVE_200DMA      = True     # hard gate (only enforced if STRICT_MODE)
REQUIRE_20D_HIGH_BREAKOUT = True     # hard gate (only enforced if STRICT_MODE) — must cross the prior 20-day high
REQUIRE_DELIVERY_VOLUME_SPIKE = True # hard gate (only enforced if STRICT_MODE) — today's delivery qty >= 2x 20D avg
MIN_FUNDAMENTAL_SCORE     = 4        # hard gate, out of 10 (only enforced if STRICT_MODE)
ACCEPTED_ANALYST_RATINGS  = {"buy", "strong_buy"}  # soft gate
MIN_SENTIMENT_SCORE       = -0.05    # soft gate
MIN_BROKER_BUY_RATIO      = 0.40     # soft gate

# ── Sector rotation ──
SECTOR_BENCHMARK        = "^CRSLDX"
SECTOR_INDEX_TICKERS    = {
    "Bank":               "^NSEBANK",
    "IT":                 "^CNXIT",
    "Auto":               "^CNXAUTO",
    "Pharma":             "^CNXPHARMA",
    "FMCG":               "^CNXFMCG",
    "Metal":              "^CNXMETAL",
    "Realty":             "^CNXREALTY",
    "Energy":             "^CNXENERGY",
    "Infra":              "^CNXINFRA",
    "Media":              "^CNXMEDIA",
    "PSU Bank":           "^CNXPSUBANK",
    "Financial Services": "^CNXFIN",
}
YFINANCE_SECTOR_TO_NSE  = {
    "Financial Services":     "Bank",
    "Technology":             "IT",
    "Consumer Cyclical":      "Auto",
    "Healthcare":             "Pharma",
    "Consumer Defensive":     "FMCG",
    "Basic Materials":        "Metal",
    "Real Estate":            "Realty",
    "Energy":                 "Energy",
    "Industrials":            "Infra",
    "Communication Services": "Media",
    "Utilities":              "Infra",
}
# Sectors where FCF-based DCF isn't a meaningful valuation approach
DCF_EXCLUDED_YF_SECTORS = {"Financial Services"}

RS_WINDOW               = 55
RS_MOMENTUM_WINDOW      = 10
SECTOR_SCORE_ADJ        = {"Leading":1.5,"Improving":1.0,"Weakening":-0.5,"Lagging":-1.5,"Unknown":0.0}
EXCLUDE_LAGGING         = False

# ── Scheduled run time (IST) ──
# NSE trading hours are 09:15-15:30 IST. This is deliberately set well
# AFTER the 15:30 close (rather than the old 08:00 pre-market default)
# so that:
#   1. The technical signals (RSI, MAs, 20-day-high breakout, plain
#      volume spike, turnover) are computed off that day's own final
#      close, not the previous session's.
#   2. The delivery-volume-spike signal has a real shot at that day's
#      NSE bhavcopy (sec_bhavdata_full_*.csv) already being published —
#      NSE typically posts it in the hour or two after close, so 18:30
#      leaves a comfortable buffer. build_delivery_cache() still skips
#      any day whose file genuinely isn't up yet, so a late bhavcopy
#      just means one fewer session in the 20-day average, not a crash.
#   3. get_watchlist()'s live Nifty LargeMidcap 250 download and the
#      delivery cache are both re-fetched fresh on every run, so this
#      single time change is enough to make the whole stock list +
#      delivery data update automatically every day — no other code
#      changes needed.
# Adjust if your bhavcopy source publishes later/earlier for you.
SCHEDULED_HOUR, SCHEDULED_MINUTE = 18, 30

# ─────────────────────── GOOGLE SHEETS ────────────────────────

SHEET_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "Rank","Symbol","Date","Score",
    "Entry ₹","Stop-Loss ₹","Target ₹",
    "Last Close ₹","RSI","Trend","200-DMA ₹","Above 200-DMA",
    "20D High Breakout","20D High Prev ₹",
    "Delivery Vol Spike (2x)","Delivery Volume","Avg Delivery Vol 20D",
    "Delivery Turnover ₹cr","Avg Delivery Turnover 20D ₹cr",
    "Avg Turnover ₹cr (20D)","Turnover Spike (2x)",
    "Volume Z-Score","Volume Z-Score Spike (>2)",
    "Fundamental /10","P/E","ROE %","Profit Margin %","Revenue Growth %","D/E",
    "DCF Intrinsic Value ₹","vs CMP %",
    "News Sentiment","Sentiment Score",
    "Analyst Rating","Analyst Avg Target ₹",
    "Broker Buy %","Broker Analysts",
    "Sector","Sector Index","Quadrant","RS Change %",
    "Meets All Filters","Filter Notes",
]


def get_gspread_client():
    if not GOOGLE_CREDENTIALS_JSON:
        raise EnvironmentError(
            "GOOGLE_CREDENTIALS_JSON environment variable is not set. "
            "See README for setup instructions."
        )
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SHEET_SCOPES)
    return gspread.authorize(creds)


def get_sheet_id():
    if not GOOGLE_SHEET_ID:
        raise EnvironmentError(
            "GOOGLE_SHEET_ID environment variable is not set. "
            "Refusing to fall back to a hardcoded spreadsheet ID — "
            "set GOOGLE_SHEET_ID to the ID from your own Sheet's URL."
        )
    return GOOGLE_SHEET_ID


def sync_headers(ws, tab_name):
    """
    Make sure an EXISTING worksheet's header row (row 1) matches the
    current HEADERS list exactly.

    Without this, a tab created by an older version of this script
    (e.g. "History", or a dated tab re-run on the same day before this
    update) keeps its old header row forever, since get_or_create_tab()
    previously only wrote headers at creation time. New columns (like
    "20D High Breakout") then get appended as extra, unlabeled values
    past the old header — they're in the sheet, but with no header text,
    so they're easy to miss or look "missing" entirely.

    This resizes the sheet if it has fewer columns than HEADERS needs,
    then overwrites row 1 with the current HEADERS whenever it differs
    from what's already there.
    """
    needed_cols = len(HEADERS)
    if ws.col_count < needed_cols:
        ws.resize(cols=needed_cols)

    existing_header = ws.row_values(1)
    if existing_header != HEADERS:
        print(f"[sheets] Header mismatch on '{tab_name}' tab — updating to current schema "
              f"({len(existing_header)} -> {len(HEADERS)} columns).")
        last_col = gspread.utils.rowcol_to_a1(1, needed_cols).rstrip("1")
        ws.update(f"A1:{last_col}1", [HEADERS], value_input_option="USER_ENTERED")
        ws.format(f"A1:{last_col}1", {"textFormat": {"bold": True}})


def get_or_create_tab(spreadsheet, tab_name):
    """Return existing worksheet (with headers kept in sync) or create a new one."""
    try:
        ws = spreadsheet.worksheet(tab_name)
        print(f"[sheets] Using existing tab: {tab_name}")
        sync_headers(ws, tab_name)
        return ws
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=200, cols=len(HEADERS))
        ws.append_row(HEADERS, value_input_option="USER_ENTERED")
        last_col = gspread.utils.rowcol_to_a1(1, len(HEADERS)).rstrip("1")
        ws.format(f"A1:{last_col}1", {"textFormat": {"bold": True}})
        ws.freeze(rows=1)
        print(f"[sheets] Created new tab: {tab_name}")
        return ws


def row_to_sheet_values(rank, r):
    """Convert a result dict to a flat list matching HEADERS order."""
    vs_cmp = ""
    if r.get("intrinsic_value") and r.get("last_close"):
        vs_cmp = f"{((r['intrinsic_value'] - r['last_close']) / r['last_close'] * 100):+.1f}%"

    return [
        rank,
        r["symbol"].replace(".NS", ""),
        datetime.now(IST).strftime("%d-%b-%Y"),
        r.get("combined_score", ""),
        r.get("entry", ""),
        r.get("stop_loss", ""),
        r.get("target", ""),
        r.get("last_close", ""),
        r.get("rsi", ""),
        "Up" if r.get("uptrend") else "Flat/Down",
        r.get("ma200", ""),
        "Yes ✅" if r.get("above_200dma") else "No ❌",
        "Yes ✅" if r.get("crosses_20d_high") else "No ❌",
        r.get("high_20d_prev", ""),
        "Yes ✅" if r.get("delivery_volume_spike") else "No ❌",
        r.get("delivery_volume", ""),
        r.get("avg_delivery_volume_20d", ""),
        r.get("delivery_turnover_cr", ""),
        r.get("avg_delivery_turnover_20d_cr", ""),
        r.get("avg_turnover_cr", ""),
        "Yes ✅" if r.get("turnover_spike") else "No ❌",
        r.get("volume_zscore", ""),
        "Yes ✅" if r.get("volume_zscore_spike") else "No ❌",
        r.get("fundamental_score", ""),
        round(r["pe"], 1) if r.get("pe") else "",
        f"{r['roe']*100:.1f}" if r.get("roe") else "",
        f"{r['profit_margin']*100:.1f}" if r.get("profit_margin") else "",
        f"{r['revenue_growth']*100:.1f}" if r.get("revenue_growth") else "",
        round(r["debt_to_equity"], 1) if r.get("debt_to_equity") else "",
        r.get("intrinsic_value", ""),
        vs_cmp,
        "Positive" if r.get("sentiment_score", 0) > 0.15 else "Negative" if r.get("sentiment_score", 0) < -0.15 else "Neutral",
        r.get("sentiment_score", ""),
        r.get("recommendation", "n/a").replace("_", " ").title(),
        round(r["target_mean"], 0) if r.get("target_mean") else "",
        f"{int(r['broker_buy_ratio']*100)}%" if r.get("broker_buy_ratio") is not None else "",
        r.get("broker_total", ""),
        r.get("sector_label", ""),
        r.get("sector_index", ""),
        r.get("sector_quadrant", ""),
        r.get("sector_rs_change_pct", ""),
        "Yes ✅" if r.get("meets_all_filters") else "No ⚠️",
        "; ".join(r.get("filter_notes", [])) or "-",
    ]


def write_to_sheets(rows):
    """Open the spreadsheet, create today's tab if needed, and append rows."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(get_sheet_id())

    today_tab  = datetime.now(IST).strftime("%d-%b-%Y")
    history_ws = get_or_create_tab(spreadsheet, "History")   # rolling log
    today_ws   = get_or_create_tab(spreadsheet, today_tab)   # daily snapshot

    # ── Clear today's tab so re-runs overwrite cleanly ──
    # IMPORTANT: we clear cell VALUES (batch_clear), not delete_rows().
    # Row 1 is frozen (see get_or_create_tab), and the Sheets API refuses
    # any deleteDimension request that would remove every non-frozen row
    # (error: "Sorry, it is not possible to delete all non-frozen rows").
    # The previous version called delete_rows(2, row_count) which does
    # exactly that whenever the tab has no rows below the header left
    # over — clearing values instead sidesteps the restriction entirely
    # and needs no row-count math.
    last_col_letter = gspread.utils.rowcol_to_a1(1, len(HEADERS)).rstrip("0123456789")
    clear_range = f"A2:{last_col_letter}{max(today_ws.row_count, 1000)}"
    try:
        today_ws.batch_clear([clear_range])
    except Exception as e:
        print(f"[sheets] Warning: could not clear existing rows in '{today_tab}' "
              f"before rewriting ({e}); proceeding to append anyway.")

    sheet_rows = [row_to_sheet_values(i + 1, r) for i, r in enumerate(rows)]
    if sheet_rows:
        reset_row_formatting(today_ws)
        today_ws.append_rows(sheet_rows, value_input_option="USER_ENTERED")
        color_by_quadrant(today_ws, sheet_rows)

    # ── Also append to the History tab ──
    history_ws.append_rows(sheet_rows, value_input_option="USER_ENTERED")

    print(f"[sheets] Wrote {len(sheet_rows)} row(s) to '{today_tab}' tab and History.")
    return today_tab


def reset_row_formatting(ws):
    """
    Clear any leftover quadrant background coloring from a previous run
    on this tab, so a re-run with fewer/different rows doesn't leave
    stale colored cells behind. Cheap: one batch_update over a generous
    row range.
    """
    quadrant_col = HEADERS.index("Quadrant") + 1
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": max(ws.row_count, 1000),
                        "startColumnIndex": quadrant_col - 1,
                        "endColumnIndex": quadrant_col,
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }]
        })
    except Exception as e:
        print(f"[sheets] Warning: could not reset row formatting: {e}")


def color_by_quadrant(ws, sheet_rows):
    """
    Colour-code the Quadrant column: Leading = green, Improving = blue,
    Weakening = yellow, Lagging = red.

    Batched into a single batch_update call instead of one ws.format()
    call per row, to avoid burning through the Sheets API's per-minute
    write-request quota as TOP_N grows.
    """
    quadrant_col = HEADERS.index("Quadrant") + 1
    color_map = {
        "Leading":   {"red": 0.72, "green": 0.96, "blue": 0.72},
        "Improving": {"red": 0.68, "green": 0.85, "blue": 0.96},
        "Weakening": {"red": 1.00, "green": 0.95, "blue": 0.60},
        "Lagging":   {"red": 1.00, "green": 0.70, "blue": 0.70},
    }

    requests = []
    for i, row in enumerate(sheet_rows):
        quadrant = row[HEADERS.index("Quadrant")]
        bg = color_map.get(quadrant)
        if not bg:
            continue
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": i + 1,       # +1 to skip header row
                    "endRowIndex": i + 2,
                    "startColumnIndex": quadrant_col - 1,
                    "endColumnIndex": quadrant_col,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": bg}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})


# ────────────────────── TECHNICALS ───────────────────────────

def compute_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_g = gain.rolling(period).mean()
    avg_l = loss.rolling(period).mean()
    rs    = avg_g / avg_l.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))


def analyze_technicals(symbol, ticker):
    try:
        hist = fetch_with_retry(ticker.history, period=f"{LOOKBACK_DAYS}d", label=f"{symbol} history")
        if hist is None or hist.empty:
            print(f"[technical] {symbol}: no price history returned (fetch failed or symbol has no data)")
            return None
        if len(hist) < 205:
            print(f"[technical] {symbol}: only {len(hist)} rows of history, need >= 205 for 200-DMA")
            return None
        close, vol = hist["Close"], hist["Volume"]
        ma20  = close.rolling(20).mean()
        ma50  = close.rolling(50).mean()
        ma200 = close.rolling(200).mean()
        rsi   = compute_rsi(close)

        lc      = close.iloc[-1]
        l200    = ma200.iloc[-1]
        uptrend = ma20.iloc[-1] > ma50.iloc[-1]
        above200= bool(pd.notna(l200) and lc > l200)
        bullish_cross = uptrend and ma20.iloc[-2] <= ma50.iloc[-2]
        healthy_rsi   = 45 <= rsi.iloc[-1] <= 68
        vol_spike     = vol.iloc[-1] > 1.3 * vol.rolling(20).mean().iloc[-1]

        # ── Average daily turnover (₹) — liquidity floor, see MIN_AVG_TURNOVER_CR ──
        # Uses close*volume per session (a standard approximation for daily
        # turnover; NSE's own bhavcopy TTL_TRD_QNTY/TURNOVER_LACS would be
        # more exact but this avoids a second network fetch per symbol).
        # This is purely a tradability/liquidity measure — separate from
        # the delivery-volume signals below, which measure conviction.
        turnover_series  = close * vol
        avg_turnover_20d = turnover_series.rolling(TURNOVER_LOOKBACK_TRADING_DAYS).mean().iloc[-1]
        avg_turnover_cr  = round(avg_turnover_20d / 1e7, 2) if pd.notna(avg_turnover_20d) else None  # ₹ crore
        today_turnover   = close.iloc[-1] * vol.iloc[-1]
        turnover_spike   = bool(
            pd.notna(avg_turnover_20d) and avg_turnover_20d > 0
            and today_turnover >= TURNOVER_SPIKE_MULTIPLIER * avg_turnover_20d
        )

        # ── Volume z-score (statistical spike detection) ──
        # z = (today's volume - rolling mean) / rolling std over
        # VOLUME_ZSCORE_LOOKBACK sessions. Distinct from vol_spike above
        # (a fixed 1.3x multiplier that feeds combined_score) — this is a
        # standard-deviation based test, flagged when z > VOLUME_ZSCORE_
        # THRESHOLD (default 2.0), i.e. today's volume is a statistically
        # unusual print relative to its own recent distribution rather
        # than just "somewhat above average."
        vol_roll_mean = vol.rolling(VOLUME_ZSCORE_LOOKBACK).mean().iloc[-1]
        vol_roll_std  = vol.rolling(VOLUME_ZSCORE_LOOKBACK).std().iloc[-1]
        volume_zscore = None
        volume_zscore_spike = False
        if pd.notna(vol_roll_mean) and pd.notna(vol_roll_std) and vol_roll_std > 0:
            volume_zscore = (vol.iloc[-1] - vol_roll_mean) / vol_roll_std
            volume_zscore_spike = bool(volume_zscore > VOLUME_ZSCORE_THRESHOLD)
            volume_zscore = round(float(volume_zscore), 2)

        # ── 20-day high breakout ──
        # True when today's close crosses above the highest close of the
        # prior HIGH_20D_LOOKBACK trading days (classic breakout trigger).
        # Uses the window BEFORE today so a stock only qualifies the day
        # it actually breaks out, not every day it simply sits at a high.
        high_20d_prev = None
        crosses_20d_high = False
        if len(close) > HIGH_20D_LOOKBACK:
            high_20d_prev = close.iloc[-(HIGH_20D_LOOKBACK + 1):-1].max()
            crosses_20d_high = bool(pd.notna(high_20d_prev) and lc > high_20d_prev)

        score = sum([bullish_cross * 2, uptrend, healthy_rsi, vol_spike])
        atr   = (hist["High"] - hist["Low"]).rolling(14).mean().iloc[-1]
        rlow  = close.rolling(20).min().iloc[-1]

        return {
            "symbol": symbol, "score": score,
            "last_close": round(lc, 2), "rsi": round(rsi.iloc[-1], 1),
            "uptrend": bool(uptrend), "volume_spike": bool(vol_spike),
            "entry": round(lc, 2),
            "stop_loss": round(min(rlow, lc - 1.5 * atr), 2),
            "target": round(lc + 2.5 * atr, 2),
            "ma200": round(l200, 2) if pd.notna(l200) else None,
            "above_200dma": above200,
            "crosses_20d_high": crosses_20d_high,
            "high_20d_prev": round(high_20d_prev, 2) if pd.notna(high_20d_prev) else None,
            "avg_turnover_cr": avg_turnover_cr,
            "turnover_spike": turnover_spike,
            "volume_zscore": volume_zscore,
            "volume_zscore_spike": volume_zscore_spike,
        }
    except Exception as e:
        print(f"[technical] {symbol}: {e}")
        return None


# ─────────────────────── ANALYST ─────────────────────────────

def analyst_consensus(symbol, ticker, info):
    return {
        "recommendation": info.get("recommendationKey", "n/a"),
        "target_mean":    info.get("targetMeanPrice"),
        "num_analysts":   info.get("numberOfAnalystOpinions"),
    }


# ─────────────────────── FUNDAMENTALS ────────────────────────

def fundamental_score(symbol, ticker, info):
    pe   = info.get("trailingPE")
    roe  = info.get("returnOnEquity")
    de   = info.get("debtToEquity")
    pm   = info.get("profitMargins")
    rg   = info.get("revenueGrowth")
    cr   = info.get("currentRatio")

    score = 0
    if pe  and 0 < pe  <= 30:  score += 2
    if roe and roe > 0.15:     score += 2
    if de  and de  < 100:      score += 2
    if pm  and pm  > 0.10:     score += 2
    if rg  and rg  > 0.05:     score += 1
    if cr  and cr  > 1:        score += 1

    return {
        "fundamental_score": score,
        "pe": pe, "roe": roe, "debt_to_equity": de,
        "profit_margin": pm, "revenue_growth": rg, "current_ratio": cr,
    }


# ──────────────────────── DCF ────────────────────────────────

def dcf_intrinsic_value(symbol, ticker, info, g=0.10, r=0.12, tg=0.04, years=5, max_hist_years=3):
    """
    FCF-based DCF, averaged over up to `max_hist_years` of historical
    operating cash flow to reduce sensitivity to a single unusual year.
    Skipped entirely for Financial Services, where FCF/capex don't map
    onto a standard business model (deposits/loans dominate cash flow).
    """
    if info.get("sector") in DCF_EXCLUDED_YF_SECTORS:
        return {"intrinsic_value": None, "dcf_skipped_reason": "sector excluded (financials)"}

    if r <= tg:
        return {"intrinsic_value": None, "dcf_skipped_reason": "invalid r <= terminal growth"}

    try:
        cf = fetch_with_retry(lambda: ticker.cashflow, label=f"{symbol} cashflow")
        so = info.get("sharesOutstanding")
        if cf is None or cf.empty or not so:
            return {"intrinsic_value": None, "dcf_skipped_reason": "missing cashflow/shares data"}

        opcf_r = next((x for x in cf.index if "Operating Cash Flow" in x), None)
        capx_r = next((x for x in cf.index if "Capital Expenditure" in x), None)
        if not opcf_r or not capx_r:
            return {"intrinsic_value": None, "dcf_skipped_reason": "missing OCF/CapEx line items"}

        n_years = min(max_hist_years, cf.loc[opcf_r].shape[0], cf.loc[capx_r].shape[0])
        fcf_samples = [
            cf.loc[opcf_r].iloc[i] + cf.loc[capx_r].iloc[i]
            for i in range(n_years)
        ]
        fcf_samples = [x for x in fcf_samples if pd.notna(x)]
        if not fcf_samples:
            return {"intrinsic_value": None, "dcf_skipped_reason": "no usable FCF samples"}

        fcf = sum(fcf_samples) / len(fcf_samples)
        if fcf <= 0:
            return {"intrinsic_value": None, "dcf_skipped_reason": "average FCF non-positive"}

        pv, proj = 0, fcf
        for yr in range(1, years + 1):
            proj *= (1 + g)
            pv   += proj / (1 + r) ** yr
        tv  = proj * (1 + tg) / (r - tg)
        pv += tv / (1 + r) ** years
        return {"intrinsic_value": round(pv / so, 2)}
    except Exception as e:
        print(f"[dcf] {symbol}: {e}")
        return {"intrinsic_value": None, "dcf_skipped_reason": f"error: {e}"}


# ─────────────────────── SENTIMENT ───────────────────────────

def news_sentiment(symbol, ticker, info, n=8):
    try:
        items = fetch_with_retry(lambda: ticker.news, label=f"{symbol} news") or []
        heads = []
        for it in items[:n]:
            t = it.get("content", {}).get("title") or it.get("title")
            if t:
                heads.append(t)
        if not heads:
            return {"sentiment_score": 0.0, "headline_count": 0}
        scores = [_sentiment_analyzer.polarity_scores(h)["compound"] for h in heads]
        return {"sentiment_score": round(sum(scores) / len(scores), 3), "headline_count": len(heads)}
    except Exception as e:
        print(f"[sentiment] {symbol}: {e}")
        return {"sentiment_score": 0.0, "headline_count": 0}


# ─────────────────────── BROKER TREND ────────────────────────

def broker_recommendation_trend(symbol, ticker, info):
    try:
        trend = fetch_with_retry(lambda: ticker.recommendations, label=f"{symbol} recommendations")
        if trend is None or trend.empty:
            return {"broker_buy_ratio": None, "broker_total": 0}
        l = trend.iloc[0]
        sb, b, h, s, ss = (l.get(k, 0) for k in ("strongBuy", "buy", "hold", "sell", "strongSell"))
        total = sb + b + h + s + ss
        if not total:
            return {"broker_buy_ratio": None, "broker_total": 0}
        return {"broker_buy_ratio": round((sb + b) / total, 2), "broker_total": int(total)}
    except Exception as e:
        print(f"[broker] {symbol}: {e}")
        return {"broker_buy_ratio": None, "broker_total": 0}


# ─────────────────── SECTOR ROTATION ─────────────────────────

_rotation_cache = None


def _fetch_close(ticker_symbol, days=250):
    try:
        h = yf.Ticker(ticker_symbol).history(period=f"{days}d")
        return h["Close"] if not h.empty else None
    except Exception:
        return None


def compute_sector_rotation():
    bench = _fetch_close(SECTOR_BENCHMARK)
    result = {}
    if bench is None or len(bench) < RS_WINDOW + RS_MOMENTUM_WINDOW:
        return {n: {"quadrant": "Unknown", "rs_ratio": None, "rs_change_pct": None}
                for n in SECTOR_INDEX_TICKERS}
    for name, tkr in SECTOR_INDEX_TICKERS.items():
        sc = _fetch_close(tkr)
        if sc is None:
            result[name] = {"quadrant": "Unknown", "rs_ratio": None, "rs_change_pct": None}
            continue
        combined = pd.concat([sc, bench], axis=1, join="inner")
        combined.columns = ["s", "b"]
        if len(combined) < RS_WINDOW + RS_MOMENTUM_WINDOW:
            result[name] = {"quadrant": "Unknown", "rs_ratio": None, "rs_change_pct": None}
            continue
        rs_ratio = (combined["s"] / combined["b"]) / ((combined["s"] / combined["b"]).rolling(RS_WINDOW).mean())
        rl, rp = rs_ratio.iloc[-1], rs_ratio.iloc[-1 - RS_MOMENTUM_WINDOW]
        if pd.isna(rl) or pd.isna(rp):
            result[name] = {"quadrant": "Unknown", "rs_ratio": None, "rs_change_pct": None}
            continue
        rising = rl > rp
        outper = rl > 1.0
        q = ("Leading" if outper and rising else "Weakening" if outper else
             "Lagging" if not outper and not rising else "Improving")
        result[name] = {"quadrant": q, "rs_ratio": round(float(rl), 4), "rs_change_pct": round(((rl / rp) - 1) * 100, 2)}
    return result


def get_rotation():
    global _rotation_cache
    if _rotation_cache is None:
        _rotation_cache = compute_sector_rotation()
    return _rotation_cache


def stock_sector_rotation(symbol, ticker, info):
    yf_sector   = info.get("sector")
    yf_industry = (info.get("industry") or "").lower()

    nse_idx = YFINANCE_SECTOR_TO_NSE.get(yf_sector)
    if nse_idx == "Bank":
        if "public" in yf_industry or "psu" in yf_industry:
            nse_idx = "PSU Bank"
        elif "bank" not in yf_industry:
            nse_idx = "Financial Services"

    if nse_idx is None:
        return {"sector_label": yf_sector or "Unknown", "sector_index": None,
                "sector_quadrant": "Unknown", "sector_rs_change_pct": None,
                "sector_score_adj": 0.0}

    data = get_rotation().get(nse_idx, {"quadrant": "Unknown", "rs_change_pct": None})
    q = data["quadrant"]
    return {"sector_label": yf_sector or nse_idx, "sector_index": nse_idx,
            "sector_quadrant": q, "sector_rs_change_pct": data.get("rs_change_pct"),
            "sector_score_adj": SECTOR_SCORE_ADJ.get(q, 0.0)}


# ─────────────────── DELIVERY VOLUME ─────────────────────────
#
# "Delivery volume" (NSE's DELIV_QTY) is the portion of a day's traded
# quantity that actually settled into demat accounts rather than being
# squared off intraday. yfinance doesn't expose this — it only has
# total traded volume — so it's sourced separately from NSE's own daily
# "full bhavcopy" file, which covers every symbol for one trading day
# per file. To avoid downloading ~20 files PER SYMBOL, the whole
# watchlist's delivery history is fetched ONCE per run into
# _delivery_cache, and delivery_volume_signal() just looks each symbol
# up in that cache.

_delivery_cache = None  # symbol (no ".NS") -> pd.Series(date-indexed DELIV_QTY), oldest->newest


def fetch_bhavcopy_delivery(date_obj):
    """
    Downloads one day's NSE full bhavcopy (sec_bhavdata_full_DDMMYYYY.csv),
    which carries a DELIV_QTY column per symbol, and returns a
    pd.Series indexed by SYMBOL (EQ series only) -> DELIV_QTY.
    Returns None if the file isn't available for that date (weekend,
    market holiday, not yet published, transient network failure) or
    doesn't parse as expected — callers simply skip that date rather
    than treating it as zero delivery volume.
    """
    date_str = date_obj.strftime("%d%m%Y")
    url = NSE_BHAVCOPY_URL_TEMPLATE.format(date=date_str)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; DailyStockAgent/1.0)"}
    resp = fetch_with_retry(
        lambda: requests.get(url, headers=headers, timeout=15),
        retries=1, label=f"bhavcopy {date_str}",
    )
    if resp is None or resp.status_code != 200 or not resp.text.strip():
        return None
    try:
        df = pd.read_csv(io.StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]
        if "SYMBOL" not in df.columns or "DELIV_QTY" not in df.columns:
            print(f"[delivery] {date_str}: bhavcopy missing SYMBOL/DELIV_QTY columns "
                  f"(columns: {list(df.columns)})")
            return None
        if "SERIES" in df.columns:
            df = df[df["SERIES"].astype(str).str.strip() == "EQ"]
        df["SYMBOL"]    = df["SYMBOL"].astype(str).str.strip()
        df["DELIV_QTY"] = pd.to_numeric(df["DELIV_QTY"], errors="coerce")
        df = df.dropna(subset=["DELIV_QTY"]).set_index("SYMBOL")
        return df["DELIV_QTY"]
    except Exception as e:
        print(f"[delivery] Failed to parse bhavcopy for {date_str}: {e}")
        return None


def build_delivery_cache(symbols,
                          lookback_trading_days=DELIVERY_LOOKBACK_TRADING_DAYS,
                          max_calendar_lookback=DELIVERY_CACHE_MAX_CALENDAR_LOOKBACK):
    """
    Walks backwards day-by-day from today (skipping Sat/Sun), downloading
    NSE's daily bhavcopy until `lookback_trading_days + 1` sessions have
    been collected (the "+1" is today's/most-recent session, used as the
    spike day; the remaining sessions form the 20-day average) or
    `max_calendar_lookback` calendar days have been checked, whichever
    comes first. Days that fail to download (holidays, files not yet
    published, transient errors) are silently skipped and don't count
    toward either limit's numerator.

    Returns: dict of symbol (no ".NS" suffix) -> pd.Series(date-indexed
    DELIV_QTY), sorted oldest->newest. Symbols with fewer than 2 sessions
    of data are omitted — delivery_volume_signal() treats a missing
    symbol as "not enough data" rather than a false spike/non-spike.
    """
    per_symbol = {}
    day = datetime.now(IST).date()
    collected_days = 0
    calendar_checked = 0
    while collected_days < lookback_trading_days + 1 and calendar_checked < max_calendar_lookback:
        if day.weekday() < 5:  # Mon-Fri only; NSE holidays are handled by the None-return skip below
            deliv = fetch_bhavcopy_delivery(day)
            if deliv is not None:
                for sym, qty in deliv.items():
                    per_symbol.setdefault(sym, {})[day] = qty
                collected_days += 1
        day -= timedelta(days=1)
        calendar_checked += 1

    cache = {sym: pd.Series(d).sort_index() for sym, d in per_symbol.items()}
    print(f"[delivery] Built delivery-volume cache from {collected_days} session(s) of NSE bhavcopy "
          f"({calendar_checked} calendar day(s) checked); {len(cache)} symbol(s) have usable history.")
    return cache


def delivery_volume_signal(symbol, last_close=None):
    """
    Looks `symbol` up in the module-level _delivery_cache (built once per
    run by build_delivery_cache, called from build_report) and checks
    whether the most recent session's delivered quantity is
    >= DELIVERY_SPIKE_MULTIPLIER x the average of the prior
    DELIVERY_LOOKBACK_TRADING_DAYS sessions.

    Returns delivery_volume_spike=False (with None volumes) if the cache
    hasn't been built yet or this symbol doesn't have enough cached
    history (bhavcopy fetch failures, newly listed stock, symbol not
    covered by NSE cash-market bhavcopy, etc.) — same "missing data
    degrades gracefully, never crashes" pattern as the other signals.

    If `last_close` is provided, also converts delivered quantity into
    ₹ DELIVERY TURNOVER (delivery_volume * last_close) — this normalizes
    across stocks of very different share prices, since "2x delivery
    volume" means something different for a ₹20 stock vs a ₹2,000 one.
    last_close (today's close) is applied to both today's and the
    historical average delivery quantity; this is an approximation (a
    fully precise figure would use each day's own close) that's
    acceptable for ranking/comparison purposes, not exact ₹ accounting.
    Does NOT change delivery_volume_spike's pass/fail logic, which stays
    share-count based to match NSE's own convention.
    """
    global _delivery_cache
    empty_result = {
        "delivery_volume": None, "avg_delivery_volume_20d": None,
        "delivery_volume_spike": False,
        "delivery_turnover_cr": None, "avg_delivery_turnover_20d_cr": None,
    }

    if not _delivery_cache:
        return empty_result

    bare = symbol.replace(".NS", "")
    series = _delivery_cache.get(bare)
    if series is None or len(series) < 2:
        return empty_result

    latest = series.iloc[-1]
    prior  = series.iloc[:-1].tail(DELIVERY_LOOKBACK_TRADING_DAYS)
    if prior.empty:
        result = {"delivery_volume": int(latest), "avg_delivery_volume_20d": None,
                   "delivery_volume_spike": False}
    else:
        avg_prior = prior.mean()
        spike = bool(avg_prior > 0 and latest >= DELIVERY_SPIKE_MULTIPLIER * avg_prior)
        result = {
            "delivery_volume": int(latest),
            "avg_delivery_volume_20d": round(float(avg_prior), 0),
            "delivery_volume_spike": spike,
        }

    # ₹-value delivery turnover, in crore, for cross-stock comparability
    if last_close and result.get("delivery_volume") is not None:
        result["delivery_turnover_cr"] = round(result["delivery_volume"] * last_close / 1e7, 2)
    else:
        result["delivery_turnover_cr"] = None

    if last_close and result.get("avg_delivery_volume_20d") is not None:
        result["avg_delivery_turnover_20d_cr"] = round(result["avg_delivery_volume_20d"] * last_close / 1e7, 2)
    else:
        result["avg_delivery_turnover_20d_cr"] = None

    return result


# ─────────────────────── FILTERS ─────────────────────────────
#
# evaluate_filters() always computes which gates a stock fails and
# NEVER itself decides to drop the stock — that decision is made by
# build_report() based on STRICT_MODE. This means:
#   - STRICT_MODE = False (default): every stock with technical data
#     gets ranked and shown; "hard" gate failures are just notes.
#   - STRICT_MODE = True: build_report drops any stock with a
#     "(hard gate)" note, same as the previous version's behavior.
#
# "Soft" gate notes (analyst rating, sentiment, broker ratio, sector
# quadrant) never drop a stock even in STRICT_MODE if the underlying
# data is simply missing — only an actual unfavorable value counts.
#
# NOTE: the 20-day high breakout signal (REQUIRE_20D_HIGH_BREAKOUT), the
# 2x delivery-volume spike signal (REQUIRE_DELIVERY_VOLUME_SPIKE), the
# turnover liquidity floor (REQUIRE_MIN_TURNOVER), and the volume
# z-score spike (REQUIRE_VOLUME_ZSCORE_SPIKE) are all wired in below as
# "hard" gates, same tier as REQUIRE_ABOVE_200DMA — only enforced (i.e.
# actually drops the stock) when STRICT_MODE=True. None of them
# contribute to combined_score directly; all are pass/fail
# buying/tradability conditions surfaced via "Meets All Filters" /
# "Filter Notes". Like turnover, missing/undefined z-score data is
# itself treated as a failure (rather than being silently skipped),
# since "not enough history to judge" shouldn't default to "pass."

def evaluate_filters(r):
    notes = []
    hard_fail = False

    if REQUIRE_ABOVE_200DMA and not r.get("above_200dma"):
        notes.append("below 200-DMA (hard gate)")
        hard_fail = True

    if REQUIRE_20D_HIGH_BREAKOUT and not r.get("crosses_20d_high"):
        notes.append("did not cross 20-day high (hard gate)")
        hard_fail = True

    if REQUIRE_DELIVERY_VOLUME_SPIKE and not r.get("delivery_volume_spike"):
        notes.append(f"delivery volume did not spike {DELIVERY_SPIKE_MULTIPLIER}x vs "
                      f"{DELIVERY_LOOKBACK_TRADING_DAYS}D avg (hard gate)")
        hard_fail = True

    if REQUIRE_MIN_TURNOVER:
        turnover = r.get("avg_turnover_cr")
        if turnover is None:
            notes.append("avg turnover unavailable (hard gate)")
            hard_fail = True
        elif turnover < MIN_AVG_TURNOVER_CR:
            notes.append(f"avg turnover ₹{turnover}cr < ₹{MIN_AVG_TURNOVER_CR}cr (hard gate)")
            hard_fail = True

    if REQUIRE_VOLUME_ZSCORE_SPIKE:
        zscore = r.get("volume_zscore")
        if zscore is None:
            notes.append("volume z-score unavailable (hard gate)")
            hard_fail = True
        elif zscore <= VOLUME_ZSCORE_THRESHOLD:
            notes.append(f"volume z-score {zscore} <= {VOLUME_ZSCORE_THRESHOLD} (hard gate)")
            hard_fail = True

    if r.get("fundamental_score", 0) < MIN_FUNDAMENTAL_SCORE:
        notes.append(f"fundamental score {r.get('fundamental_score', 0)} < {MIN_FUNDAMENTAL_SCORE} (hard gate)")
        hard_fail = True

    rec = r.get("recommendation")
    if rec not in (None, "n/a") and rec not in ACCEPTED_ANALYST_RATINGS:
        notes.append(f"analyst rating '{rec}' not in {ACCEPTED_ANALYST_RATINGS} (soft gate)")

    sentiment = r.get("sentiment_score", 0)
    if r.get("headline_count", 0) > 0 and sentiment < MIN_SENTIMENT_SCORE:
        notes.append(f"sentiment {sentiment} < {MIN_SENTIMENT_SCORE} (soft gate)")

    br = r.get("broker_buy_ratio")
    if br is not None and br < MIN_BROKER_BUY_RATIO:
        notes.append(f"broker buy ratio {br} < {MIN_BROKER_BUY_RATIO} (soft gate)")

    if EXCLUDE_LAGGING and r.get("sector_quadrant") == "Lagging":
        notes.append("sector quadrant is Lagging (soft gate)")

    return {"meets_all_filters": len(notes) == 0, "hard_fail": hard_fail, "filter_notes": notes}


# ────────────────────── BUILD REPORT ─────────────────────────

def build_report(verbose=False, watchlist=None):
    global _delivery_cache
    if watchlist is None:
        watchlist = get_watchlist()

    # Built ONCE for the whole watchlist (one bhavcopy file per day covers
    # every symbol), not once per symbol — see the DELIVERY VOLUME section.
    _delivery_cache = build_delivery_cache(watchlist)

    rows = []
    no_history = []   # symbols we couldn't even get price data for

    diag = {"scanned": 0, "history_ok": 0, "info_ok": 0}

    for sym in watchlist:
        diag["scanned"] += 1
        ticker = yf.Ticker(sym)

        tech = analyze_technicals(sym, ticker)
        if not tech:
            no_history.append(sym)
            continue
        diag["history_ok"] += 1

        info = fetch_with_retry(lambda: ticker.info, label=f"{sym} info") or {}
        if info:
            diag["info_ok"] += 1
        else:
            print(f"[info] {sym}: .info returned empty after retries — "
                  f"fundamental/analyst/sector fields will be blank for this stock")

        for fn in (analyst_consensus, fundamental_score, dcf_intrinsic_value,
                   news_sentiment, broker_recommendation_trend, stock_sector_rotation):
            tech.update(fn(sym, ticker, info))

        tech.update(delivery_volume_signal(sym, last_close=tech.get("last_close")))

        # Combined score
        c = tech["score"]
        c += tech.get("fundamental_score", 0) * 0.5
        c += tech.get("sentiment_score", 0) * 2
        br = tech.get("broker_buy_ratio")
        if br is not None:
            c += br * 2
        iv, lc = tech.get("intrinsic_value"), tech.get("last_close", 0)
        if iv and lc and (iv - lc) / lc > 0.15:
            c += 2
        if tech.get("recommendation") in ("buy", "strong_buy") and tech.get("uptrend"):
            c += 2
        c += tech.get("sector_score_adj", 0)
        tech["combined_score"] = round(c, 2)

        tech.update(evaluate_filters(tech))
        rows.append(tech)
        time.sleep(REQUEST_PAUSE_SEC)

    print(f"\n[diagnostics] {diag['scanned']} symbols scanned, "
          f"{diag['history_ok']} had usable price history, "
          f"{diag['info_ok']} had usable fundamental/.info data.")
    if no_history:
        print(f"[diagnostics] No price history for: {', '.join(no_history)} "
              f"(likely a yfinance/network issue if this list is long — "
              f"see the [technical]/[retry] lines above for the specific error)")

    if not rows:
        print("[build_report] No stocks had usable technical data at all — "
              "this is a data/connectivity problem, not a filter problem. "
              "Nothing can be ranked or written until at least some symbols "
              "return price history.")
        return [], [], watchlist

    if STRICT_MODE:
        eligible = [r for r in rows if not r["hard_fail"]]
        dropped  = [r for r in rows if r["hard_fail"]]
        if not eligible:
            print("[build_report] STRICT_MODE=True and every scanned stock failed "
                  "a hard gate today — 0 stocks will be written. Set STRICT_MODE=False "
                  "to always get a ranked list of the best available candidates instead.")
    else:
        eligible = rows
        dropped  = []

    shortlisted = sorted(eligible, key=lambda x: x["combined_score"], reverse=True)[:TOP_N]

    if verbose:
        print(f"\n[build_report] {len(rows)} scanned with data, {len(eligible)} eligible "
              f"(STRICT_MODE={STRICT_MODE}), {len(shortlisted)} shortlisted (TOP_N={TOP_N})\n")
        print("--- Shortlisted (ranked) ---")
        for i, r in enumerate(shortlisted, 1):
            flag = "✅" if r["meets_all_filters"] else "⚠️ "
            breakout = "🚀20D-HIGH" if r.get("crosses_20d_high") else ""
            deliv_spike = "📦2x-DELIVERY" if r.get("delivery_volume_spike") else ""
            turn_spike = "💰2x-TURNOVER" if r.get("turnover_spike") else ""
            z_spike = "📈Z>2" if r.get("volume_zscore_spike") else ""
            print(f"{i:>2}. {flag} {r['symbol']:<14} score={r['combined_score']:<6} "
                  f"close={r.get('last_close')}  sector={r.get('sector_label')}  "
                  f"quadrant={r.get('sector_quadrant')}  {breakout} {deliv_spike} {turn_spike} {z_spike}")
            if r["filter_notes"]:
                print(f"       notes: {'; '.join(r['filter_notes'])}")
        if dropped:
            print("\n--- Dropped by STRICT_MODE hard gates ---")
            for r in dropped:
                print(f"  {r['symbol']:<14} {'; '.join(r['filter_notes'])}")
        if no_history:
            print("\n--- No price history (excluded before scoring) ---")
            for sym in no_history:
                print(f"  {sym}")

    return shortlisted, dropped, watchlist


# ────────────────── SUMMARY TAB ──────────────────────────────

def update_summary_tab(spreadsheet, tab_name, count, scanned, status="OK", note=""):
    """Keeps a running Summary tab with today's metadata (or a failure note)."""
    try:
        try:
            ws = spreadsheet.worksheet("Summary")
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title="Summary", rows=500, cols=6)
            ws.append_row(["Date", "Stocks Scanned", "Shortlisted", "Tab Name", "Status", "Note"],
                          value_input_option="USER_ENTERED")
            ws.format("A1:F1", {"textFormat": {"bold": True}})
            ws.freeze(rows=1)
        ws.append_row(
            [datetime.now(IST).strftime("%d-%b-%Y %H:%M IST"), scanned, count, tab_name, status, note],
            value_input_option="USER_ENTERED"
        )
    except Exception as e:
        print(f"[sheets] Summary tab update failed: {e}")


# ─────────────────────── MAIN RUN ────────────────────────────

def run_once(dry_run=False):
    print(f"[{datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}] Starting daily scan"
          f"{' (DRY RUN — no Sheets writes)' if dry_run else ''}...")

    try:
        shortlisted, rejections, watchlist_used = build_report(verbose=dry_run)
    except Exception as e:
        print(f"[run_once] Report build failed: {e}")
        traceback.print_exc()
        if not dry_run:
            try:
                client = get_gspread_client()
                spreadsheet = client.open_by_key(get_sheet_id())
                # We may have failed before the watchlist was even fetched
                # (e.g. inside build_report before it returns), so fall
                # back to the fallback list's length rather than crashing
                # this failure-logging path itself.
                update_summary_tab(spreadsheet, "-", 0, len(FALLBACK_WATCHLIST), status="FAILED", note=str(e))
            except Exception as inner:
                print(f"[run_once] Could not even log failure to Summary tab: {inner}")
        return

    scanned = len(watchlist_used)

    if dry_run:
        print("\n[dry-run] Skipping Google Sheets write.")
        return

    if not shortlisted:
        print("No stocks passed filters today — nothing written to sheet.")
        try:
            client = get_gspread_client()
            spreadsheet = client.open_by_key(get_sheet_id())
            update_summary_tab(spreadsheet, "-", 0, scanned, status="OK", note="no stocks passed filters")
        except Exception as e:
            print(f"[run_once] Could not log empty-result run: {e}")
        return

    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(get_sheet_id())
        tab_name = write_to_sheets(shortlisted)
        update_summary_tab(spreadsheet, tab_name, len(shortlisted), scanned, status="OK")
        print(f"Done. {len(shortlisted)} stock(s) written to Google Sheet tab '{tab_name}'.")
    except Exception as e:
        print(f"[run_once] Sheets write failed: {e}")
        traceback.print_exc()
        try:
            client = get_gspread_client()
            spreadsheet = client.open_by_key(get_sheet_id())
            update_summary_tab(spreadsheet, "-", len(shortlisted), scanned, status="FAILED", note=str(e))
        except Exception as inner:
            print(f"[run_once] Could not log Sheets-write failure: {inner}")


# ──────────────────── SCHEDULER ──────────────────────────────

def run_scheduler():
    print(f"Scheduler active. Will run Mon-Fri at "
          f"{SCHEDULED_HOUR:02d}:{SCHEDULED_MINUTE:02d} IST "
          f"(after NSE market close at 15:30 IST), auto-refreshing the "
          f"stock watchlist and delivery-volume data on every run.")
    last_date = None
    while True:
        now = datetime.now(IST)
        target_reached = (
            now.weekday() < 5
            and (now.hour > SCHEDULED_HOUR
                 or (now.hour == SCHEDULED_HOUR and now.minute >= SCHEDULED_MINUTE))
        )
        if target_reached and last_date != now.date():
            try:
                run_once()
            except Exception as e:
                print(f"Run failed: {e}")
                traceback.print_exc()
            last_date = now.date()
        time.sleep(30)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--now",      action="store_true", help="Run once immediately")
    p.add_argument("--schedule", action="store_true", help="Run on Mon-Fri, after market close (see SCHEDULED_HOUR/MINUTE)")
    p.add_argument("--dry-run",  action="store_true", help="Run once, print results, skip Google Sheets entirely")
    args = p.parse_args()

    if args.dry_run:
        run_once(dry_run=True)
    elif args.now:
        run_once()
    else:
        run_scheduler()
