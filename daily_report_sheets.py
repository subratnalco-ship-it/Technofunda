"""
Daily Stock Recommendation Agent → Google Sheets
-------------------------------------------------
Combines:
  1. Technical signals (RSI, MA crossover, volume spike, 200-DMA gate,
     20-day high breakout)
  2. Fundamental analysis (P/E, ROE, D/E, margins, revenue growth)
  3. DCF intrinsic value model (skipped for banks/financials, averaged over
     multiple years, with guardrails on the discount/terminal-growth spread)
  4. News headline sentiment (VADER, local — no external API cost)
  5. Analyst consensus + broker recommendation trend (Yahoo aggregated,
     treated as "soft" signals since NSE coverage is sparse)
  6. Sector rotation (RRG-style quadrant vs Nifty 500 benchmark)

Output: writes a dated tab inside a Google Sheet every weekday morning.
        Old tabs are kept so you have a rolling history.

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
  python3 daily_report_sheets.py --schedule         # persistent 8 AM IST
                                                    # weekday loop

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
- analyze_technicals() now also checks whether today's close crosses
  above the highest close of the prior 20 trading days (a classic
  breakout / buying trigger). Exposed as "crosses_20d_high" and
  "high_20d_prev" on the technical result dict.
- This is added as an ADDITIONAL scoring input alongside the existing
  bullish_cross / uptrend / healthy_rsi / vol_spike factors — it does
  NOT replace or alter any of them, and it is not wired in as a new
  hard/soft filter gate, so every previously existing condition
  (200-DMA gate, fundamental score gate, analyst/sentiment/broker soft
  gates, STRICT_MODE behavior, etc.) behaves exactly as before.
- New "20D High Breakout" column added to the Google Sheet output so
  the signal is visible per-row, right after the "Above 200-DMA" column.
"""


import os, sys, time, json, argparse, traceback
import pandas as pd
import yfinance as yf
from datetime import datetime
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

WATCHLIST = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
    "LT.NS","SBIN.NS","BHARTIARTL.NS","ITC.NS","AXISBANK.NS",
    "KOTAKBANK.NS","MARUTI.NS","TATAMOTORS.NS","SUNPHARMA.NS","TITAN.NS",
    "BAJFINANCE.NS","ADANIENT.NS","ULTRACEMCO.NS","WIPRO.NS","HCLTECH.NS",
]

TOP_N                   = 10    # rows written to the sheet
LOOKBACK_DAYS           = 400
REQUEST_PAUSE_SEC       = 0.3   # be polite to Yahoo's unofficial endpoints
FETCH_RETRIES           = 3     # retries for transient yfinance/network failures
FETCH_BACKOFF_SEC       = 1.5   # base backoff, doubles each retry
HIGH_20D_LOOKBACK       = 20    # window for the 20-day-high breakout signal

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
SCHEDULED_HOUR, SCHEDULED_MINUTE = 8, 0

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


def get_or_create_tab(spreadsheet, tab_name):
    """Return existing worksheet or create a new one with the standard headers."""
    try:
        ws = spreadsheet.worksheet(tab_name)
        print(f"[sheets] Using existing tab: {tab_name}")
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

        score = sum([bullish_cross * 2, uptrend, healthy_rsi, vol_spike, crosses_20d_high])
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
# NOTE: the 20-day high breakout signal is intentionally NOT wired in
# here as a new gate. It only feeds into combined_score in
# build_report(), exactly like volume_spike/healthy_rsi/etc already
# did — so every existing hard/soft condition below is unchanged.

def evaluate_filters(r):
    notes = []
    hard_fail = False

    if REQUIRE_ABOVE_200DMA and not r.get("above_200dma"):
        notes.append("below 200-DMA (hard gate)")
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

def build_report(verbose=False):
    rows = []
    no_history = []   # symbols we couldn't even get price data for

    diag = {"scanned": 0, "history_ok": 0, "info_ok": 0}

    for sym in WATCHLIST:
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
        return [], []

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
            print(f"{i:>2}. {flag} {r['symbol']:<14} score={r['combined_score']:<6} "
                  f"close={r.get('last_close')}  sector={r.get('sector_label')}  "
                  f"quadrant={r.get('sector_quadrant')}  {breakout}")
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

    return shortlisted, dropped


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
        shortlisted, rejections = build_report(verbose=dry_run)
    except Exception as e:
        print(f"[run_once] Report build failed: {e}")
        traceback.print_exc()
        if not dry_run:
            try:
                client = get_gspread_client()
                spreadsheet = client.open_by_key(get_sheet_id())
                update_summary_tab(spreadsheet, "-", 0, len(WATCHLIST), status="FAILED", note=str(e))
            except Exception as inner:
                print(f"[run_once] Could not even log failure to Summary tab: {inner}")
        return

    scanned = len(WATCHLIST)

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
          f"{SCHEDULED_HOUR:02d}:{SCHEDULED_MINUTE:02d} IST.")
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
    p.add_argument("--schedule", action="store_true", help="Run on Mon-Fri 8AM IST loop")
    p.add_argument("--dry-run",  action="store_true", help="Run once, print results, skip Google Sheets entirely")
    args = p.parse_args()

    if args.dry_run:
        run_once(dry_run=True)
    elif args.now:
        run_once()
    else:
        run_scheduler()
