"""
Daily Stock Recommendation Agent → Google Sheets
-------------------------------------------------
Combines:
  1. Technical signals (RSI, MA crossover, volume spike, 200-DMA gate)
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

# ── Shortlist gate ──
# "Hard" gates disqualify a stock outright. "Soft" gates only apply a
# scoring penalty/bonus and are skipped (not penalized) when data is
# missing, since Yahoo's NSE analyst/broker coverage is often absent.
REQUIRE_ABOVE_200DMA      = True     # hard gate
MIN_FUNDAMENTAL_SCORE     = 4        # hard gate, out of 10
ACCEPTED_ANALYST_RATINGS  = {"buy", "strong_buy"}  # soft gate (see passes_filters)
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
    "Fundamental /10","P/E","ROE %","Profit Margin %","Revenue Growth %","D/E",
    "DCF Intrinsic Value ₹","vs CMP %",
    "News Sentiment","Sentiment Score",
    "Analyst Rating","Analyst Avg Target ₹",
    "Broker Buy %","Broker Analysts",
    "Sector","Sector Index","Quadrant","RS Change %",
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
    ]


def write_to_sheets(rows):
    """Open the spreadsheet, create today's tab if needed, and append rows."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(get_sheet_id())

    today_tab  = datetime.now(IST).strftime("%d-%b-%Y")
    history_ws = get_or_create_tab(spreadsheet, "History")   # rolling log
    today_ws   = get_or_create_tab(spreadsheet, today_tab)   # daily snapshot

    # ── Write to today's tab (clear first so re-runs overwrite cleanly) ──
    if today_ws.row_count > 1:
        today_ws.delete_rows(2, max(2, today_ws.row_count))

    sheet_rows = [row_to_sheet_values(i + 1, r) for i, r in enumerate(rows)]
    if sheet_rows:
        today_ws.append_rows(sheet_rows, value_input_option="USER_ENTERED")
        color_by_quadrant(today_ws, sheet_rows)

    # ── Also append to the History tab ──
    history_ws.append_rows(sheet_rows, value_input_option="USER_ENTERED")

    print(f"[sheets] Wrote {len(sheet_rows)} row(s) to '{today_tab}' tab and History.")
    return today_tab


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
        hist = ticker.history(period=f"{LOOKBACK_DAYS}d")
        if hist.empty or len(hist) < 205:
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
        cf = ticker.cashflow
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
        items = ticker.news or []
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
        trend = ticker.recommendations
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
# HARD gates always apply and always disqualify.
# SOFT gates only disqualify when the data is actually present and
# unfavorable — missing data (very common for NSE analyst/broker
# coverage on Yahoo) is treated as "no opinion", not "fail".
#
# passes_filters returns (passed: bool, reasons: list[str]) so a
# --dry-run can show exactly why each stock was kept or dropped.

def passes_filters(r):
    reasons = []

    if REQUIRE_ABOVE_200DMA and not r.get("above_200dma"):
        reasons.append("below 200-DMA (hard gate)")

    if r.get("fundamental_score", 0) < MIN_FUNDAMENTAL_SCORE:
        reasons.append(f"fundamental score {r.get('fundamental_score', 0)} < {MIN_FUNDAMENTAL_SCORE} (hard gate)")

    rec = r.get("recommendation")
    if rec not in (None, "n/a") and rec not in ACCEPTED_ANALYST_RATINGS:
        reasons.append(f"analyst rating '{rec}' not in {ACCEPTED_ANALYST_RATINGS} (soft gate)")

    sentiment = r.get("sentiment_score", 0)
    if r.get("headline_count", 0) > 0 and sentiment < MIN_SENTIMENT_SCORE:
        reasons.append(f"sentiment {sentiment} < {MIN_SENTIMENT_SCORE} (soft gate)")

    br = r.get("broker_buy_ratio")
    if br is not None and br < MIN_BROKER_BUY_RATIO:
        reasons.append(f"broker buy ratio {br} < {MIN_BROKER_BUY_RATIO} (soft gate)")

    if EXCLUDE_LAGGING and r.get("sector_quadrant") == "Lagging":
        reasons.append("sector quadrant is Lagging (soft gate)")

    return (len(reasons) == 0, reasons)


# ────────────────────── BUILD REPORT ─────────────────────────

def build_report(verbose=False):
    rows = []
    rejections = []  # (symbol, reasons) for --dry-run visibility

    for sym in WATCHLIST:
        ticker = yf.Ticker(sym)

        tech = analyze_technicals(sym, ticker)
        if not tech:
            rejections.append((sym, ["insufficient price history"]))
            continue

        try:
            info = ticker.info or {}
        except Exception as e:
            print(f"[info] {sym}: {e}")
            info = {}

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

        rows.append(tech)
        time.sleep(REQUEST_PAUSE_SEC)

    if not rows:
        if verbose:
            print("[build_report] No stocks had usable technical data at all.")
        return [], rejections

    scored_with_filters = [(r, *passes_filters(r)) for r in rows]
    passed    = [r for (r, ok, _) in scored_with_filters if ok]
    for r, ok, reasons in scored_with_filters:
        if not ok:
            rejections.append((r["symbol"], reasons))

    shortlisted = sorted(passed, key=lambda x: x["combined_score"], reverse=True)[:TOP_N]

    if verbose:
        print(f"\n[build_report] {len(rows)} scanned, {len(passed)} passed filters, "
              f"{len(shortlisted)} shortlisted (TOP_N={TOP_N})\n")
        print("--- Shortlisted ---")
        for i, r in enumerate(shortlisted, 1):
            print(f"{i:>2}. {r['symbol']:<14} score={r['combined_score']:<6} "
                  f"close={r.get('last_close')}  sector={r.get('sector_label')}  "
                  f"quadrant={r.get('sector_quadrant')}")
        print("\n--- Rejected ---")
        for sym, reasons in rejections:
            print(f"  {sym:<14} {'; '.join(reasons)}")

    return shortlisted, rejections


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
