"""
Daily Stock Recommendation Agent → Google Sheets
-------------------------------------------------
Combines:
  1. Technical signals (RSI, MA crossover, volume spike, 200-DMA gate)
  2. Fundamental analysis (P/E, ROE, D/E, margins, revenue growth)
  3. DCF intrinsic value model
  4. News headline sentiment (VADER, local — no external API cost)
  5. Analyst consensus + broker recommendation trend (Yahoo aggregated)
  6. Sector rotation (RRG-style quadrant vs Nifty 500 benchmark)

Output: writes a dated tab inside a Google Sheet every weekday morning.
        Old tabs are kept so you have a rolling history.

SETUP  (full step-by-step guide is in the README — read that first)
-----
  pip install yfinance pandas requests vaderSentiment gspread google-auth tzdata

Environment variables required:
  GOOGLE_CREDENTIALS_JSON   <- contents of your service-account JSON key file
  GOOGLE_SHEET_ID           <- the long ID from your Google Sheet URL

RUNNING
-------
  python3 daily_report_sheets.py --now        # run once immediately
  python3 daily_report_sheets.py --schedule   # persistent 8 AM IST weekday loop
"""

import os, io, sys, time, json, argparse
import requests
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
GOOGLE_SHEET_ID         = os.environ.get("GOOGLE_SHEET_ID", "PASTE_SHEET_ID_HERE")

WATCHLIST = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
    "LT.NS","SBIN.NS","BHARTIARTL.NS","ITC.NS","AXISBANK.NS",
    "KOTAKBANK.NS","MARUTI.NS","TATAMOTORS.NS","SUNPHARMA.NS","TITAN.NS",
    "BAJFINANCE.NS","ADANIENT.NS","ULTRACEMCO.NS","WIPRO.NS","HCLTECH.NS",
]

TOP_N                   = 10    # rows written to the sheet
LOOKBACK_DAYS           = 400

# ── Shortlist gate (set to False to disable individual filters) ──
REQUIRE_ABOVE_200DMA    = True
MIN_FUNDAMENTAL_SCORE   = 4     # out of 10 (relaxed slightly vs Telegram version)
ACCEPTED_ANALYST_RATINGS= {"buy","strong_buy"}
MIN_SENTIMENT_SCORE     = -0.05 # allow mildly neutral news
MIN_BROKER_BUY_RATIO    = 0.40

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


def get_or_create_tab(spreadsheet, tab_name):
    """Return existing worksheet or create a new one with the standard headers."""
    try:
        ws = spreadsheet.worksheet(tab_name)
        print(f"[sheets] Using existing tab: {tab_name}")
        return ws
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=200, cols=len(HEADERS))
        ws.append_row(HEADERS, value_input_option="USER_ENTERED")
        # Freeze header row and bold it
        ws.format("A1:AD1", {"textFormat": {"bold": True}})
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
        r["symbol"].replace(".NS",""),
        datetime.now(IST).strftime("%d-%b-%Y"),
        r.get("combined_score",""),
        r.get("entry",""),
        r.get("stop_loss",""),
        r.get("target",""),
        r.get("last_close",""),
        r.get("rsi",""),
        "Up" if r.get("uptrend") else "Flat/Down",
        r.get("ma200",""),
        "Yes ✅" if r.get("above_200dma") else "No ❌",
        r.get("fundamental_score",""),
        round(r["pe"],1) if r.get("pe") else "",
        f"{r['roe']*100:.1f}" if r.get("roe") else "",
        f"{r['profit_margin']*100:.1f}" if r.get("profit_margin") else "",
        f"{r['revenue_growth']*100:.1f}" if r.get("revenue_growth") else "",
        round(r["debt_to_equity"],1) if r.get("debt_to_equity") else "",
        r.get("intrinsic_value",""),
        vs_cmp,
        "Positive" if r.get("sentiment_score",0)>0.15 else "Negative" if r.get("sentiment_score",0)<-0.15 else "Neutral",
        r.get("sentiment_score",""),
        r.get("recommendation","n/a").replace("_"," ").title(),
        round(r["target_mean"],0) if r.get("target_mean") else "",
        f"{int(r['broker_buy_ratio']*100)}%" if r.get("broker_buy_ratio") is not None else "",
        r.get("broker_total",""),
        r.get("sector_label",""),
        r.get("sector_index",""),
        r.get("sector_quadrant",""),
        r.get("sector_rs_change_pct",""),
    ]


def write_to_sheets(rows):
    """Open the spreadsheet, create today's tab if needed, and append rows."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    today_tab  = datetime.now(IST).strftime("%d-%b-%Y")
    history_ws = get_or_create_tab(spreadsheet, "History")   # rolling log
    today_ws   = get_or_create_tab(spreadsheet, today_tab)   # daily snapshot

    # ── Write to today's tab (clear first so re-runs overwrite cleanly) ──
    if today_ws.row_count > 1:
        today_ws.delete_rows(2, max(2, today_ws.row_count))

    sheet_rows = [row_to_sheet_values(i+1, r) for i, r in enumerate(rows)]
    if sheet_rows:
        today_ws.append_rows(sheet_rows, value_input_option="USER_ENTERED")
        color_by_quadrant(today_ws, sheet_rows)

    # ── Also append to the History tab ──
    history_ws.append_rows(sheet_rows, value_input_option="USER_ENTERED")

    print(f"[sheets] Wrote {len(sheet_rows)} row(s) to '{today_tab}' tab and History.")
    return today_tab


def color_by_quadrant(ws, sheet_rows):
    """
    Lightly colour-code the Quadrant column so Leading = green,
    Improving = blue, Weakening = yellow, Lagging = red.
    Column index of 'Quadrant' in HEADERS (1-based for Sheets API).
    """
    quadrant_col = HEADERS.index("Quadrant") + 1
    color_map = {
        "Leading":   {"red":0.72,"green":0.96,"blue":0.72},
        "Improving": {"red":0.68,"green":0.85,"blue":0.96},
        "Weakening": {"red":1.00,"green":0.95,"blue":0.60},
        "Lagging":   {"red":1.00,"green":0.70,"blue":0.70},
    }
    for i, row in enumerate(sheet_rows):
        quadrant = row[HEADERS.index("Quadrant")]
        bg = color_map.get(quadrant)
        if bg:
            cell = gspread.utils.rowcol_to_a1(i + 2, quadrant_col)
            ws.format(cell, {"backgroundColor": bg})


# ────────────────────── TECHNICALS ───────────────────────────

def compute_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_g = gain.rolling(period).mean()
    avg_l = loss.rolling(period).mean()
    rs    = avg_g / avg_l.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))


def analyze_technicals(symbol):
    try:
        hist = yf.Ticker(symbol).history(period=f"{LOOKBACK_DAYS}d")
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

        score = sum([bullish_cross*2, uptrend, healthy_rsi, vol_spike])
        atr   = (hist["High"]-hist["Low"]).rolling(14).mean().iloc[-1]
        rlow  = close.rolling(20).min().iloc[-1]

        return {
            "symbol": symbol, "score": score,
            "last_close": round(lc,2), "rsi": round(rsi.iloc[-1],1),
            "uptrend": uptrend, "volume_spike": vol_spike,
            "entry": round(lc,2),
            "stop_loss": round(min(rlow, lc-1.5*atr),2),
            "target": round(lc+2.5*atr,2),
            "ma200": round(l200,2) if pd.notna(l200) else None,
            "above_200dma": above200,
        }
    except Exception as e:
        print(f"[technical] {symbol}: {e}")
        return None


# ─────────────────────── ANALYST ─────────────────────────────

def analyst_consensus(symbol):
    try:
        info = yf.Ticker(symbol).info
        return {
            "recommendation": info.get("recommendationKey","n/a"),
            "target_mean":    info.get("targetMeanPrice"),
            "num_analysts":   info.get("numberOfAnalystOpinions"),
        }
    except Exception as e:
        print(f"[analyst] {symbol}: {e}")
        return {"recommendation":"n/a","target_mean":None,"num_analysts":None}


# ─────────────────────── FUNDAMENTALS ────────────────────────

def fundamental_score(symbol):
    try:
        info = yf.Ticker(symbol).info
        pe   = info.get("trailingPE")
        roe  = info.get("returnOnEquity")
        de   = info.get("debtToEquity")
        pm   = info.get("profitMargins")
        rg   = info.get("revenueGrowth")
        cr   = info.get("currentRatio")

        score = 0
        if pe  and 0 < pe  <= 30:  score += 2
        if roe and roe > 0.15:      score += 2
        if de  and de  < 100:       score += 2
        if pm  and pm  > 0.10:      score += 2
        if rg  and rg  > 0.05:      score += 1
        if cr  and cr  > 1:         score += 1

        return {
            "fundamental_score": score,
            "pe": pe, "roe": roe, "debt_to_equity": de,
            "profit_margin": pm, "revenue_growth": rg, "current_ratio": cr,
        }
    except Exception as e:
        print(f"[fundamental] {symbol}: {e}")
        return {"fundamental_score":0}


# ──────────────────────── DCF ────────────────────────────────

def dcf_intrinsic_value(symbol, g=0.10, r=0.12, tg=0.04, years=5):
    try:
        t   = yf.Ticker(symbol)
        cf  = t.cashflow
        so  = t.info.get("sharesOutstanding")
        if cf is None or cf.empty or not so:
            return {"intrinsic_value": None}

        opcf_r = next((x for x in cf.index if "Operating Cash Flow" in x), None)
        capx_r = next((x for x in cf.index if "Capital Expenditure"  in x), None)
        if not opcf_r or not capx_r:
            return {"intrinsic_value": None}

        fcf = cf.loc[opcf_r].iloc[0] + cf.loc[capx_r].iloc[0]
        if not fcf or fcf <= 0:
            return {"intrinsic_value": None}

        pv, proj = 0, fcf
        for yr in range(1, years+1):
            proj *= (1+g)
            pv   += proj / (1+r)**yr
        tv  = proj*(1+tg)/(r-tg)
        pv += tv/(1+r)**years
        return {"intrinsic_value": round(pv/so, 2)}
    except Exception as e:
        print(f"[dcf] {symbol}: {e}")
        return {"intrinsic_value": None}


# ─────────────────────── SENTIMENT ───────────────────────────

def news_sentiment(symbol, n=8):
    try:
        items = yf.Ticker(symbol).news or []
        heads = []
        for it in items[:n]:
            t = it.get("content",{}).get("title") or it.get("title")
            if t: heads.append(t)
        if not heads:
            return {"sentiment_score":0.0,"headline_count":0}
        scores = [_sentiment_analyzer.polarity_scores(h)["compound"] for h in heads]
        return {"sentiment_score":round(sum(scores)/len(scores),3),"headline_count":len(heads)}
    except Exception as e:
        print(f"[sentiment] {symbol}: {e}")
        return {"sentiment_score":0.0,"headline_count":0}


# ─────────────────────── BROKER TREND ────────────────────────

def broker_recommendation_trend(symbol):
    try:
        trend = yf.Ticker(symbol).recommendations
        if trend is None or trend.empty:
            return {"broker_buy_ratio":None,"broker_total":0}
        l = trend.iloc[0]
        sb,b,h,s,ss = (l.get(k,0) for k in ("strongBuy","buy","hold","sell","strongSell"))
        total = sb+b+h+s+ss
        if not total:
            return {"broker_buy_ratio":None,"broker_total":0}
        return {"broker_buy_ratio":round((sb+b)/total,2),"broker_total":int(total)}
    except Exception as e:
        print(f"[broker] {symbol}: {e}")
        return {"broker_buy_ratio":None,"broker_total":0}


# ─────────────────── SECTOR ROTATION ─────────────────────────

_rotation_cache = None

def _fetch_close(ticker, days=250):
    try:
        h = yf.Ticker(ticker).history(period=f"{days}d")
        return h["Close"] if not h.empty else None
    except:
        return None


def compute_sector_rotation():
    bench = _fetch_close(SECTOR_BENCHMARK)
    result = {}
    if bench is None or len(bench) < RS_WINDOW+RS_MOMENTUM_WINDOW:
        return {n:{"quadrant":"Unknown","rs_ratio":None,"rs_change_pct":None}
                for n in SECTOR_INDEX_TICKERS}
    for name, ticker in SECTOR_INDEX_TICKERS.items():
        sc = _fetch_close(ticker)
        if sc is None:
            result[name]={"quadrant":"Unknown","rs_ratio":None,"rs_change_pct":None}
            continue
        combined = pd.concat([sc, bench], axis=1, join="inner")
        combined.columns = ["s","b"]
        if len(combined) < RS_WINDOW+RS_MOMENTUM_WINDOW:
            result[name]={"quadrant":"Unknown","rs_ratio":None,"rs_change_pct":None}
            continue
        rs_ratio = (combined["s"]/combined["b"]) / ((combined["s"]/combined["b"]).rolling(RS_WINDOW).mean())
        rl, rp = rs_ratio.iloc[-1], rs_ratio.iloc[-1-RS_MOMENTUM_WINDOW]
        if pd.isna(rl) or pd.isna(rp):
            result[name]={"quadrant":"Unknown","rs_ratio":None,"rs_change_pct":None}
            continue
        rising = rl > rp
        outper = rl > 1.0
        q = ("Leading" if outper and rising else "Weakening" if outper else
             "Lagging" if not outper and not rising else "Improving")
        result[name]={"quadrant":q,"rs_ratio":round(float(rl),4),"rs_change_pct":round(((rl/rp)-1)*100,2)}
    return result


def get_rotation():
    global _rotation_cache
    if _rotation_cache is None:
        _rotation_cache = compute_sector_rotation()
    return _rotation_cache


def stock_sector_rotation(symbol):
    try:
        info = yf.Ticker(symbol).info
        yf_sector   = info.get("sector")
        yf_industry = (info.get("industry") or "").lower()
    except:
        yf_sector, yf_industry = None, ""

    nse_idx = YFINANCE_SECTOR_TO_NSE.get(yf_sector)
    if nse_idx == "Bank":
        if "public" in yf_industry or "psu" in yf_industry: nse_idx = "PSU Bank"
        elif "bank" not in yf_industry: nse_idx = "Financial Services"

    if nse_idx is None:
        return {"sector_label":yf_sector or "Unknown","sector_index":None,
                "sector_quadrant":"Unknown","sector_rs_change_pct":None,
                "sector_score_adj":0.0}

    data = get_rotation().get(nse_idx, {"quadrant":"Unknown","rs_change_pct":None})
    q    = data["quadrant"]
    return {"sector_label":yf_sector or nse_idx,"sector_index":nse_idx,
            "sector_quadrant":q,"sector_rs_change_pct":data.get("rs_change_pct"),
            "sector_score_adj":SECTOR_SCORE_ADJ.get(q,0.0)}


# ─────────────────────── FILTERS ─────────────────────────────

def passes_filters(r):
    if REQUIRE_ABOVE_200DMA and not r.get("above_200dma"):          return False
    if r.get("fundamental_score",0) < MIN_FUNDAMENTAL_SCORE:        return False
    if r.get("recommendation") not in ACCEPTED_ANALYST_RATINGS:     return False
    if r.get("sentiment_score",0) < MIN_SENTIMENT_SCORE:            return False
    br = r.get("broker_buy_ratio")
    if br is None or br < MIN_BROKER_BUY_RATIO:                     return False
    if EXCLUDE_LAGGING and r.get("sector_quadrant") == "Lagging":   return False
    return True


# ────────────────────── BUILD REPORT ─────────────────────────

def build_report():
    rows = []
    for sym in WATCHLIST:
        tech = analyze_technicals(sym)
        if not tech: continue
        for fn in (analyst_consensus, fundamental_score, dcf_intrinsic_value,
                   news_sentiment, broker_recommendation_trend, stock_sector_rotation):
            tech.update(fn(sym))
        # Combined score
        c = tech["score"]
        c += tech.get("fundamental_score",0) * 0.5
        c += tech.get("sentiment_score",0)   * 2
        br = tech.get("broker_buy_ratio")
        if br is not None: c += br * 2
        iv, lc = tech.get("intrinsic_value"), tech.get("last_close",0)
        if iv and lc and (iv-lc)/lc > 0.15: c += 2
        if tech.get("recommendation") in ("buy","strong_buy") and tech.get("uptrend"): c += 2
        c += tech.get("sector_score_adj",0)
        tech["combined_score"] = round(c,2)
        rows.append(tech)
        time.sleep(0.3)

    if not rows:
        return []

    shortlisted = sorted(
        [r for r in rows if passes_filters(r)],
        key=lambda x: x["combined_score"], reverse=True
    )[:TOP_N]

    return shortlisted


# ────────────────── SUMMARY TAB ──────────────────────────────

def update_summary_tab(spreadsheet, tab_name, count, scanned):
    """Keeps a running Summary tab with today's metadata."""
    try:
        try:
            ws = spreadsheet.worksheet("Summary")
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title="Summary", rows=500, cols=4)
            ws.append_row(["Date","Stocks Scanned","Shortlisted","Tab Name"],
                          value_input_option="USER_ENTERED")
            ws.format("A1:D1", {"textFormat":{"bold":True}})
            ws.freeze(rows=1)
        ws.append_row(
            [datetime.now(IST).strftime("%d-%b-%Y %H:%M IST"), scanned, count, tab_name],
            value_input_option="USER_ENTERED"
        )
    except Exception as e:
        print(f"[sheets] Summary tab update failed: {e}")


# ─────────────────────── MAIN RUN ────────────────────────────

def run_once():
    print(f"[{datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}] Starting daily scan...")
    shortlisted = build_report()
    scanned     = len(WATCHLIST)

    if not shortlisted:
        print("No stocks passed filters today — nothing written to sheet.")
        return

    client      = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    tab_name    = write_to_sheets(shortlisted)
    update_summary_tab(spreadsheet, tab_name, len(shortlisted), scanned)
    print(f"Done. {len(shortlisted)} stock(s) written to Google Sheet tab '{tab_name}'.")


# ──────────────────── SCHEDULER ──────────────────────────────

def run_scheduler():
    print(f"Scheduler active. Will run Mon-Fri at "
          f"{SCHEDULED_HOUR:02d}:{SCHEDULED_MINUTE:02d} IST.")
    last_date = None
    while True:
        now = datetime.now(IST)
        if (now.weekday() < 5 and now.hour == SCHEDULED_HOUR
                and now.minute == SCHEDULED_MINUTE and last_date != now.date()):
            try:
                run_once()
            except Exception as e:
                print(f"Run failed: {e}")
            last_date = now.date()
        time.sleep(30)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--now",      action="store_true", help="Run once immediately")
    p.add_argument("--schedule", action="store_true", help="Run on Mon-Fri 8AM IST loop")
    args = p.parse_args()
    run_once() if args.now else run_scheduler()
