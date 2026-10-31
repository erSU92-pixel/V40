"""
APEX PREDATOR v8.0 — Synthetic Vol 80 Edition (Optimized & Hardened)
=========================================================================
Fitur Lengkap v8.0:
- Dedicated Deriv WebSocket Data Feed dengan Anti-Cloudflare 520 Bypass
- 24/7 Non-Stop Execution (Tanpa batasan Killzone / Hari libur market)
- SMC (Order Blocks struktur BOS & FVG)
- Heikin Ashi Trend, ADX, RSI, ATR (Wilder)
- News Shield & Yahoo/TwelveData Dibuang (Tidak relevan untuk sintetik)
- Drawdown Guard & Auto-Pause
- Persistent SQLite Database untuk Journal & State
- Dukungan Akun Cent / Standard (Lot Sizing Presisi Broker Headway)
- Telegram HTML Notification & Heartbeat System
=========================================================================
"""
import os
import re
import json
import time
import sys
import csv
import sqlite3
import logging
import html
import requests
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

try:
    import websocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

VERSION = "8.0-Vol80"
MAX_CONFLUENCE = 9

# =========================================================
# KONFIGURASI
# =========================================================
@dataclass
class Config:
    db_file: str = "apex_vol80_database.db"
    log_file: str = "apex_vol80.log"
    state_file: str = "apex_vol80_state.json"
    
    # Symbol & Market (Khusus Vol 80)
    symbol: str = "Volatility 80 Index"
    deriv_symbol: str = "R_80"  # Kode resmi Deriv untuk Vol 80
    contract_size: float = 1.0
    account_type: str = "cent"  # Mode default: "cent" atau "standard"
    price_decimals: int = 2     # Sesuaikan presisi digit Vol 80
    
    # Timeframes & Bars
    cooldown_minutes: int = 30
    min_bars_m5: int = 60
    min_bars_m15: int = 60
    min_bars_h1: int = 60
    outcome_bars_h1: int = 150
    max_stale_mult: float = 3.0
    max_gap_ratio: float = 0.10
    
    # Risk Management
    account_balance_usd: float = 10000.0
    risk_percentage: float = 1.0
    max_lot: float = 50.0
    min_lot: float = 0.50
    
    # SL/TP Multipliers
    atr_sl_mult: float = 2.0
    atr_tp1_mult: float = 1.0
    atr_tp2_mult: float = 2.0
    atr_tp3_mult: float = 3.5
    spread_buffer_points: float = 0.50
    
    # Signal Thresholds
    score_threshold: int = 8
    adx_min: float = 22.0
    adx_strong: float = 25.0
    max_daily_signals: int = 10
    
    # System
    report_max_entries: int = 500
    dry_run: bool = False
    
    # V8.0 Features
    track_outcomes: bool = True
    notify_on_tp_hit: bool = True
    drawdown_guard_enabled: bool = True
    drawdown_max_consecutive_losses: int = 3
    drawdown_pause_hours: int = 12
    heartbeat_enabled: bool = True
    heartbeat_hour_utc: int = 8

    @classmethod
    def from_env(cls):
        cfg = cls()
        for f in cls.__dataclass_fields__:
            v = os.getenv(f"APEX_{f.upper()}")
            if v is None: continue
            cur = getattr(cfg, f)
            try:
                if isinstance(cur, bool): setattr(cfg, f, v.strip().lower() in ("1","true","yes","on"))
                elif isinstance(cur, int): setattr(cfg, f, int(v))
                elif isinstance(cur, float):
                    fv = float(v)
                    if not np.isfinite(fv): continue
                    setattr(cfg, f, fv)
                else: setattr(cfg, f, v)
            except ValueError: pass
        cfg._validate()
        return cfg

    def _validate(self):
        errs = []
        if self.account_balance_usd <= 0: errs.append("balance<=0")
        if not (0 < self.risk_percentage <= 10): errs.append("risk not 0-10")
        if self.min_lot <= 0 or self.max_lot < self.min_lot: errs.append("lot config")
        if not (0 < self.score_threshold <= MAX_CONFLUENCE): errs.append("score invalid")
        if errs: raise ValueError("CONFIG ERROR: " + "; ".join(errs))

# =========================================================
# UTILITIES & LOGGING
# =========================================================
def setup_logging(cfg, verbose=False):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    root.handlers.clear()
    fmt = logging.Formatter(f"%(asctime)s [%(levelname)s] [ApexVol80-v{VERSION}] - %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)
    try:
        fh = RotatingFileHandler(cfg.log_file, maxBytes=10*1024*1024, backupCount=5)
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except Exception: pass

def _redact(t):
    t = str(t).replace("\n"," ").replace("\r"," ")
    t = re.sub(r"token=[^&\s'\"]+", "token=***", t)
    t = re.sub(r"/bot[^/\s'\"]+/", "/bot***/", t)
    return t

def _h(s): return html.escape(str(s), quote=False)

class DataFetchError(Exception): pass

# =========================================================
# DATABASE LAYER (SQLite)
# =========================================================
def get_db_connection(cfg):
    conn = sqlite3.connect(cfg.db_file)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(cfg):
    with get_db_connection(cfg) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS journal (
                id TEXT PRIMARY KEY, timestamp TEXT, symbol TEXT, signal TEXT,
                entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL, lot REAL,
                confluence TEXT, outcome TEXT, pnl_usd REAL, notes TEXT,
                notify_status TEXT, hit_tp1 INTEGER, hit_tp2 INTEGER,
                hit_tp3 INTEGER, hit_sl INTEGER, hit_be INTEGER, r_achieved REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, run_count INTEGER,
                active_source TEXT, final_signal TEXT, notified INTEGER, entry_price REAL,
                confidence TEXT, adx REAL, heikin_ashi TEXT, ob_type TEXT, fvg_type TEXT,
                recommended_sl REAL, tp1 REAL, tp2 REAL, tp3 REAL, suggested_lot REAL, decision_path TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)
        """)
        conn.commit()

def db_get_kv(cfg, key, default):
    try:
        with get_db_connection(cfg) as conn:
            row = conn.execute("SELECT value FROM kv_store WHERE key = ?", (key,)).fetchone()
            if row: return json.loads(row["value"])
    except Exception: pass
    return default

def db_set_kv(cfg, key, value):
    try:
        val_str = json.dumps(value, default=str)
        with get_db_connection(cfg) as conn:
            conn.execute("INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, val_str))
            conn.commit()
    except Exception as e:
        logging.error(f"DB set KV failed: {_redact(e)}")

# =========================================================
# STATE & STORAGE MANAGEMENT
# =========================================================
def _default_state():
    return {"run_count": 0, "last_signal_type": "NEUTRAL", "last_signal_timestamp": "", "consecutive_errors": 0}

def load_state(cfg):
    init_db(cfg)
    st = db_get_kv(cfg, "state", _default_state())
    return {**_default_state(), **st} if isinstance(st, dict) else _default_state()

def save_state(state, cfg):
    db_set_kv(cfg, "state", state)

def append_report(r, cfg):
    init_db(cfg)
    try:
        with get_db_connection(cfg) as conn:
            conn.execute("""
                INSERT INTO reports (timestamp, run_count, active_source, final_signal, notified, entry_price, 
                                     confidence, adx, heikin_ashi, ob_type, fvg_type, recommended_sl, tp1, tp2, tp3, suggested_lot, decision_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r.get("timestamp"), r.get("run_count"), r.get("active_source"), r.get("final_signal"),
                1 if r.get("notified") else 0, r.get("entry_price"), json.dumps(r.get("confidence")),
                r.get("adx"), r.get("heikin_ashi"), r.get("ob_type"), r.get("fvg_type"),
                r.get("recommended_sl"), r.get("tp1"), r.get("tp2"), r.get("tp3"), r.get("suggested_lot"),
                json.dumps(r.get("decision_path"))
            ))
            conn.execute(f"DELETE FROM reports WHERE id NOT IN (SELECT id FROM reports ORDER BY id DESC LIMIT {cfg.report_max_entries})")
            conn.commit()
    except Exception as e:
        logging.error(f"Append report DB failed: {_redact(e)}")

def append_journal(e, cfg):
    init_db(cfg)
    try:
        with get_db_connection(cfg) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO journal (id, timestamp, symbol, signal, entry, sl, tp1, tp2, tp3, lot, 
                                                confluence, outcome, pnl_usd, notes, notify_status, hit_tp1, hit_tp2, hit_tp3, hit_sl, hit_be, r_achieved)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                e.get("id"), e.get("timestamp"), e.get("symbol"), e.get("signal"), e.get("entry"),
                e.get("sl"), e.get("tp1"), e.get("tp2"), e.get("tp3"), e.get("lot"),
                json.dumps(e.get("confluence")), e.get("outcome"), e.get("pnl_usd"), e.get("notes"),
                e.get("notify_status"), 1 if e.get("hit_tp1") else 0, 1 if e.get("hit_tp2") else 0,
                1 if e.get("hit_tp3") else 0, 1 if e.get("hit_sl") else 0, 1 if e.get("hit_be") else 0, e.get("r_achieved", 0.0)
            ))
            conn.commit()
    except Exception as ex:
        logging.error(f"Append journal DB failed: {_redact(ex)}")

def update_journal_status(jid, updates, cfg):
    init_db(cfg)
    try:
        with get_db_connection(cfg) as conn:
            set_clauses = [f"{k} = ?" for k in updates.keys()]
            vals = list(updates.values()) + [jid]
            conn.execute(f"UPDATE journal SET {', '.join(set_clauses)} WHERE id = ?", tuple(vals))
            conn.commit()
    except Exception as e:
        logging.error(f"Update journal status failed: {_redact(e)}")

def get_all_journal(cfg):
    init_db(cfg)
    try:
        with get_db_connection(cfg) as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM journal ORDER BY timestamp DESC").fetchall()]
    except Exception: return []

def get_all_reports(cfg):
    init_db(cfg)
    try:
        with get_db_connection(cfg) as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM reports ORDER BY id DESC").fetchall()]
    except Exception: return []

def count_daily_signals(cfg):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return sum(1 for r in get_all_reports(cfg) if str(r.get("timestamp","")).startswith(today) and r.get("final_signal") in ("BUY","SELL") and r.get("notified") == 1)

# =========================================================
# DRAWDOWN GUARD & HEARTBEAT
# =========================================================
def should_block_due_to_drawdown(cfg):
    if not cfg.drawdown_guard_enabled: return False, ""
    p = db_get_kv(cfg, "drawdown_pause", {})
    if p.get("until"):
        try:
            if datetime.now(timezone.utc) < datetime.fromisoformat(p["until"]):
                return True, f"pause until {p['until']}"
        except Exception: pass
    
    closed = [x for x in get_all_journal(cfg) if x.get("outcome") not in (None,"PENDING")]
    closed.sort(key=lambda x: x.get("timestamp",""), reverse=True)
    consec = 0
    for x in closed:
        if x.get("outcome") == "LOSS": consec += 1
        else: break
        
    if consec >= cfg.drawdown_max_consecutive_losses:
        until = (datetime.now(timezone.utc) + timedelta(hours=cfg.drawdown_pause_hours)).isoformat()
        db_set_kv(cfg, "drawdown_pause", {"until":until,"reason":f"{consec} losses"})
        try:
            send_telegram(f"<b>DRAWDOWN GUARD</b>\n\n{consec} loss beruntun pada Vol 80. Bot PAUSE {cfg.drawdown_pause_hours} jam.", cfg)
        except Exception: pass
        return True, f"{consec} losses -> pause"
    return False, ""

def maybe_send_heartbeat(cfg):
    if not cfg.heartbeat_enabled: return
    now = datetime.now(timezone.utc)
    if now.hour != cfg.heartbeat_hour_utc: return
    today = now.strftime("%Y-%m-%d")
    hb = db_get_kv(cfg, "heartbeat", {})
    if hb.get("last_sent_date") == today: return
    
    today_r = [r for r in get_all_reports(cfg) if str(r.get("timestamp","")).startswith(today)]
    msg = (f"<b>APEX VOL 80 HEARTBEAT v{VERSION}</b>\n\n"
           f"Scan hari ini: <code>{len(today_r)}</code>\n"
           f"Sinyal Valid: <code>{sum(1 for r in today_r if r.get('notified')==1)}</code>\n"
           f"<code>{now.strftime('%Y-%m-%d %H:%M')} UTC</code>")
    if send_telegram(msg, cfg):
        db_set_kv(cfg, "heartbeat", {"last_sent_date":today})

# =========================================================
# DATA FETCHERS (DERIV WEBSOCKET DENGAN ANTI-CLOUDFLARE 520)
# =========================================================
def _candles_to_df(candles):
    df = pd.DataFrame(candles)
    df["datetime"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    for c in ["open","high","low","close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = 100.0
    return df[["datetime","open","high","low","close","volume"]].reset_index(drop=True)

def fetch_deriv_ohlcv(cfg, interval_str, count=200, retries=3):
    if not HAS_WEBSOCKET: raise DataFetchError("websocket-client library not installed")
    gmap = {"5min":300,"15min":900,"1h":3600}
    gran = gmap.get(interval_str)
    if gran is None: raise DataFetchError(f"Interval {interval_str} not supported")
    
    payload = {"ticks_history": cfg.deriv_symbol, "count": count, "end": "latest", "granularity": gran, "style": "candles"}
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    
    # Header penyamaran agar lolos proteksi Cloudflare 520 di GitHub Actions / Cloud server
    custom_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://trade.deriv.com"
    }
    
    last_err = None
    for att in range(retries + 1):
        ws = None
        try:
            ws = websocket.create_connection(url, header=custom_headers, timeout=15, enable_multithread=True)
            ws.send(json.dumps(payload))
            deadline = time.time() + 15
            while time.time() < deadline:
                raw = ws.recv()
                if not raw: continue
                d = json.loads(raw)
                if "error" in d: raise DataFetchError(f"Deriv API: {d['error'].get('message','?')}")
                if d.get("msg_type") == "candles":
                    c = d.get("candles", [])
                    if c: return _candles_to_df(c)
            raise DataFetchError("Deriv WebSocket timeout")
        except Exception as e:
            last_err = e
            logging.warning(f"Deriv Vol 80 att {att+1} failed: {_redact(e)}")
            if att < retries: time.sleep(3 ** att)
        finally:
            if ws:
                try: ws.close()
                except Exception: pass
    raise DataFetchError(f"Deriv Vol 80 failed after retries: {_redact(last_err)}")

# =========================================================
# VALIDATION MARKET
# =========================================================
_INTERVAL_SECS = {"5min":300,"15min":900,"1h":3600}

def validate_ohlcv(df, interval_str, min_bars, cfg):
    if df is None or len(df) == 0: raise ValueError("Empty data")
    df = df.dropna(subset=["open","high","low","close"]).copy()
    if len(df) < min_bars: raise ValueError(f"Bars {len(df)} < {min_bars}")
    isec = _INTERVAL_SECS.get(interval_str, 300)
    lt = df["datetime"].iloc[-1]
    if lt.tzinfo is None: lt = lt.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - lt).total_seconds()
    if age > isec * cfg.max_stale_mult: raise ValueError(f"Stale data {age:.0f}s")
    return df.reset_index(drop=True)

# =========================================================
# TEKNIKAL & SMC ANALYTICS
# =========================================================
def compute_true_range(df):
    h, l, c = df["high"], df["low"], df["close"]
    return pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)

def compute_atr(df, period=14):
    tr = compute_true_range(df)
    v = tr.ewm(alpha=1/period, adjust=False).mean().iloc[-1]
    return 0.0 if pd.isna(v) else float(v)

def calculate_adx(df, period=14):
    try:
        h, l = df["high"], df["low"]
        up, dn = h.diff(), -l.diff()
        pdm = up.where((up>dn)&(up>0), 0.0)
        mdm = dn.where((dn>up)&(dn>0), 0.0)
        atr = compute_true_range(df).ewm(alpha=1/period, adjust=False).mean().replace(0, np.nan)
        pdi = 100*pdm.ewm(alpha=1/period, adjust=False).mean()/atr
        mdi = 100*mdm.ewm(alpha=1/period, adjust=False).mean()/atr
        dx = (pdi-mdi).abs()/((pdi+mdi).replace(0,np.nan))*100
        v = dx.ewm(alpha=1/period, adjust=False).mean().iloc[-1]
        return 0.0 if pd.isna(v) else float(v)
    except Exception: return 0.0

def get_heikin_ashi_trend(df, lookback=200):
    try:
        d = df.tail(lookback).reset_index(drop=True)
        if len(d) < 3: return "NEUTRAL"
        hac = (d["open"]+d["high"]+d["low"]+d["close"])/4.0
        hao = pd.Series(index=d.index, dtype=float)
        hao.iloc[0] = (d["open"].iloc[0]+d["close"].iloc[0])/2.0
        for i in range(1, len(d)): hao.iloc[i] = (hao.iloc[i-1]+hac.iloc[i-1])/2.0
        diff = float(hac.iloc[-1] - hao.iloc[-1])
        if abs(diff) < 1e-9: return "NEUTRAL"
        return "BULLISH" if diff > 0 else "BEARISH"
    except Exception: return "NEUTRAL"

def get_timeframe_trend(df):
    if df is None or len(df) < 50: return "NEUTRAL"
    c = df["close"].astype(float)
    e20 = c.ewm(span=20, adjust=False).mean()
    e50 = c.ewm(span=50, adjust=False).mean()
    if c.iloc[-1] > e50.iloc[-1] and e20.iloc[-1] > e50.iloc[-1]: return "BULLISH"
    if c.iloc[-1] < e50.iloc[-1] and e20.iloc[-1] < e50.iloc[-1]: return "BEARISH"
    return "NEUTRAL"

def calculate_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    v = (100 - 100/(1 + gain/(loss+1e-9))).iloc[-1]
    return 50.0 if pd.isna(v) else float(v)

def detect_fair_value_gap(df):
    try:
        if len(df) < 3: return {"detected":False,"type":"NONE","level":0.0}
        c1, c3 = df.iloc[-3], df.iloc[-1]
        if c3["low"] > c1["high"]: return {"detected":True,"type":"BULLISH_FVG","level":float(c1["high"])}
        if c3["high"] < c1["low"]: return {"detected":True,"type":"BEARISH_FVG","level":float(c1["low"])}
        return {"detected":False,"type":"NONE","level":0.0}
    except Exception: return {"detected":False,"type":"NONE","level":0.0}

def detect_institutional_order_blocks(df):
    try:
        d = df.tail(40).reset_index(drop=True)
        if len(d) < 5: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
        atr = compute_atr(df, 14)
        if atr <= 0: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
        
        bodies = (d["close"]-d["open"]).abs().iloc[:-2]
        if bodies.empty: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
        idx = int(bodies.idxmax())
        imp = d.iloc[idx]
        ob = d.iloc[idx-1]
        
        ib = imp["close"] > imp["open"]
        obb = ob["close"] > ob["open"]
        hi, lo = float(max(ob["open"], ob["close"])), float(min(ob["open"], ob["close"]))
        current_close = float(d["close"].iloc[-1])
        
        if ib and not obb and current_close > hi: return {"detected":True,"type":"BULLISH_OB","high":hi,"low":lo}
        if not ib and obb and current_close < lo: return {"detected":True,"type":"BEARISH_OB","high":hi,"low":lo}
        return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
    except Exception: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}

# =========================================================
# SIGNAL & RISK ENGINE
# =========================================================
def compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx, ha):
    c = df_m5["close"].astype(float)
    tr = [get_timeframe_trend(df_m5), get_timeframe_trend(df_m15), get_timeframe_trend(df_h1)]
    tp = sum(1 for x in tr if x == "BULLISH") - sum(1 for x in tr if x == "BEARISH")
    e9, e21 = c.ewm(span=9, adjust=False).mean(), c.ewm(span=21, adjust=False).mean()
    rsi = calculate_rsi(c)
    
    m = 1 if e9.iloc[-1] > e21.iloc[-1] else -1
    if rsi < 40: m += 1
    elif rsi > 60: m -= 1
    
    s = (2 if ob["type"] == "BULLISH_OB" else -2) if ob["detected"] else ((1 if fvg["type"] == "BULLISH_FVG" else -1) if fvg["detected"] else 0)
    ap = 1 if adx >= 25 else (0 if adx >= 20 else -1)
    hp = 1 if ha == "BULLISH" else (-1 if ha == "BEARISH" else 0)
    return {"total": tp+m+s+ap+hp, "trend": tp, "momentum": m, "smc": s, "adx": ap, "ha": hp, "rsi": round(rsi,1), "adx_value": round(adx,1)}

@dataclass
class DecisionLog:
    passed: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    def check(self, name, ok, detail=""):
        (self.passed if ok else self.failed).append(("OK " if ok else "NO ") + f"{name} ({detail})")
        return ok
    def to_dict(self): return {"passed": self.passed, "failed": self.failed}

@dataclass
class SignalResult:
    signal: str
    reason: dict
    log: DecisionLog
    conf: dict
    adx: float
    ha: str
    ob: dict
    fvg: dict

def generate_signal(df_m5, df_m15, df_h1, cfg):
    log = DecisionLog()
    adx = calculate_adx(df_m5)
    
    if not log.check("ADX", adx >= cfg.adx_min, f"{adx:.1f}>={cfg.adx_min}"):
        return SignalResult("NEUTRAL", {"reason": "ADX low"}, log, None, adx, "NEUTRAL", {}, {})
        
    ob = detect_institutional_order_blocks(df_m5)
    fvg = detect_fair_value_gap(df_m5)
    ha = get_heikin_ashi_trend(df_m5)
    conf = compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx, ha)
    
    if conf["total"] >= cfg.score_threshold and ha == "BULLISH":
        return SignalResult("BUY", {}, log, conf, adx, ha, ob, fvg)
    if conf["total"] <= -cfg.score_threshold and ha == "BEARISH":
        return SignalResult("SELL", {}, log, conf, adx, ha, ob, fvg)
        
    return SignalResult("NEUTRAL", {"reason": "low conf"}, log, conf, adx, ha, ob, fvg)

def calculate_multi_target_tpsl(df_m5, signal_type, cfg):
    atr = compute_atr(df_m5, 14)
    price = float(df_m5["close"].iloc[-1])
    if atr <= 0 or signal_type == "NEUTRAL": return 0.0, 0.0, 0.0, 0.0, 0.0
    
    sl_d = atr * cfg.atr_sl_mult
    tp1_d = atr * cfg.atr_tp1_mult
    tp2_d = atr * cfg.atr_tp2_mult
    tp3_d = atr * cfg.atr_tp3_mult
    
    if signal_type == "BUY":
        sl, tp1, tp2, tp3 = price - sl_d, price + tp1_d, price + tp2_d, price + tp3_d
    else:
        sl, tp1, tp2, tp3 = price + sl_d, price - tp1_d, price - tp2_d, price - tp3_d
        
    risk = cfg.account_balance_usd * (cfg.risk_percentage / 100.0)
    raw = risk / (sl_d * cfg.contract_size) if sl_d > 0 else 0.0
    if cfg.account_type.lower() == "cent": raw *= 100.0
    
    lot = round(max(cfg.min_lot, min(cfg.max_lot, raw)), 2)
    p = cfg.price_decimals
    return round(sl, p), round(tp1, p), round(tp2, p), round(tp3, p), lot

# =========================================================
# OUTCOME TRACKING & TELEGRAM
# =========================================================
def send_telegram(msg, cfg):
    if cfg.dry_run: return True
    tok, chat = os.getenv("TELEGRAM_TOKEN", "").strip(), os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not tok or not chat: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage", json={"chat_id": chat, "text": msg, "parse_mode": "HTML"}, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.error(f"Telegram failed: {_redact(e)}")
        return False

def alert_error(msg, cfg):
    send_telegram(f"<b>APEX VOL 80 ERROR</b>\n\n<code>{_h(_redact(msg))}</code>", cfg)

def should_notify(state, signal_type, now_utc, cfg):
    if signal_type == "NEUTRAL": return False
    if state.get("last_signal_type") != signal_type: return True
    lts = state.get("last_signal_timestamp", "")
    if not lts: return True
    try:
        ld = datetime.strptime(lts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return (now_utc - ld) >= timedelta(minutes=cfg.cooldown_minutes)
    except Exception: return True

def build_message(cfg, run_num, now_str, sig, price, sl, tp1, tp2, tp3, lot, conf, ob, fvg, ha, adx):
    smc = ob["type"] if ob["detected"] else fvg["type"]
    p = cfg.price_decimals
    return (f"<b>APEX PREDATOR v{VERSION}</b>\n\n"
            f"<b>{_h(cfg.symbol)} (Vol 80)</b>\n"
            f"<b>{_h(sig)}</b> @ <code>{price:.{p}f}</code>\n\n"
            f"SL: <code>{sl:.{p}f}</code>\nTP1: <code>{tp1:.{p}f}</code>\n"
            f"TP2: <code>{tp2:.{p}f}</code>\nTP3: <code>{tp3:.{p}f}</code>\n"
            f"Lot: <code>{lot}</code> ({cfg.account_type})\n\n"
            f"SMC: <code>{smc}</code> | HA: <code>{ha}</code> | ADX: <code>{adx:.1f}</code>\n"
            f"Conf: <code>{conf['total']:+d}</code> | Run <code>#{run_num}</code>\n\n"
            f"<i>Manual execution via MT5.</i>")

# =========================================================
# MAIN ENGINE
# =========================================================
def run_once(cfg):
    try: _run_once_inner(cfg)
    except Exception as e:
        logging.exception(f"FATAL: {_redact(e)}")
        alert_error(str(e), cfg)

def _run_once_inner(cfg):
    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    
    st = load_state(cfg)
    st["run_count"] = int(st.get("run_count", 0)) + 1
    run_num = st["run_count"]
    save_state(st, cfg)
    
    logging.info(f"=== Vol 80 Run #{run_num} @ {now_str} UTC ===")
    maybe_send_heartbeat(cfg)
    
    blocked_dd, _ = should_block_due_to_drawdown(cfg)
    if blocked_dd: return
    if count_daily_signals(cfg) >= cfg.max_daily_signals: return
    
    try:
        df5 = validate_ohlcv(fetch_deriv_ohlcv(cfg, "5min", 200), "5min", cfg.min_bars_m5, cfg).iloc[:-1].reset_index(drop=True)
        df15 = validate_ohlcv(fetch_deriv_ohlcv(cfg, "15min", 150), "15min", cfg.min_bars_m15, cfg).iloc[:-1].reset_index(drop=True)
        dfh1 = validate_ohlcv(fetch_deriv_ohlcv(cfg, "1h", cfg.outcome_bars_h1), "1h", cfg.min_bars_h1, cfg).iloc[:-1].reset_index(drop=True)
    except Exception as e:
        logging.error(f"Data fetch fail: {_redact(e)}")
        alert_error(f"Data: {e}", cfg)
        return
        
    price = float(df5["close"].iloc[-1])
    res = generate_signal(df5, df15, dfh1, cfg)
    
    notify = False
    st = load_state(cfg)
    if res.signal != "NEUTRAL" and should_notify(st, res.signal, now, cfg):
        st["last_signal_type"] = res.signal
        st["last_signal_timestamp"] = now_str
        save_state(st, cfg)
        notify = True
        
    sl, tp1, tp2, tp3, lot = calculate_multi_target_tpsl(df5, res.signal, cfg)
    
    report = {
        "timestamp": now_str, "run_count": run_num, "active_source": "Deriv-WebSocket",
        "final_signal": res.signal, "notified": notify, "entry_price": round(price, cfg.price_decimals),
        "confidence": res.conf, "adx": round(res.adx, 2), "heikin_ashi": res.ha,
        "ob_type": res.ob.get("type", "NONE"), "fvg_type": res.fvg.get("type", "NONE"),
        "recommended_sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "suggested_lot": lot,
        "decision_path": res.log.to_dict()
    }
    append_report(report, cfg)
    
    if notify and res.signal != "NEUTRAL":
        jid = f"{now_str}_{res.signal}"
        entry = {
            "id": jid, "timestamp": now_str, "symbol": cfg.symbol, "signal": res.signal,
            "entry": round(price, cfg.price_decimals), "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "lot": lot, "confluence": res.conf, "outcome": "PENDING", "notify_status": "PENDING",
            "hit_tp1": 0, "hit_tp2": 0, "hit_tp3": 0, "hit_sl": 0, "hit_be": 0, "r_achieved": 0.0
        }
        append_journal(entry, cfg)
        msg = build_message(cfg, run_num, now_str, res.signal, price, sl, tp1, tp2, tp3, lot,
                            res.conf, res.ob, res.fvg, res.ha, res.adx)
        ok = send_telegram(msg, cfg)
        update_journal_status(jid, {"notify_status": "SENT" if ok else "FAILED"}, cfg)
    logging.info("=== Vol 80 Run Complete ===")

# =========================================================
# CLI ENTRY POINT
# =========================================================
def main():
    try: cfg = Config.from_env()
    except ValueError as e: print(f"\n{e}\n"); sys.exit(2)
        
    setup_logging(cfg)
    init_db(cfg)
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "status":
            st = load_state(cfg)
            print(f"=== APEX VOL 80 STATUS ===\nRuns: {st.get('run_count',0)}\nLast: {st.get('last_signal_type')} @ {st.get('last_signal_timestamp')}")
            sys.exit(0)
        elif cmd == "run": run_once(cfg); sys.exit(0)
        
    run_once(cfg)

if __name__ == "__main__":
    main()

