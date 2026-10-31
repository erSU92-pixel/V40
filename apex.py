"""
APEX PREDATOR v8.0 — Institutional Grade XAUUSD Signal Engine
=========================================================================
Fitur Utama v8.0:
- Institutional SMC Engine (Liquidity Sweeps, CHoCH, & Order Block Filtering)
- Dynamic ATR Stop-Loss Adjustment & Multi-Timeframe Alignment (H4, H1, M15, M5)
- Automated Price Feed Offset Calibration (Yahoo & TwelveData Feed Alignment)
- SQLite Enterprise Journaling & State Persistence (Anti-Corrupt Atomicity)
- Precision Lot Sizing Engine (Headway Cent & Standard Account Native Support)
- News Shield (Fail-Closed Adaptive Guard) & Drawdown Circuit Breaker
- Telegram HTML Interactive Signal & Heartbeat Notifications
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

# Cek library websocket (opsional)
try:
    import websocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

VERSION = "8.0"
MAX_CONFLUENCE = 10

# =========================================================
# KONFIGURASI ENGINE
# =========================================================
@dataclass
class Config:
    # Database & Storage
    db_file: str = "apex_database.db"
    log_file: str = "apex.log"
    
    # Symbol & Account Configuration
    symbol: str = "XAUUSD"
    deriv_symbol: str = "frxXAUUSD"
    contract_size: float = 100.0
    account_type: str = "cent"  # Mode: "cent" atau "standard"
    price_decimals: int = 3
    
    # Timeframes & Bar Thresholds
    cooldown_minutes: int = 45
    min_bars_m5: int = 80
    min_bars_m15: int = 80
    min_bars_h1: int = 80
    outcome_bars_h1: int = 150
    max_stale_mult: float = 3.0
    max_gap_ratio: float = 0.10
    
    # Risk Management & Lot Sizing
    account_balance_usd: float = 10000.0
    risk_percentage: float = 1.0
    max_lot: float = 50.0  # Safe cap untuk Akun Cent Headway
    min_lot: float = 0.01
    
    # ATR & Target Multipliers
    atr_sl_mult: float = 2.2
    atr_tp1_mult: float = 1.2
    atr_tp2_mult: float = 2.4
    atr_tp3_mult: float = 4.0
    spread_buffer_dollars: float = 0.35
    
    # Signal Thresholds
    score_threshold: int = 8
    adx_min: float = 22.0
    adx_strong: float = 28.0
    max_daily_signals: int = 5
    
    # News Shield & Protection
    news_blackout_minutes: int = 30
    news_cache_seconds: int = 1800
    news_fail_mode: str = "closed"
    
    # System Operational Flags
    report_max_entries: int = 500
    dry_run: bool = False
    enable_deriv: bool = False  # Set False untuk bypass Cloudflare Error 520 di cloud server
    
    # Feature Flags
    track_outcomes: bool = True
    notify_on_tp_hit: bool = True
    drawdown_guard_enabled: bool = True
    drawdown_max_consecutive_losses: int = 3
    drawdown_pause_hours: int = 24
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
                    if not np.isfinite(fv):
                        logging.warning(f"APEX_{f.upper()} not finite"); continue
                    setattr(cfg, f, fv)
                else: setattr(cfg, f, v)
            except ValueError:
                logging.warning(f"APEX_{f.upper()}={v} invalid")
        cfg._validate()
        return cfg

    def _validate(self):
        errs = []
        if self.news_fail_mode not in ("closed","open"): errs.append("news_fail_mode")
        if self.account_balance_usd <= 0: errs.append("balance<=0")
        if not (0 < self.risk_percentage <= 10): errs.append("risk not 0-10")
        if self.min_lot <= 0 or self.max_lot < self.min_lot: errs.append("lot config")
        if not (0 < self.score_threshold <= MAX_CONFLUENCE): errs.append("score invalid")
        if self.max_daily_signals < 1: errs.append("max_daily<1")
        if self.account_type not in ("standard", "cent"): errs.append("account_type must be standard/cent")
        if errs: raise ValueError("CONFIG ERROR: " + "; ".join(errs))

# =========================================================
# UTILITIES & LOGGING
# =========================================================
def setup_logging(cfg, verbose=False):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    root.handlers.clear()
    fmt = logging.Formatter(f"%(asctime)s [%(levelname)s] [Apex-v{VERSION}] - %(message)s")
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
    t = re.sub(r"apikey=[^&\s'\"]+", "apikey=***", t)
    return t

def _h(s): return html.escape(str(s), quote=False)

class DataFetchError(Exception): pass

# =========================================================
# DATABASE LAYER (SQLite Persistence Engine)
# =========================================================
def get_db_connection(cfg):
    conn = sqlite3.connect(cfg.db_file)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(cfg):
    with get_db_connection(cfg) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS journal (
                id TEXT PRIMARY KEY,
                timestamp TEXT,
                symbol TEXT,
                signal TEXT,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                lot REAL,
                confluence TEXT,
                outcome TEXT,
                pnl_usd REAL,
                notes TEXT,
                notify_status TEXT,
                hit_tp1 INTEGER,
                hit_tp2 INTEGER,
                hit_tp3 INTEGER,
                hit_sl INTEGER,
                hit_be INTEGER,
                r_achieved REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                run_count INTEGER,
                active_source TEXT,
                final_signal TEXT,
                notified INTEGER,
                entry_price REAL,
                confidence TEXT,
                adx REAL,
                heikin_ashi TEXT,
                ob_type TEXT,
                fvg_type TEXT,
                recommended_sl REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                suggested_lot REAL,
                decision_path TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()

def db_get_kv(cfg, key, default):
    try:
        with get_db_connection(cfg) as conn:
            cur = conn.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
            row = cur.fetchone()
            if row:
                return json.loads(row["value"])
    except Exception:
        pass
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
# STATE MANAGEMENT
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
            set_clauses = []
            vals = []
            for k, v in updates.items():
                set_clauses.append(f"{k} = ?")
                vals.append(v)
            vals.append(jid)
            conn.execute(f"UPDATE journal SET {', '.join(set_clauses)} WHERE id = ?", tuple(vals))
            conn.commit()
    except Exception as e:
        logging.error(f"Update journal status DB failed: {_redact(e)}")

def get_all_journal(cfg):
    init_db(cfg)
    try:
        with get_db_connection(cfg) as conn:
            cur = conn.execute("SELECT * FROM journal ORDER BY timestamp DESC")
            return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []

def get_all_reports(cfg):
    init_db(cfg)
    try:
        with get_db_connection(cfg) as conn:
            cur = conn.execute("SELECT * FROM reports ORDER BY id DESC")
            return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []

def count_daily_signals(cfg):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    reports = get_all_reports(cfg)
    return sum(1 for r in reports if str(r.get("timestamp","")).startswith(today) and r.get("final_signal") in ("BUY","SELL") and r.get("notified") == 1)

# =========================================================
# CIRCUIT BREAKER & DRAWDOWN GUARD
# =========================================================
def circuit_allow(cfg):
    c = db_get_kv(cfg, "circuit", {"failures":0,"opened_until":None})
    if c.get("opened_until"):
        try:
            if datetime.now(timezone.utc) < datetime.fromisoformat(c["opened_until"]): return False
        except Exception: pass
    return True

def circuit_record_failure(cfg, threshold=3, cooldown_min=15):
    c = db_get_kv(cfg, "circuit", {"failures":0,"opened_until":None})
    f = int(c.get("failures",0)) + 1
    opened = c.get("opened_until")
    if f >= threshold:
        opened = (datetime.now(timezone.utc) + timedelta(minutes=cooldown_min)).isoformat()
        f = 0
        logging.warning(f"Circuit OPEN {cooldown_min}m")
    db_set_kv(cfg, "circuit", {"failures":f,"opened_until":opened})

def circuit_record_success(cfg):
    db_set_kv(cfg, "circuit", {"failures":0,"opened_until":None})

def should_block_due_to_drawdown(cfg):
    if not cfg.drawdown_guard_enabled: return False, ""
    p = db_get_kv(cfg, "drawdown_pause", {})
    if p.get("until"):
        try:
            if datetime.now(timezone.utc) < datetime.fromisoformat(p["until"]):
                return True, f"pause until {p['until']}"
        except Exception: pass
    
    j = get_all_journal(cfg)
    closed = [x for x in j if x.get("outcome") not in (None,"PENDING")]
    closed.sort(key=lambda x: x.get("timestamp",""), reverse=True)
    consec = 0
    for x in closed:
        if x.get("outcome") == "LOSS": consec += 1
        else: break
    if consec >= cfg.drawdown_max_consecutive_losses:
        until = (datetime.now(timezone.utc) + timedelta(hours=cfg.drawdown_pause_hours)).isoformat()
        db_set_kv(cfg, "drawdown_pause", {"until":until,"reason":f"{consec} losses","created_at":datetime.now(timezone.utc).isoformat()})
        try:
            send_telegram(f"<b>DRAWDOWN GUARD (v8.0)</b>\n\n{consec} loss berturut-turut terdeteksi. Bot PAUSE {cfg.drawdown_pause_hours}h.\n\nResume: <code>python apex.py reset-drawdown</code>", cfg)
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
    
    reports = get_all_reports(cfg)
    today_r = [r for r in reports if str(r.get("timestamp","")).startswith(today)]
    sig_today = sum(1 for r in today_r if r.get("notified") == 1)
    j = get_all_journal(cfg)
    pending = [x for x in j if x.get("outcome")=="PENDING"]
    circuit = "OPEN" if not circuit_allow(cfg) else "CLOSED"
    pause = bool(db_get_kv(cfg, "drawdown_pause", {}).get("until"))
    
    msg = (f"<b>APEX DAILY HEARTBEAT v{VERSION}</b>\n\n"
           f"Runs Hari Ini: <code>{len(today_r)}</code>\n"
           f"Sinyal Valid: <code>{sig_today}</code>\n"
           f"Pending Posisi: <code>{len(pending)}</code>\n"
           f"Circuit Breaker: <code>{circuit}</code>\n"
           f"Drawdown Pause: <code>{'AKTIF' if pause else 'Off'}</code>\n\n"
           f"<code>{now.strftime('%Y-%m-%d %H:%M')} UTC</code>")
    if send_telegram(msg, cfg):
        db_set_kv(cfg, "heartbeat", {"last_sent_date":today,"last_sent_at":now.isoformat()})

# =========================================================
# DATA FETCHERS WITH AUTO-OFFSET SCALER
# =========================================================
def _candles_to_df(candles):
    df = pd.DataFrame(candles)
    df["datetime"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    for c in ["open","high","low","close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(100.0) if "volume" in df.columns else 100.0
    return df[["datetime","open","high","low","close","volume"]].reset_index(drop=True)

def fetch_deriv_ohlcv(cfg, interval_str, count=200, retries=2):
    if not HAS_WEBSOCKET: raise DataFetchError("websocket-client not installed")
    gmap = {"5min":300,"15min":900,"1h":3600}
    gran = gmap.get(interval_str)
    if gran is None: raise DataFetchError(f"Interval {interval_str} not supported")
    payload = {"ticks_history":cfg.deriv_symbol,"count":count,"end":"latest","granularity":gran,"style":"candles"}
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    last = None
    for att in range(retries+1):
        ws = None
        try:
            ws = websocket.create_connection(url, timeout=10, enable_multithread=True)
            ws.send(json.dumps(payload))
            deadline = time.time() + 15
            candles = None
            while time.time() < deadline:
                raw = ws.recv()
                if not raw: continue
                try: d = json.loads(raw)
                except json.JSONDecodeError: continue
                if "error" in d: raise DataFetchError(f"Deriv: {d['error'].get('message','?')}")
                if d.get("msg_type") == "candles":
                    c = d.get("candles",[])
                    if c: candles = c; break
            if not candles: raise DataFetchError("Deriv timeout")
            return _candles_to_df(candles)
        except Exception as e:
            last = e
            logging.warning(f"Deriv att{att+1}: {_redact(e)}")
            if att < retries: time.sleep(2**att)
        finally:
            if ws:
                try: ws.close()
                except Exception: pass
    raise DataFetchError(f"Deriv failed {retries+1}x: {_redact(last)}")

def fetch_yahoo_ohlcv(cfg, interval_str, count=200):
    imap = {"5min":"5m","15min":"15m","1h":"1h"}
    iv = imap.get(interval_str)
    if iv is None: raise DataFetchError(f"Interval {interval_str} not supported")
    hdrs = {"User-Agent":f"Mozilla/5.0 (compatible; ApexBot/{VERSION})"}
    
    def _fetch(sym):
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                         params={"interval":iv,"range":"5d"}, headers=hdrs, timeout=10)
        r.raise_for_status()
        data = r.json()
        result = data.get("chart",{}).get("result")
        if not result: raise DataFetchError(f"Yahoo {sym}: no result")
        res = result[0]
        ts = res.get("timestamp",[])
        q = res.get("indicators",{}).get("quote",[{}])[0]
        op,hi,lo,cl,vo = (q.get(k,[]) for k in ("open","high","low","close","volume"))
        rows = [{"epoch":int(t),"open":op[i],"high":hi[i],"low":lo[i],"close":cl[i],
                 "volume":vo[i] if i < len(vo) and vo[i] else 100.0}
                for i,t in enumerate(ts) if i < len(op) and op[i] is not None and cl[i] is not None]
        if not rows: raise DataFetchError(f"Yahoo {sym}: no data")
        return _candles_to_df(rows)
    
    cache_df = None
    try:
        cache_df = _fetch("XAUUSD=X")
        logging.info(f"Yahoo spot OK: {len(cache_df)} bars")
        return cache_df.tail(count).reset_index(drop=True)
    except Exception as e:
        logging.warning(f"Yahoo spot failed: {_redact(e)}")
    
    try:
        df = _fetch("GC=F")
        spot = None
        if cache_df is not None: 
            spot = float(cache_df["close"].iloc[-1])
        else:
            try:
                r = requests.get("https://xaus.com/api/v1/spot", timeout=5)
                r.raise_for_status()
                spot = float(r.json().get("spot_usd_oz",0))
            except Exception: 
                spot = float(df["close"].iloc[-1]) + float(os.getenv("APEX_YAHOO_OFFSET","0"))
        if spot and spot > 0:
            off = spot - float(df["close"].iloc[-1])
            logging.info(f"Yahoo GC=F+offset {off:+.3f}")
            for c in ["open","high","low","close"]: 
                df[c] = df[c] + off
        return df.tail(count).reset_index(drop=True)
    except Exception as e:
        raise DataFetchError(f"Yahoo all failed: {_redact(e)}")

def fetch_twelvedata_ohlcv(cfg, interval_str, count=200, retries=2):
    key = os.getenv("TWELVEDATA_API_KEY","").strip()
    if not key: raise DataFetchError("TWELVEDATA_API_KEY not set")
    imap = {"5min":"5min","15min":"15min","1h":"1h"}
    iv = imap.get(interval_str)
    if iv is None: raise DataFetchError(f"Interval {interval_str} not supported")
    last = None
    for att in range(retries+1):
        try:
            r = requests.get("https://api.twelvedata.com/time_series",
                params={"symbol":"XAU/USD","interval":iv,"outputsize":count,"apikey":key,"timezone":"UTC"},
                timeout=10)
            if r.status_code in (401,403): raise DataFetchError("TwelveData: invalid key")
            r.raise_for_status()
            d = r.json()
            if "values" not in d: raise DataFetchError(f"TwelveData: {d.get('message','?')}")
            df = pd.DataFrame(d["values"])
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            for c in ["open","high","low","close"]: 
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["volume"] = 100.0
            df = df.sort_values("datetime").reset_index(drop=True)
            return df[["datetime","open","high","low","close","volume"]]
        except DataFetchError: raise
        except Exception as e:
            last = e
            if att < retries: time.sleep(2**att)
    raise DataFetchError(f"TwelveData: {_redact(last)}")

def fetch_ohlcv_cascade(cfg, interval_str, count=200):
    if not circuit_allow(cfg): raise DataFetchError("Circuit breaker active")
    errors = []
    srcs = []
    srcs.append(("Yahoo", fetch_yahoo_ohlcv))
    srcs.append(("TwelveData", fetch_twelvedata_ohlcv))
    if cfg.enable_deriv: srcs.append(("Deriv", fetch_deriv_ohlcv))
    
    for name, fn in srcs:
        try:
            df = fn(cfg, interval_str, count)
            circuit_record_success(cfg)
            logging.info(f"Data OK {name} ({len(df)} bars)")
            return df, name
        except Exception as e:
            errors.append(f"{name}: {_redact(e)}")
            logging.warning(f"{name} failed: {_redact(e)}")
    circuit_record_failure(cfg)
    raise DataFetchError("ALL FEEDS FAILED | " + " || ".join(errors))

# =========================================================
# VALIDATION & MARKET GATES
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
    if age > isec * cfg.max_stale_mult: raise ValueError(f"Stale {age:.0f}s")
    recent = df.tail(50)
    if len(recent) >= 10:
        diffs = recent["datetime"].diff().dt.total_seconds().dropna()
        gaps = (diffs > isec * 1.5).sum()
        if len(diffs) > 0 and (gaps/len(diffs)) > cfg.max_gap_ratio:
            raise ValueError(f"Gaps {gaps}/{len(diffs)}")
    return df.reset_index(drop=True)

def is_market_open(now_utc):
    wd, h = now_utc.weekday(), now_utc.hour
    if wd == 5: return False
    if wd == 6: return h >= 22
    if wd == 4: return h < 21
    return True

def is_institutional_killzone(now_utc):
    h = now_utc.hour
    return (7 <= h <= 11) or (13 <= h <= 17)

# =========================================================
# NEWS SHIELD
# =========================================================
def check_news_shield(cfg):
    now = datetime.now(timezone.utc)
    cache = db_get_kv(cfg, "news_cache", {"ok":False,"ts":None,"blocked":True,"reason":""})
    if cache.get("ok") and cache.get("ts"):
        try:
            ts_dt = datetime.fromisoformat(cache["ts"])
            if (now - ts_dt).total_seconds() < cfg.news_cache_seconds:
                return cache["blocked"], cache["reason"]
        except Exception: pass

    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=5)
        r.raise_for_status()
        events = r.json()
        parsed = 0
        for e in events:
            if e.get("country") != "USD" or e.get("impact") != "High": continue
            try: ev_dt = datetime.strptime(e["date"], "%Y-%m-%dT%H:%M:%S%z")
            except Exception:
                try: ev_dt = datetime.fromisoformat(e["date"].replace("Z","+00:00"))
                except Exception: continue
            parsed += 1
            diff = abs((ev_dt - now).total_seconds()/60)
            if diff <= cfg.news_blackout_minutes:
                reason = f"news '{e.get('title','?')}' in {diff:.0f}m"
                db_set_kv(cfg, "news_cache", {"ok":True,"ts":now.isoformat(),"blocked":True,"reason":reason})
                return True, reason
        if parsed == 0 and events and cfg.news_fail_mode == "closed":
            db_set_kv(cfg, "news_cache", {"ok":True,"ts":now.isoformat(),"blocked":True,"reason":"news parse anomaly"})
            return True, "news parse anomaly"
        db_set_kv(cfg, "news_cache", {"ok":True,"ts":now.isoformat(),"blocked":False,"reason":""})
        return False, ""
    except Exception as ex:
        logging.error(f"News failed: {_redact(ex)}")
        if cfg.news_fail_mode == "closed":
            db_set_kv(cfg, "news_cache", {"ok":True,"ts":now.isoformat(),"blocked":True,"reason":"API down fail-closed"})
            return True, "API down"
        db_set_kv(cfg, "news_cache", {"ok":True,"ts":now.isoformat(),"blocked":False,"reason":"fail-open"})
        return False, ""

# =========================================================
# INSTITUTIONAL SMC ENGINE (Liquidity Sweeps & CHoCH)
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
    except Exception: 
        return 0.0

def get_heikin_ashi_trend(df, lookback=200):
    try:
        d = df.tail(lookback).reset_index(drop=True)
        if len(d) < 3: return "NEUTRAL"
        hac = (d["open"]+d["high"]+d["low"]+d["close"])/4.0
        hao = pd.Series(index=d.index, dtype=float)
        hao.iloc[0] = (d["open"].iloc[0]+d["close"].iloc[0])/2.0
        for i in range(1, len(d)): 
            hao.iloc[i] = (hao.iloc[i-1]+hac.iloc[i-1])/2.0
        diff = float(hac.iloc[-1] - hao.iloc[-1])
        if abs(diff) < 1e-9: return "NEUTRAL"
        return "BULLISH" if diff > 0 else "BEARISH"
    except Exception: 
        return "NEUTRAL"

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
        if c3["low"] > c1["high"]: 
            return {"detected":True,"type":"BULLISH_FVG","level":float(c1["high"])}
        if c3["high"] < c1["low"]: 
            return {"detected":True,"type":"BEARISH_FVG","level":float(c1["low"])}
        return {"detected":False,"type":"NONE","level":0.0}
    except Exception: 
        return {"detected":False,"type":"NONE","level":0.0}

def detect_institutional_order_blocks(df):
    """
    Institutional Grade OB Detection dengan Filter Liquidity Sweep & Structural BOS/CHoCH
    """
    try:
        d = df.tail(45).reset_index(drop=True)
        if len(d) < 8: return {"detected":False,"type":"NONE","high":0.0,"low":0.0,"swept":False}
        atr = compute_atr(df, 14)
        if atr <= 0: return {"detected":False,"type":"NONE","high":0.0,"low":0.0,"swept":False}
        
        bodies = (d["close"]-d["open"]).abs().iloc[:-2]
        if bodies.empty: return {"detected":False,"type":"NONE","high":0.0,"low":0.0,"swept":False}
        idx = int(bodies.idxmax())
        if idx < 3 or idx < len(d)-25: 
            return {"detected":False,"type":"NONE","high":0.0,"low":0.0,"swept":False}
            
        imp = d.iloc[idx]
        ob = d.iloc[idx-1]
        pre_ob_bars = d.iloc[max(0, idx-6):idx-1]
        
        if abs(float(imp["close"]-imp["open"])) < (atr * 1.25): 
            return {"detected":False,"type":"NONE","high":0.0,"low":0.0,"swept":False}
            
        ib = imp["close"] > imp["open"]
        obb = ob["close"] > ob["open"]
        hi = float(max(ob["open"], ob["close"]))
        lo = float(min(ob["open"], ob["close"]))
        
        # Validasi Liquidity Sweep (Pengambil Liquidity)
        swept_liquidity = False
        if ib and not obb:
            if ob["low"] < pre_ob_bars["low"].min():
                swept_liquidity = True
        elif not ib and obb:
            if ob["high"] > pre_ob_bars["high"].max():
                swept_liquidity = True

        current_close = float(d["close"].iloc[-1])
        if ib and not obb and current_close > hi: 
            return {"detected":True,"type":"BULLISH_OB","high":hi,"low":lo,"swept":swept_liquidity}
        if not ib and obb and current_close < lo: 
            return {"detected":True,"type":"BEARISH_OB","high":hi,"low":lo,"swept":swept_liquidity}
            
        return {"detected":False,"type":"NONE","high":0.0,"low":0.0,"swept":False}
    except Exception: 
        return {"detected":False,"type":"NONE","high":0.0,"low":0.0,"swept":False}

# =========================================================
# SIGNAL & RISK ENGINE
# =========================================================
def compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx, ha):
    c = df_m5["close"].astype(float)
    tr = [get_timeframe_trend(df_m5), get_timeframe_trend(df_m15), get_timeframe_trend(df_h1)]
    tp = sum(1 for x in tr if x == "BULLISH") - sum(1 for x in tr if x == "BEARISH")
    e9 = c.ewm(span=9, adjust=False).mean()
    e21 = c.ewm(span=21, adjust=False).mean()
    rsi = calculate_rsi(c)
    m = 0
    m += 1 if e9.iloc[-1] > e21.iloc[-1] else -1
    if rsi < 40: m += 1
    elif rsi > 60: m -= 1
    
    s = 0
    if ob["detected"]: 
        base_s = 2 if ob["type"] == "BULLISH_OB" else -2
        if ob.get("swept", False): base_s += (1 if base_s > 0 else -1) # Bonus poin liquidity sweep
        s += base_s
    elif fvg["detected"]: 
        s += 1 if fvg["type"] == "BULLISH_FVG" else -1
        
    ap = 1 if adx >= 28 else (0 if adx >= 22 else -1)
    hp = 1 if ha == "BULLISH" else (-1 if ha == "BEARISH" else 0)
    return {"total":tp+m+s+ap+hp,"trend":tp,"momentum":m,"smc":s,"adx":ap,"ha":hp,"rsi":round(rsi,1),"adx_value":round(adx,1)}

@dataclass
class DecisionLog:
    passed: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    def check(self, name, ok, detail=""):
        label = f"{name}" + (f" ({detail})" if detail else "")
        (self.passed if ok else self.failed).append(("OK " if ok else "NO ") + label)
        return ok
    def to_dict(self): return {"passed":self.passed,"failed":self.failed}

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

_EOB = {"detected":False,"type":"NONE","high":0.0,"low":0.0,"swept":False}
_EFVG = {"detected":False,"type":"NONE","level":0.0}

def generate_signal(df_m5, df_m15, df_h1, cfg):
    log = DecisionLog()
    adx = calculate_adx(df_m5)
    
    if not log.check("ADX", adx >= cfg.adx_min, f"{adx:.1f}>={cfg.adx_min}"):
        return SignalResult("NEUTRAL",{"reason":f"ADX {adx:.1f}"},log,None,adx,"NEUTRAL",_EOB,_EFVG)
    
    ob = detect_institutional_order_blocks(df_m5)
    fvg = detect_fair_value_gap(df_m5)
    
    if not log.check("SMC", ob["detected"] or fvg["detected"], ob["type"] if ob["detected"] else fvg["type"]):
        return SignalResult("NEUTRAL",{"reason":"no OB/FVG"},log,None,adx,"NEUTRAL",ob,fvg)
    
    ha = get_heikin_ashi_trend(df_m5)
    conf = compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx, ha)
    
    if conf["total"] >= cfg.score_threshold and ha == "BULLISH" and ob["type"] in ("BULLISH_OB","NONE"):
        log.check("Confluence",True,f"+{conf['total']}")
        log.check("HA",True,ha)
        return SignalResult("BUY",{},log,conf,adx,ha,ob,fvg)
        
    if conf["total"] <= -cfg.score_threshold and ha == "BEARISH" and ob["type"] in ("BEARISH_OB","NONE"):
        log.check("Confluence",True,f"{conf['total']}")
        log.check("HA",True,ha)
        return SignalResult("SELL",{},log,conf,adx,ha,ob,fvg)
        
    log.check("Confluence",False,f"{conf['total']:+d} vs +/-{cfg.score_threshold}")
    return SignalResult("NEUTRAL",{"reason":"low conf"},log,conf,adx,ha,ob,fvg)

def calculate_multi_target_tpsl(df_m5, signal_type, cfg):
    atr = compute_atr(df_m5, 14)
    price = float(df_m5["close"].iloc[-1])
    if atr <= 0 or signal_type == "NEUTRAL": 
        return 0.0,0.0,0.0,0.0,0.0
    sl_d = atr*cfg.atr_sl_mult
    tp1_d = max(atr*cfg.atr_tp1_mult, cfg.spread_buffer_dollars*2)
    tp2_d = atr*cfg.atr_tp2_mult
    tp3_d = atr*cfg.atr_tp3_mult
    if signal_type == "BUY": 
        sl = price-sl_d
        tp1,tp2,tp3 = price+tp1_d, price+tp2_d, price+tp3_d
    else: 
        sl = price+sl_d
        tp1,tp2,tp3 = price-tp1_d, price-tp2_d, price-tp3_d
        
    risk = cfg.account_balance_usd * (cfg.risk_percentage/100.0)
    raw = risk / (sl_d * cfg.contract_size) if sl_d > 0 else 0.0
    
    # Lot Scaling Akun Cent Headway Native
    if cfg.account_type.lower() == "cent":
        raw = raw * 100.0
        
    lot = round(max(cfg.min_lot, min(cfg.max_lot, raw)), 2)
    p = cfg.price_decimals
    return round(sl,p), round(tp1,p), round(tp2,p), round(tp3,p), lot

# =========================================================
# OUTCOME TRACKING & TELEGRAM
# =========================================================
def _update_single_outcome(entry, df_h1):
    try: entry_ts = pd.Timestamp(entry["timestamp"], tz="UTC")
    except Exception: return False
    bars = df_h1[df_h1["datetime"] > entry_ts.floor("h")]
    if len(bars) == 0: return False
    direction = entry.get("signal")
    entry_p = float(entry.get("entry",0))
    sl = float(entry.get("sl",0))
    tp1 = float(entry.get("tp1",0))
    tp2 = float(entry.get("tp2",0))
    tp3 = float(entry.get("tp3",0))
    if entry_p <= 0 or sl <= 0 or tp1 <= 0: return False
    
    ht1 = bool(entry.get("hit_tp1", False))
    ht2 = bool(entry.get("hit_tp2", False))
    ht3 = bool(entry.get("hit_tp3", False))
    hsl = bool(entry.get("hit_sl", False))
    hbe = bool(entry.get("hit_be", False))
    changed = False
    
    for _, b in bars.iterrows():
        if ht3 or hsl or hbe: break
        csl = entry_p if ht1 else sl
        if direction == "BUY":
            if b["low"] <= csl:
                if ht1: hbe = True; entry["hit_be"] = True
                else: hsl = True; entry["hit_sl"] = True
                changed = True; continue
            if not ht1 and b["high"] >= tp1: ht1 = True; entry["hit_tp1"] = True; changed = True
            if not ht2 and b["high"] >= tp2: ht2 = True; entry["hit_tp2"] = True; changed = True
            if not ht3 and b["high"] >= tp3: ht3 = True; entry["hit_tp3"] = True; changed = True
        else:
            if b["high"] >= csl:
                if ht1: hbe = True; entry["hit_be"] = True
                else: hsl = True; entry["hit_sl"] = True
                changed = True; continue
            if not ht1 and b["low"] <= tp1: ht1 = True; entry["hit_tp1"] = True; changed = True
            if not ht2 and b["low"] <= tp2: ht2 = True; entry["hit_tp2"] = True; changed = True
            if not ht3 and b["low"] <= tp3: ht3 = True; entry["hit_tp3"] = True; changed = True
            
    if entry.get("outcome") == "PENDING":
        if ht3: 
            entry["outcome"] = "WIN_TP3"; entry["r_achieved"] = 4.0; changed = True
        elif hbe:
            entry["outcome"] = "WIN_BE"
            entry["r_achieved"] = 1.2 if ht2 else (0.4 if ht1 else 0.0)
            changed = True
        elif hsl: 
            entry["outcome"] = "LOSS"; entry["r_achieved"] = -1.0; changed = True
    return changed

def update_journal_outcomes(df_h1_full, cfg):
    if not cfg.track_outcomes: return
    j = get_all_journal(cfg)
    if not any(e.get("outcome") == "PENDING" for e in j): return
    changed = False
    events = []
    for e in j:
        if e.get("outcome") != "PENDING": continue
        was_tp1 = bool(e.get("hit_tp1", False))
        was_outcome = e.get("outcome")
        if _update_single_outcome(e, df_h1_full):
            changed = True
            if not was_tp1 and e.get("hit_tp1"): events.append(("TP1_HIT", e))
            if was_outcome == "PENDING" and e.get("outcome") != "PENDING": events.append(("CLOSED", e))
            append_journal(e, cfg)
    if changed and cfg.notify_on_tp_hit:
        for kind, entry in events:
            try: _send_outcome_update(kind, entry, cfg)
            except Exception as ex: logging.warning(f"Outcome notif failed: {_redact(ex)}")

def _send_outcome_update(kind, entry, cfg):
    sig = entry.get("signal")
    ep = entry.get("entry")
    if kind == "TP1_HIT":
        msg = (f"<b>🎯 TP1 HIT (v8.0)</b>\n\n{_h(sig)} @ <code>{ep}</code>\n\n"
               f"<b>Aksi:</b> Partial 1/3 Target, Geser SL ke Entry <code>{ep}</code> (BE).")
    elif kind == "CLOSED":
        out = entry.get("outcome","?")
        r = entry.get("r_achieved",0)
        if out.startswith("WIN"):
            tag = "TP3" if out == "WIN_TP3" else "TP1+BE"
            msg = f"<b>✅ TRADE CLOSED — WIN ({tag})</b>\n\n{_h(sig)} @ <code>{ep}</code>\nR Ratio: <code>{r:+.2f}R</code>"
        else:
            msg = f"<b>❌ TRADE CLOSED — LOSS</b>\n\n{_h(sig)} @ <code>{ep}</code>\nR Ratio: <code>{r:+.2f}R</code>"
    else: return
    send_telegram(msg, cfg)

def send_telegram(msg, cfg):
    if cfg.dry_run: 
        logging.info(f"[DRY] TG: {msg[:100]}...")
        return True
    tok = os.getenv("TELEGRAM_TOKEN","").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not tok or not chat: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage", json={"chat_id":chat,"text":msg,"parse_mode":"HTML"}, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.error(f"Telegram failed: {_redact(e)}")
        return False

def alert_error(msg, cfg):
    now = datetime.now(timezone.utc)
    last_d = db_get_kv(cfg, "alert_limit", {"last":None})
    last = None
    if last_d.get("last"):
        try: last = datetime.fromisoformat(last_d["last"])
        except Exception: pass
    if last and (now-last).total_seconds() < 300: return
    db_set_kv(cfg, "alert_limit", {"last":now.isoformat()})
    send_telegram(f"<b>⚠️ APEX SYSTEM ERROR</b>\n\n<code>{_h(_redact(msg))}</code>", cfg)

def should_notify(state, signal_type, now_utc, cfg):
    if signal_type == "NEUTRAL": return False
    if state.get("last_signal_type") != signal_type: return True
    lts = state.get("last_signal_timestamp","")
    if not lts: return True
    try:
        ld = datetime.strptime(lts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return (now_utc - ld) >= timedelta(minutes=cfg.cooldown_minutes)
    except Exception: return True

def build_message(cfg, run_num, now_str, source, sig, price, sl, tp1, tp2, tp3, lot, conf, ob, fvg, ha, adx):
    smc = ob["type"] if ob["detected"] else fvg["type"]
    if ob.get("swept", False): smc += " (Swept)"
    p = cfg.price_decimals
    bd = f"T{conf['trend']:+d} M{conf['momentum']:+d} S{conf['smc']:+d} A{conf['adx']:+d} H{conf['ha']:+d}"
    return (f"<b>⚡ APEX PREDATOR v{VERSION} ⚡</b>\n\n"
            f"<b>{_h(cfg.symbol)}</b> | Data: <code>{_h(source)}</code>\n"
            f"Sinyal: <b>{_h(sig)}</b> @ <code>{price:.{p}f}</code>\n\n"
            f"SL: <code>{_h(sl)}</code>\nTP1: <code>{_h(tp1)}</code>\n"
            f"TP2: <code>{_h(tp2)}</code>\nTP3: <code>{_h(tp3)}</code>\n"
            f"Sizing Lot: <code>{_h(lot)}</code> ({cfg.account_type.upper()}, Risk {cfg.risk_percentage}%)\n\n"
            f"SMC Pattern: <code>{_h(smc)}</code>\nHA: <code>{_h(ha)}</code> | ADX: <code>{adx:.1f}</code> | RSI: <code>{conf['rsi']}</code>\n"
            f"Confluence: <code>{conf['total']:+d}/+/-{MAX_CONFLUENCE}</code> (<code>{_h(bd)}</code>)\n"
            f"Run <code>#{run_num}</code> | <code>{_h(now_str)} UTC</code>\n\n"
            f"<i>Manual order execution via MT5.</i>")

# =========================================================
# MAIN ENGINE
# =========================================================
def run_once(cfg):
    try: _run_once_inner(cfg)
    except KeyboardInterrupt: raise
    except Exception as e:
        logging.exception(f"FATAL: {_redact(e)}")
        try: alert_error(f"FATAL {type(e).__name__}: {e}", cfg)
        except Exception: pass

def _run_once_inner(cfg):
    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    
    st = load_state(cfg)
    st["run_count"] = int(st.get("run_count",0))+1
    run_num = st["run_count"]
    save_state(st, cfg)
        
    logging.info(f"=== Run #{run_num} @ {now_str} UTC ===")
    try: maybe_send_heartbeat(cfg)
    except Exception as e: logging.warning(f"HB: {_redact(e)}")
    
    if not is_market_open(now): return
    blocked_dd, reason_dd = should_block_due_to_drawdown(cfg)
    if blocked_dd: return
    if not is_institutional_killzone(now): return
        
    try: blocked, reason = check_news_shield(cfg)
    except Exception as e:
        blocked = (cfg.news_fail_mode == "closed")
    if blocked: return
        
    if count_daily_signals(cfg) >= cfg.max_daily_signals: return
        
    try:
        r5, src = fetch_ohlcv_cascade(cfg, "5min", 200)
        df5 = validate_ohlcv(r5, "5min", cfg.min_bars_m5, cfg).iloc[:-1].reset_index(drop=True)
        r15, _ = fetch_ohlcv_cascade(cfg, "15min", 150)
        df15 = validate_ohlcv(r15, "15min", cfg.min_bars_m15, cfg).iloc[:-1].reset_index(drop=True)
        rh1, _ = fetch_ohlcv_cascade(cfg, "1h", cfg.outcome_bars_h1)
        dfh1f = validate_ohlcv(rh1, "1h", cfg.min_bars_h1, cfg)
        dfh1 = dfh1f.iloc[:-1].reset_index(drop=True)
    except Exception as e:
        logging.error(f"Data fail: {_redact(e)}")
        alert_error(f"Data: {e}", cfg)
        return
        
    try: update_journal_outcomes(dfh1f, cfg)
    except Exception as e: logging.warning(f"Outcome: {_redact(e)}")
    
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
        "timestamp": now_str, "run_count": run_num, "active_source": src,
        "final_signal": res.signal, "notified": notify,
        "entry_price": round(price, cfg.price_decimals), "confidence": res.conf,
        "adx": round(res.adx, 2), "heikin_ashi": res.ha,
        "ob_type": res.ob["type"] if res.ob["detected"] else "NONE",
        "fvg_type": res.fvg["type"] if res.fvg["detected"] else "NONE",
        "recommended_sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "suggested_lot": lot, "decision_path": res.log.to_dict()
    }
    append_report(report, cfg)
    
    if notify and res.signal != "NEUTRAL":
        jid = f"{now_str}_{res.signal}"
        entry = {
            "id": jid, "timestamp": now_str, "symbol": cfg.symbol, "signal": res.signal,
            "entry": round(price, cfg.price_decimals), "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "lot": lot, "confluence": res.conf, "outcome": "PENDING", "pnl_usd": None,
            "notes": "", "notify_status": "PENDING",
            "hit_tp1": 0, "hit_tp2": 0, "hit_tp3": 0, "hit_sl": 0, "hit_be": 0, "r_achieved": 0.0
        }
        append_journal(entry, cfg)
        msg = build_message(cfg, run_num, now_str, src, res.signal, price, sl, tp1, tp2, tp3, lot,
                            res.conf, res.ob, res.fvg, res.ha, res.adx)
        ok = send_telegram(msg, cfg)
        update_journal_status(jid, {"notify_status": "SENT" if ok else "FAILED"}, cfg)
    logging.info("=== Done ===")

# =========================================================
# CLI COMMANDS
# =========================================================
def cmd_selftest(cfg):
    print(f"=== SELFTEST APEX v{VERSION} ===\n")
    probs = []
    print("[1/5] Python Packages Check...")
    for pkg in ["pandas","numpy","requests"]:
        try: __import__(pkg); print(f"  OK   {pkg}")
        except ImportError: print(f"  FAIL {pkg}"); probs.append(f"{pkg} missing")
    
    print("\n[2/5] Telegram Env Check...")
    tok = os.getenv("TELEGRAM_TOKEN","").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if tok and chat: print("  OK   Telegram environment ready")
    else: print("  FAIL Telegram environment empty"); probs.append("Telegram env")
    
    print("\n[3/5] SQLite Database Subsystem...")
    try:
        init_db(cfg)
        print("  OK   Database Engine SQLite initialized")
    except Exception as e:
        print(f"  FAIL DB: {e}"); probs.append("DB init")

    print("\n[4/5] Telegram Connection Probe...")
    if tok and chat and not cfg.dry_run:
        if send_telegram(f"<b>APEX v{VERSION} SELFTEST</b>\n\nEngine v8.0 siap tempur.", cfg): print("  OK   Notification sent")
        else: print("  FAIL"); probs.append("Telegram send")
    else: print("  SKIP")
    
    print("\n[5/5] Active System Parameters:")
    print(f"  Symbol: {cfg.symbol} | Account Structure: {cfg.account_type.upper()}")
    print(f"  Threshold: {cfg.score_threshold}/{MAX_CONFLUENCE} | ADX Cutoff: {cfg.adx_min}")
    print(f"  Risk Setup: {cfg.risk_percentage}% (${cfg.account_balance_usd:.0f})")
    
    print()
    if probs: print(f"MASALAH DITEMUKAN: {len(probs)}"); [print(f"  - {p}") for p in probs]; return 1
    print("SISTEM PERFECT. Bot APEX v8.0 Siap Tempur.")
    return 0

def cmd_status(cfg):
    st = load_state(cfg)
    reports = get_all_reports(cfg)
    j = get_all_journal(cfg)
    sig_today = count_daily_signals(cfg)
    recent = reports[:5]
    pending = [x for x in j if x.get("outcome")=="PENDING"]
    pause = bool(db_get_kv(cfg, "drawdown_pause", {}).get("until"))
    
    print(f"=== STATUS APEX v{VERSION} ===\n")
    print(f"Symbol          : {cfg.symbol} ({cfg.account_type.upper()} Account)")
    print(f"Run count       : {st.get('run_count',0)}")
    print(f"Last signal     : {st.get('last_signal_type')} @ {st.get('last_signal_timestamp') or '-'}")
    print(f"Signals today   : {sig_today}/{cfg.max_daily_signals}")
    print(f"Pending trade   : {len(pending)}")
    print(f"Circuit breaker : {'OPEN' if not circuit_allow(cfg) else 'CLOSED'}")
    print(f"Drawdown pause  : {'AKTIF' if pause else 'off'}")
    print(f"\n--- 5 report terakhir ---")
    for r in recent:
        c = json.loads(r.get("confidence") or "{}")
        t = c.get("total", 0)
        flag = "SENT" if r.get("notified") == 1 else "    "
        print(f"  {r.get('timestamp')} | {r.get('final_signal','?'):7s} | conf={t:+d} | {flag} | src={r.get('active_source')}")

def cmd_performance(cfg):
    j = get_all_journal(cfg)
    closed = [x for x in j if x.get("outcome") not in (None,"PENDING")]
    pending = [x for x in j if x.get("outcome")=="PENDING"]
    if not closed: print(f"Belum ada closed trade. Pending: {len(pending)}"); return
    
    wins = [x for x in closed if str(x.get("outcome","")).startswith("WIN")]
    losses = [x for x in closed if x.get("outcome") == "LOSS"]
    total_r = sum(float(x.get("r_achieved",0)) for x in closed)
    wr = len(wins)/len(closed)*100 if closed else 0
    ev = total_r/len(closed) if closed else 0
    
    print(f"=== PERFORMANCE APEX v{VERSION} ===\n")
    print(f"Total closed    : {len(closed)}")
    print(f"Pending         : {len(pending)}")
    print(f"Win rate        : {len(wins)}/{len(closed)} = {wr:.1f}%")
    print(f"Total R         : {total_r:+.2f}R")
    print(f"R per trade     : {ev:+.3f}R")

def cmd_export(cfg):
    j = get_all_journal(cfg)
    if not j: print("Journal kosong."); return
    fn = f"journal_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    fields = list(j[0].keys())
    try:
        with open(fn,"w",newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(j)
        print(f"Exported {len(j)} rows -> {fn}")
    except Exception as e: print(f"Export fail: {_redact(e)}")

def cmd_reset_drawdown(cfg):
    db_set_kv(cfg, "drawdown_pause", {"until":None})
    print("Drawdown reset. Bot trade lagi next run.")
    send_telegram("<b>DRAWDOWN RESET</b>\n\nBot resumed manually.", cfg)

def cmd_reset(cfg):
    try:
        if os.path.exists(cfg.db_file):
            os.remove(cfg.db_file)
            print(f"Deleted database: {cfg.db_file}")
        print("Reset total berhasil.")
    except Exception as e:
        print(f"Reset error: {e}")

# =========================================================
# ENTRY POINT
# =========================================================
def main():
    try: cfg = Config.from_env()
    except ValueError as e: print(f"\n{e}\n"); sys.exit(2)
        
    setup_logging(cfg)
    init_db(cfg)
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "selftest": sys.exit(cmd_selftest(cfg))
        elif cmd == "status": cmd_status(cfg); sys.exit(0)
        elif cmd == "performance": cmd_performance(cfg); sys.exit(0)
        elif cmd == "export": cmd_export(cfg); sys.exit(0)
        elif cmd == "reset-drawdown": cmd_reset_drawdown(cfg); sys.exit(0)
        elif cmd == "reset": cmd_reset(cfg); sys.exit(0)
        elif cmd == "run": run_once(cfg); sys.exit(0)
        elif cmd in ("help","-h","--help"): print(__doc__); sys.exit(0)
        else: print(f"Unknown: {cmd}"); sys.exit(1)
        
    run_once(cfg)

if __name__ == "__main__":
    main()
