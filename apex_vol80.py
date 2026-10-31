"""
APEX PREDATOR v8.0 — Synthetic Vol 80 Edition (WebSocket Stabilizer)
=========================================================================
- Dedicated Deriv WebSocket Data Feed dengan Timeout & Retry Handler
- 24/7 Non-Stop Execution
- SMC, Heikin Ashi, ADX, RSI, ATR
- Telegram HTML Notification & Risk Management
=========================================================================
"""
import os
import re
import json
import time
import sys
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

VERSION = "8.0-Vol80-WS"
MAX_CONFLUENCE = 9

@dataclass
class Config:
    db_file: str = "apex_vol80_database.db"
    log_file: str = "apex_vol80.log"
    state_file: str = "apex_vol80_state.json"
    
    symbol: str = "Volatility 80 Index"
    deriv_symbol: str = "R_80"
    contract_size: float = 1.0
    account_type: str = "cent"
    price_decimals: int = 2
    
    cooldown_minutes: int = 30
    min_bars_m5: int = 60
    min_bars_m15: int = 60
    min_bars_h1: int = 60
    outcome_bars_h1: int = 150
    max_stale_mult: float = 3.0
    
    account_balance_usd: float = 10000.0
    risk_percentage: float = 1.0
    max_lot: float = 50.0
    min_lot: float = 0.50
    
    atr_sl_mult: float = 2.0
    atr_tp1_mult: float = 1.0
    atr_tp2_mult: float = 2.0
    atr_tp3_mult: float = 3.5
    
    score_threshold: int = 8
    adx_min: float = 22.0
    max_daily_signals: int = 10
    
    report_max_entries: int = 500
    dry_run: bool = False
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
                elif isinstance(cur, float): setattr(cfg, f, float(v))
                else: setattr(cfg, f, v)
            except ValueError: pass
        return cfg

def setup_logging(cfg):
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    fmt = logging.Formatter(f"%(asctime)s [%(levelname)s] [ApexVol80] - %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

def _redact(t):
    return re.sub(r"token=[^&\s'\"]+", "token=***", str(t))

def _h(s): return html.escape(str(s), quote=False)

class DataFetchError(Exception): pass

def get_db_connection(cfg):
    conn = sqlite3.connect(cfg.db_file)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(cfg):
    with get_db_connection(cfg) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS journal (
            id TEXT PRIMARY KEY, timestamp TEXT, symbol TEXT, signal TEXT,
            entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL, lot REAL,
            confluence TEXT, outcome TEXT, pnl_usd REAL, notes TEXT,
            notify_status TEXT, hit_tp1 INTEGER, hit_tp2 INTEGER,
            hit_tp3 INTEGER, hit_sl INTEGER, hit_be INTEGER, r_achieved REAL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, run_count INTEGER,
            active_source TEXT, final_signal TEXT, notified INTEGER, entry_price REAL,
            confidence TEXT, adx REAL, heikin_ashi TEXT, ob_type TEXT, fvg_type TEXT,
            recommended_sl REAL, tp1 REAL, tp2 REAL, tp3 REAL, suggested_lot REAL, decision_path TEXT
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)""")
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
        with get_db_connection(cfg) as conn:
            conn.execute("INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, json.dumps(value, default=str)))
            conn.commit()
    except Exception: pass

def load_state(cfg):
    init_db(cfg)
    return db_get_kv(cfg, "state", {"run_count": 0, "last_signal_type": "NEUTRAL", "last_signal_timestamp": ""})

def save_state(state, cfg):
    db_set_kv(cfg, "state", state)

def append_report(r, cfg):
    init_db(cfg)
    try:
        with get_db_connection(cfg) as conn:
            conn.execute("""INSERT INTO reports (timestamp, run_count, active_source, final_signal, notified, entry_price, 
                                 confidence, adx, heikin_ashi, ob_type, fvg_type, recommended_sl, tp1, tp2, tp3, suggested_lot, decision_path)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                         (r.get("timestamp"), r.get("run_count"), r.get("active_source"), r.get("final_signal"),
                          1 if r.get("notified") else 0, r.get("entry_price"), json.dumps(r.get("confidence")),
                          r.get("adx"), r.get("heikin_ashi"), r.get("ob_type"), r.get("fvg_type"),
                          r.get("recommended_sl"), r.get("tp1"), r.get("tp2"), r.get("tp3"), r.get("suggested_lot"),
                          json.dumps(r.get("decision_path"))))
            conn.commit()
    except Exception: pass

def count_daily_signals(cfg):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        with get_db_connection(cfg) as conn:
            rows = conn.execute("SELECT timestamp, final_signal, notified FROM reports").fetchall()
            return sum(1 for r in rows if str(r["timestamp"]).startswith(today) and r["final_signal"] in ("BUY","SELL") and r["notified"] == 1)
    except Exception: return 0

# FETCH DATA MENGGUNAKAN WEBSOCKET RESMI DERIV DENGAN ROTASI ENDPOINT
def fetch_deriv_ohlcv(cfg, interval_str, count=200, retries=3):
    if not HAS_WEBSOCKET: raise DataFetchError("websocket-client not installed")
    gmap = {"5min": 300, "15min": 900, "1h": 3600}
    gran = gmap.get(interval_str, 300)
    
    payload = {"ticks_history": cfg.deriv_symbol, "count": count, "end": "latest", "granularity": gran, "style": "candles"}
    
    # Rotasi endpoint untuk menghindari blokir IP datacenter
    endpoints = [
        "wss://ws.derivws.com/websockets/v3?app_id=1089",
        "wss://ws.deriv.com/websockets/v3?app_id=1089",
        "wss://blue.derivws.com/websockets/v3?app_id=1089"
    ]
    
    last_err = None
    for url in endpoints:
        for att in range(retries):
            ws = None
            try:
                ws = websocket.create_connection(url, timeout=10, header={"User-Agent": "Mozilla/5.0"})
                ws.send(json.dumps(payload))
                deadline = time.time() + 10
                while time.time() < deadline:
                    raw = ws.recv()
                    if not raw: continue
                    d = json.loads(raw)
                    if "error" in d: raise DataFetchError(d['error'].get('message'))
                    if d.get("msg_type") == "candles":
                        candles = d.get("candles", [])
                        if candles:
                            df = pd.DataFrame(candles)
                            df["datetime"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
                            for c in ["open", "high", "low", "close"]:
                                df[c] = pd.to_numeric(df[c], errors="coerce")
                            df["volume"] = 100.0
                            return df[["datetime", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
            except Exception as e:
                last_err = e
                time.sleep(1)
            finally:
                if ws:
                    try: ws.close()
                    except Exception: pass
        
    raise DataFetchError(f"All Deriv WS endpoints failed: {_redact(last_err)}")

def validate_ohlcv(df, interval_str, min_bars, cfg):
    if df is None or len(df) == 0: raise ValueError("Empty data")
    df = df.dropna(subset=["open","high","low","close"]).copy()
    if len(df) < min_bars: raise ValueError(f"Bars {len(df)} < {min_bars}")
    return df.reset_index(drop=True)

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

def get_heikin_ashi_trend(df):
    try:
        d = df.tail(200).reset_index(drop=True)
        if len(d) < 3: return "NEUTRAL"
        hac = (d["open"]+d["high"]+d["low"]+d["close"])/4.0
        hao = pd.Series(index=d.index, dtype=float)
        hao.iloc[0] = (d["open"].iloc[0]+d["close"].iloc[0])/2.0
        for i in range(1, len(d)): hao.iloc[i] = (hao.iloc[i-1]+hac.iloc[i-1])/2.0
        diff = float(hac.iloc[-1] - hao.iloc[-1])
        return "BULLISH" if diff > 0 else "BEARISH"
    except Exception: return "NEUTRAL"

def get_timeframe_trend(df):
    if df is None or len(df) < 50: return "NEUTRAL"
    c = df["close"].astype(float)
    e20, e50 = c.ewm(span=20, adjust=False).mean(), c.ewm(span=50, adjust=False).mean()
    if c.iloc[-1] > e50.iloc[-1] and e20.iloc[-1] > e50.iloc[-1]: return "BULLISH"
    if c.iloc[-1] < e50.iloc[-1] and e20.iloc[-1] < e50.iloc[-1]: return "BEARISH"
    return "NEUTRAL"

def calculate_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    v = (100 - 100/(1 + gain/(loss+1e-9))).iloc[-1]
    return 50.0 if pd.isna(v) else float(v)

def detect_institutional_order_blocks(df):
    try:
        d = df.tail(40).reset_index(drop=True)
        if len(d) < 5: return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
        bodies = (d["close"]-d["open"]).abs().iloc[:-2]
        idx = int(bodies.idxmax())
        imp, ob = d.iloc[idx], d.iloc[idx-1]
        ib, obb = imp["close"] > imp["open"], ob["close"] > ob["open"]
        hi, lo = float(max(ob["open"], ob["close"])), float(min(ob["open"], ob["close"]))
        cc = float(d["close"].iloc[-1])
        if ib and not obb and cc > hi: return {"detected": True, "type": "BULLISH_OB", "high": hi, "low": lo}
        if not ib and obb and cc < lo: return {"detected": True, "type": "BEARISH_OB", "high": hi, "low": lo}
        return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
    except Exception: return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}

def compute_confluence(df_m5, df_m15, df_h1, ob, adx, ha):
    c = df_m5["close"].astype(float)
    tr = [get_timeframe_trend(df_m5), get_timeframe_trend(df_m15), get_timeframe_trend(df_h1)]
    tp = sum(1 for x in tr if x == "BULLISH") - sum(1 for x in tr if x == "BEARISH")
    e9, e21 = c.ewm(span=9, adjust=False).mean(), c.ewm(span=21, adjust=False).mean()
    rsi = calculate_rsi(c)
    m = 1 if e9.iloc[-1] > e21.iloc[-1] else -1
    s = (2 if ob["type"] == "BULLISH_OB" else -2) if ob["detected"] else 0
    ap = 1 if adx >= 25 else 0
    hp = 1 if ha == "BULLISH" else (-1 if ha == "BEARISH" else 0)
    return {"total": tp+m+s+ap+hp, "rsi": round(rsi, 1)}

def generate_signal(df_m5, df_m15, df_h1, cfg):
    adx = calculate_adx(df_m5)
    if adx < cfg.adx_min: return "NEUTRAL", {}, adx, "NEUTRAL", {}
    ob = detect_institutional_order_blocks(df_m5)
    ha = get_heikin_ashi_trend(df_m5)
    conf = compute_confluence(df_m5, df_m15, df_h1, ob, adx, ha)
    
    if conf["total"] >= cfg.score_threshold and ha == "BULLISH": return "BUY", conf, adx, ha, ob
    if conf["total"] <= -cfg.score_threshold and ha == "BEARISH": return "SELL", conf, adx, ha, ob
    return "NEUTRAL", conf, adx, ha, ob

def calculate_tpsl(df_m5, signal_type, cfg):
    atr = compute_atr(df_m5, 14)
    price = float(df_m5["close"].iloc[-1])
    if atr <= 0 or signal_type == "NEUTRAL": return 0.0, 0.0, 0.0, 0.0, 0.0
    sl_d = atr * cfg.atr_sl_mult
    if signal_type == "BUY":
        sl, tp1, tp2, tp3 = price - sl_d, price + atr*cfg.atr_tp1_mult, price + atr*cfg.attrs if hasattr(cfg, 'attrs') else price + atr*cfg.atr_tp2_mult, price + atr*cfg.atr_tp3_mult
    else:
        sl, tp1, tp2, tp3 = price + sl_d, price - atr*cfg.atr_tp1_mult, price - atr*cfg.atr_tp2_mult, price - atr*cfg.atr_tp3_mult
    
    risk = cfg.account_balance_usd * (cfg.risk_percentage / 100.0)
    raw = risk / (sl_d * cfg.contract_size) if sl_d > 0 else 0.5
    if cfg.account_type.lower() == "cent": raw *= 100.0
    lot = round(max(cfg.min_lot, min(cfg.max_lot, raw)), 2)
    p = cfg.price_decimals
    return round(sl, p), round(tp1, p), round(tp2, p), round(tp3, p), lot

def send_telegram(msg, cfg):
    if cfg.dry_run: return True
    tok, chat = os.getenv("TELEGRAM_TOKEN", "").strip(), os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not tok or not chat: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage", json={"chat_id": chat, "text": msg, "parse_mode": "HTML"}, timeout=10)
        return r.status_code == 200
    except Exception: return False

def run_once(cfg):
    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    st = load_state(cfg)
    st["run_count"] = int(st.get("run_count", 0)) + 1
    save_state(st, cfg)
    
    if count_daily_signals(cfg) >= cfg.max_daily_signals: return
    
    try:
        df5 = validate_ohlcv(fetch_deriv_ohlcv(cfg, "5min", 200), "5min", cfg.min_bars_m5, cfg)
        df15 = validate_ohlcv(fetch_deriv_ohlcv(cfg, "15min", 150), "15min", cfg.min_bars_m15, cfg)
        dfh1 = validate_ohlcv(fetch_deriv_ohlcv(cfg, "1h", cfg.outcome_bars_h1), "1h", cfg.min_bars_h1, cfg)
    except Exception as e:
        logging.error(f"Data fetch fail: {e}")
        return
        
    price = float(df5["close"].iloc[-1])
    signal, conf, adx, ha, ob = generate_signal(df5, df15, dfh1, cfg)
    
    notify = False
    if signal != "NEUTRAL" and st.get("last_signal_type") != signal:
        st["last_signal_type"] = signal
        st["last_signal_timestamp"] = now_str
        save_state(st, cfg)
        notify = True
        
    sl, tp1, tp2, tp3, lot = calculate_tpsl(df5, signal, cfg)
    append_report({"timestamp": now_str, "run_count": st["run_count"], "active_source": "Deriv-WS-Multi", "final_signal": signal, "notified": notify, "entry_price": price}, cfg)
    
    if notify:
        msg = f"<b>APEX VOL 80 v{VERSION}</b>\n\n<b>{signal}</b> @ <code>{price}</code>\nSL: <code>{sl}</code>\nTP1: <code>{tp1}</code>\nLot: <code>{lot}</code>"
        send_telegram(msg, cfg)

def main():
    cfg = Config.from_env()
    setup_logging(cfg)
    init_db(cfg)
    run_once(cfg)

if __name__ == "__main__":
    main()
