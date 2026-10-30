import os, re, json, time, sys, csv, logging, html
import requests, numpy as np, pandas as pd
import fcntl
import websocket
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

VERSION = "7.2"
MAX_CONFLUENCE = 9
HAS_FCNTL = True
HAS_WEBSOCKET = True

@dataclass
class Config:
    state_file: str = "signal_state.json"
    report_json: str = "xauusd_apex_report.json"
    journal_file: str = "trade_journal.json"
    log_file: str = "apex.log"
    symbol: str = "XAUUSD"
    deriv_symbol: str = "frxXAUUSD"
    contract_size: float = 100.0
    price_decimals: int = 3
    cooldown_minutes: int = 60
    min_bars_m5: int = 60
    min_bars_m15: int = 60
    min_bars_h1: int = 60
    outcome_bars_h1: int = 150
    max_stale_mult: float = 3.0
    max_gap_ratio: float = 0.10
    account_balance_usd: float = 10000.0
    risk_percentage: float = 1.0
    max_lot: float = 0.5
    min_lot: float = 0.01
    atr_sl_mult: float = 2.0
    atr_tp1_mult: float = 1.0
    atr_tp2_mult: float = 2.0
    atr_tp3_mult: float = 3.5
    spread_buffer_dollars: float = 0.30
    score_threshold: int = 8
    adx_min: float = 22.0
    adx_strong: float = 25.0
    max_daily_signals: int = 5
    news_blackout_minutes: int = 30
    news_cache_seconds: int = 1800
    news_fail_mode: str = "closed"
    report_max_entries: int = 500
    dry_run: bool = False
    enable_deriv: bool = True
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
                    if not np.isfinite(fv): continue
                    setattr(cfg, f, fv)
                else: setattr(cfg, f, v)
            except ValueError: pass
        return cfg

def setup_logging(cfg):
    root = logging.getLogger()
    root.setLevel(logging.INFO)
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

@contextmanager
def file_lock(path):
    if not HAS_FCNTL:
        yield
        return
    lock_path = path + ".lock"
    with open(lock_path, "w") as lf:
        try: os.chmod(lock_path, 0o600)
        except Exception: pass
        try:
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try: fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
            except Exception: pass

def _atomic_write_json(data, path):
    temp = path + ".tmp"
    try:
        with open(temp, "w") as f:
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        try: os.chmod(temp, 0o600)
        except Exception: pass
        os.replace(temp, path)
    except Exception as e:
        logging.error(f"Write {path} failed: {_redact(e)}")

def _safe_read_json(path, default):
    if not os.path.exists(path): return default
    try:
        with open(path) as f:
            c = f.read().strip()
            return json.loads(c) if c else default
    except json.JSONDecodeError:
        try: os.rename(path, f"{path}.corrupt.{int(time.time())}")
        except Exception: pass
        return default
    except Exception: return default

def _default_state():
    return {"run_count":0,"last_signal_type":"NEUTRAL","last_signal_timestamp":"","consecutive_errors":0}

def load_state(cfg):
    d = _safe_read_json(cfg.state_file, _default_state())
    return {**_default_state(), **d} if isinstance(d, dict) else _default_state()

def save_state(state, cfg):
    with file_lock(cfg.state_file):
        _atomic_write_json(state, cfg.state_file)

def append_report(r, cfg):
    with file_lock(cfg.report_json):
        h = _safe_read_json(cfg.report_json, [])
        if isinstance(h, dict): h = [h]
        if not isinstance(h, list): h = []
        h.append(r)
        h = h[-cfg.report_max_entries:]
        _atomic_write_json(h, cfg.report_json)

def append_journal(e, cfg):
    with file_lock(cfg.journal_file):
        j = _safe_read_json(cfg.journal_file, [])
        if not isinstance(j, list): j = []
        j.append(e)
        _atomic_write_json(j, cfg.journal_file)

def update_journal_status(jid, updates, cfg):
    with file_lock(cfg.journal_file):
        j = _safe_read_json(cfg.journal_file, [])
        if not isinstance(j, list): return
        for e in j:
            if isinstance(e, dict) and e.get("id") == jid:
                e.update(updates)
                break
        _atomic_write_json(j, cfg.journal_file)

def count_daily_signals(cfg):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    reports = _safe_read_json(cfg.report_json, [])
    if not isinstance(reports, list): return 0
    return sum(1 for r in reports if isinstance(r, dict) and r.get("timestamp","").startswith(today) and r.get("final_signal") in ("BUY","SELL") and r.get("notified", False))

def _circuit_path(cfg): return cfg.state_file + ".breaker"

def _load_circuit(cfg):
    d = _safe_read_json(_circuit_path(cfg), {"failures":0,"opened_until":None})
    return d if isinstance(d, dict) else {"failures":0,"opened_until":None}

def circuit_allow(cfg):
    c = _load_circuit(cfg)
    if c.get("opened_until"):
        try:
            if datetime.now(timezone.utc) < datetime.fromisoformat(c["opened_until"]): return False
        except Exception: pass
    return True

def circuit_record_failure(cfg, threshold=3, cooldown_min=15):
    c = _load_circuit(cfg)
    f = int(c.get("failures",0)) + 1
    opened = c.get("opened_until")
    if f >= threshold:
        opened = (datetime.now(timezone.utc) + timedelta(minutes=cooldown_min)).isoformat()
        f = 0
    with file_lock(_circuit_path(cfg)):
        _atomic_write_json({"failures":f,"opened_until":opened}, _circuit_path(cfg))

def circuit_record_success(cfg):
    with file_lock(_circuit_path(cfg)):
        _atomic_write_json({"failures":0,"opened_until":None}, _circuit_path(cfg))

def _pause_path(cfg): return cfg.state_file + ".pause"

def should_block_due_to_drawdown(cfg):
    if not cfg.drawdown_guard_enabled: return False, ""
    p = _safe_read_json(_pause_path(cfg), {})
    if p.get("until"):
        try:
            if datetime.now(timezone.utc) < datetime.fromisoformat(p["until"]): return True, f"pause until {p['until']}"
        except Exception: pass
    j = _safe_read_json(cfg.journal_file, [])
    if not isinstance(j, list): return False, ""
    closed = [x for x in j if isinstance(x, dict) and x.get("outcome") not in (None,"PENDING")]
    closed.sort(key=lambda x: x.get("timestamp",""), reverse=True)
    consec = 0
    for x in closed:
        if x.get("outcome") == "LOSS": consec += 1
        else: break
    if consec >= cfg.drawdown_max_consecutive_losses:
        until = (datetime.now(timezone.utc) + timedelta(hours=cfg.drawdown_pause_hours)).isoformat()
        with file_lock(_pause_path(cfg)):
            _atomic_write_json({"until":until,"reason":f"{consec} losses","created_at":datetime.now(timezone.utc).isoformat()}, _pause_path(cfg))
        try: send_telegram(f"<b>DRAWDOWN GUARD</b>\n\n{consec} loss berturut. Bot PAUSE {cfg.drawdown_pause_hours}h.", cfg)
        except Exception: pass
        return True, f"{consec} losses -> pause"
    return False, ""

def _heartbeat_path(cfg): return cfg.state_file + ".heartbeat"

def maybe_send_heartbeat(cfg):
    if not cfg.heartbeat_enabled: return
    now = datetime.now(timezone.utc)
    if now.hour != cfg.heartbeat_hour_utc: return
    today = now.strftime("%Y-%m-%d")
    hb = _safe_read_json(_heartbeat_path(cfg), {})
    if hb.get("last_sent_date") == today: return
    msg = f"<b>APEX HEARTBEAT</b>\nBot hidup.\n<code>{now.strftime('%H:%M')} UTC</code>"
    if send_telegram(msg, cfg):
        _atomic_write_json({"last_sent_date":today,"last_sent_at":now.isoformat()}, _heartbeat_path(cfg))

def _candles_to_df(candles):
    df = pd.DataFrame(candles)
    df["datetime"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    for c in ["open","high","low","close"]: df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = 100.0
    return df[["datetime","open","high","low","close","volume"]].reset_index(drop=True)

def fetch_deriv_ohlcv(cfg, interval_str, count=200, retries=2):
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
            last = e; logging.warning(f"Deriv att{att+1}: {_redact(e)}")
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
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}", params={"interval":iv,"range":"5d"}, headers=hdrs, timeout=10)
        r.raise_for_status(); data = r.json()
        result = data.get("chart",{}).get("result")
        if not result: raise DataFetchError(f"Yahoo {sym}: no result")
        res = result[0]; ts = res.get("timestamp",[])
        q = res.get("indicators",{}).get("quote",[{}])[0]
        op,hi,lo,cl,vo = (q.get(k,[]) for k in ("open","high","low","close","volume"))
        rows = [{"epoch":int(t),"open":op[i],"high":hi[i],"low":lo[i],"close":cl[i],"volume":vo[i] if i < len(vo) and vo[i] else 100.0} for i,t in enumerate(ts) if i < len(op) and op[i] is not None and cl[i] is not None]
        if not rows: raise DataFetchError(f"Yahoo {sym}: no data")
        return _candles_to_df(rows)
    cache = {"df":None}
    try:
        cache["df"] = _fetch("XAUUSD=X")
        return cache["df"].tail(count).reset_index(drop=True)
    except Exception as e: logging.warning(f"Yahoo spot failed: {_redact(e)}")
    try:
        df = _fetch("GC=F")
        spot = None
        if cache["df"] is not None: spot = float(cache["df"]["close"].iloc[-1])
        else:
            try:
                r = requests.get("https://xaus.com/api/v1/spot", timeout=5); r.raise_for_status()
                spot = float(r.json().get("spot_usd_oz",0))
            except Exception: spot = float(df["close"].iloc[-1]) + float(os.getenv("APEX_YAHOO_OFFSET","0"))
        if spot and spot > 0:
            off = spot - float(df["close"].iloc[-1])
            for c in ["open","high","low","close"]: df[c] = df[c] + off
        return df.tail(count).reset_index(drop=True)
    except Exception as e: raise DataFetchError(f"Yahoo all failed: {_redact(e)}")

def fetch_twelvedata_ohlcv(cfg, interval_str, count=200, retries=2):
    key = os.getenv("TWELVEDATA_API_KEY","").strip()
    if not key: raise DataFetchError("TWELVEDATA_API_KEY not set")
    imap = {"5min":"5min","15min":"15min","1h":"1h"}
    iv = imap.get(interval_str)
    if iv is None: raise DataFetchError(f"Interval {interval_str} not supported")
    last = None
    for att in range(retries+1):
        try:
            r = requests.get("https://api.twelvedata.com/time_series", params={"symbol":"XAU/USD","interval":iv,"outputsize":count,"apikey":key,"timezone":"UTC"}, timeout=10)
            if r.status_code in (401,403): raise DataFetchError("TwelveData: invalid key")
            r.raise_for_status(); d = r.json()
            if "values" not in d: raise DataFetchError(f"TwelveData: {d.get('message','?')}")
            df = pd.DataFrame(d["values"])
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            for c in ["open","high","low","close"]: df[c] = pd.to_numeric(df[c], errors="coerce")
            df["volume"] = 100.0
            return df.sort_values("datetime").reset_index(drop=True)[["datetime","open","high","low","close","volume"]]
        except DataFetchError: raise
        except Exception as e:
            last = e
            if att < retries: time.sleep(2**att)
    raise DataFetchError(f"TwelveData: {_redact(last)}")

def fetch_ohlcv_cascade(cfg, interval_str, count=200):
    if not circuit_allow(cfg): raise DataFetchError("Circuit breaker active")
    errors = []
    srcs = []
    if cfg.enable_deriv: srcs.append(("Deriv", fetch_deriv_ohlcv))
    srcs.append(("Yahoo", fetch_yahoo_ohlcv))
    srcs.append(("TwelveData", fetch_twelvedata_ohlcv))
    for name, fn in srcs:
        try:
            df = fn(cfg, interval_str, count)
            circuit_record_success(cfg)
            return df, name
        except Exception as e:
            errors.append(f"{name}: {_redact(e)}")
    circuit_record_failure(cfg)
    raise DataFetchError("ALL FEEDS FAILED | " + " || ".join(errors))

_INTERVAL_SECS = {"5min":300,"15min":900,"1h":3600}

def validate_ohlcv(df, interval_str, min_bars, cfg):
    if df is None or len(df) == 0: raise ValueError("Empty data")
    df = df.dropna(subset=["open","high","low","close"]).copy()
    if len(df) < min_bars: raise ValueError(f"Bars {len(df)} < {min_bars}")
    isec = _INTERVAL_SECS.get(interval_str, 300)
    lt = df["datetime"].iloc[-1]
    if lt.tzinfo is None: lt = lt.replace(tzinfo=timezone.utc)
    if (datetime.now(timezone.utc) - lt).total_seconds() > isec * cfg.max_stale_mult: raise ValueError(f"Stale")
    return df.reset_index(drop=True)

def is_market_open(now_utc):
    wd, h = now_utc.weekday(), now_utc.hour
    if wd == 5: return False
    if wd == 6: return h >= 22
    if wd == 4: return h < 21
    return True

def is_institutional_killzone(now_utc):
    return (7 <= now_utc.hour <= 11) or (13 <= now_utc.hour <= 17)

def _news_cache_path(cfg): return cfg.state_file + ".news"

def check_news_shield(cfg):
    now = datetime.now(timezone.utc)
    cache = _safe_read_json(_news_cache_path(cfg), {})
    if cache.get("ts"):
        try:
            if (now - datetime.fromisoformat(cache["ts"])).total_seconds() < cfg.news_cache_seconds:
                return cache.get("blocked", False), cache.get("reason", "")
        except Exception: pass
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=5)
        r.raise_for_status(); events = r.json()
        for e in events:
            if e.get("country") != "USD" or e.get("impact") != "High": continue
            try: ev_dt = datetime.strptime(e["date"], "%Y-%m-%dT%H:%M:%S%z")
            except Exception:
                try: ev_dt = datetime.fromisoformat(e["date"].replace("Z","+00:00"))
                except Exception: continue
            if abs((ev_dt - now).total_seconds()/60) <= cfg.news_blackout_minutes:
                _atomic_write_json({"ok":True,"ts":now.isoformat(),"blocked":True,"reason":"news"}, _news_cache_path(cfg))
                return True, "news"
        _atomic_write_json({"ok":True,"ts":now.isoformat(),"blocked":False,"reason":""}, _news_cache_path(cfg))
        return False, ""
    except Exception:
        if cfg.news_fail_mode == "closed":
            return True, "API down"
        return False, ""

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
        pdm = up.where((up>dn)&(up>0), 0.0); mdm = dn.where((dn>up)&(dn>0), 0.0)
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
    e20 = c.ewm(span=20, adjust=False).mean(); e50 = c.ewm(span=50, adjust=False).mean()
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
        d = df.tail(30).reset_index(drop=True)
        if len(d) < 3: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
        atr = compute_atr(df, 14)
        if atr <= 0: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
        bodies = (d["close"]-d["open"]).abs().iloc[:-1]
        if bodies.empty: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
        idx = int(bodies.idxmax())
        if idx == 0 or idx < len(d)-15: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
        imp = d.iloc[idx]; ob = d.iloc[idx-1]
        if abs(float(imp["close"]-imp["open"])) < atr: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
        ib = imp["close"] > imp["open"]; obb = ob["close"] > ob["open"]
        hi = float(max(ob["open"], ob["close"])); lo = float(min(ob["open"], ob["close"]))
        if ib and not obb: return {"detected":True,"type":"BULLISH_OB","high":hi,"low":lo}
        if not ib and obb: return {"detected":True,"type":"BEARISH_OB","high":hi,"low":lo}
        return {"detected":False,"type":"NONE","high":0.0,"low":0.0}
    except Exception: return {"detected":False,"type":"NONE","high":0.0,"low":0.0}

def compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx, ha):
    c = df_m5["close"].astype(float)
    tr = [get_timeframe_trend(df_m5), get_timeframe_trend(df_m15), get_timeframe_trend(df_h1)]
    tp = sum(1 for x in tr if x == "BULLISH") - sum(1 for x in tr if x == "BEARISH")
    e9 = c.ewm(span=9, adjust=False).mean(); e21 = c.ewm(span=21, adjust=False).mean()
    rsi = calculate_rsi(c)
    m = 0
    m += 1 if e9.iloc[-1] > e21.iloc[-1] else -1
    if rsi < 40: m += 1
    elif rsi > 60: m -= 1
    s = 0
    if ob["detected"]: s += 2 if ob["type"] == "BULLISH_OB" else -2
    elif fvg["detected"]: s += 1 if fvg["type"] == "BULLISH_FVG" else -1
    ap = 1 if adx >= 25 else (0 if adx >= 20 else -1)
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
    signal: str; reason: dict; log: DecisionLog; conf: dict; adx: float; ha: str; ob: dict; fvg: dict

_EOB = {"detected":False,"type":"NONE","high":0.0,"low":0.0}
_EFVG = {"detected":False,"type":"NONE","level":0.0}

def generate_signal(df_m5, df_m15, df_h1, cfg):
    log = DecisionLog(); adx = calculate_adx(df_m5)
    if not log.check("ADX", adx >= cfg.adx_min, f"{adx:.1f}>={cfg.adx_min}"):
        return SignalResult("NEUTRAL",{"reason":f"ADX {adx:.1f}"},log,None,adx,"NEUTRAL",_EOB,_EFVG)
    ob = detect_institutional_order_blocks(df_m5); fvg = detect_fair_value_gap(df_m5)
    if not log.check("SMC", ob["detected"] or fvg["detected"], ob["type"] if ob["detected"] else fvg["type"]):
        return SignalResult("NEUTRAL",{"reason":"no OB/FVG"},log,None,adx,"NEUTRAL",ob,fvg)
    ha = get_heikin_ashi_trend(df_m5)
    conf = compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx, ha)
    if conf["total"] >= cfg.score_threshold and ha == "BULLISH" and ob["type"] in ("BULLISH_OB","NONE"):
        log.check("Confluence",True,f"+{conf['total']}"); log.check("HA",True,ha)
        return SignalResult("BUY",{},log,conf,adx,ha,ob,fvg)
    if conf["total"] <= -cfg.score_threshold and ha == "BEARISH" and ob["type"] in ("BEARISH_OB","NONE"):
        log.check("Confluence",True,f"{conf['total']}"); log.check("HA",True,ha)
        return SignalResult("SELL",{},log,conf,adx,ha,ob,fvg)
    return SignalResult("NEUTRAL",{"reason":"low conf"},log,conf,adx,ha,ob,fvg)

def calculate_multi_target_tpsl(df_m5, signal_type, cfg):
    atr = compute_atr(df_m5, 14); price = float(df_m5["close"].iloc[-1])
    if atr <= 0 or signal_type == "NEUTRAL": return 0.0,0.0,0.0,0.0,0.0
    sl_d = atr*cfg.atr_sl_mult
    tp1_d = max(atr*cfg.atr_tp1_mult, cfg.spread_buffer_dollars*2)
    tp2_d = atr*cfg.atr_tp2_mult; tp3_d = atr*cfg.atr_tp3_mult
    if signal_type == "BUY": sl = price-sl_d; tp1,tp2,tp3 = price+tp1_d, price+tp2_d, price+tp3_d
    else: sl = price+sl_d; tp1,tp2,tp3 = price-tp1_d, price-tp2_d, price-tp3_d
    risk = cfg.account_balance_usd * (cfg.risk_percentage/100.0)
    raw = risk/(sl_d*cfg.contract_size) if sl_d > 0 else 0.0
    lot = round(max(cfg.min_lot, min(cfg.max_lot, raw)), 2)
    p = cfg.price_decimals
    return round(sl,p), round(tp1,p), round(tp2,p), round(tp3,p), lot

def update_journal_outcomes(df_h1_full, cfg):
    if not cfg.track_outcomes: return
    j = _safe_read_json(cfg.journal_file, [])
    if not isinstance(j, list): return
    changed = False
    for e in j:
        if not isinstance(e, dict) or e.get("outcome") != "PENDING": continue
        try: entry_ts = pd.Timestamp(e["timestamp"], tz="UTC")
        except Exception: continue
        bars = df_h1_full[df_h1_full["datetime"] > entry_ts.floor("h")]
        if len(bars) == 0: continue
        d = e.get("signal"); ep = float(e.get("entry",0)); sl = float(e.get("sl",0)); tp1 = float(e.get("tp1",0))
        if ep <= 0 or sl <= 0 or tp1 <= 0: continue
        ht1 = e.get("hit_tp1",False); hsl = e.get("hit_sl",False)
        for _, b in bars.iterrows():
            if ht1 or hsl: break
            if d == "BUY":
                if b["low"] <= sl: hsl = True; e["outcome"] = "LOSS"; e["r_achieved"] = -1.0; changed = True; break
                if b["high"] >= tp1: ht1 = True; e["hit_tp1"] = True; changed = True; break
            else:
                if b["high"] >= sl: hsl = True; e["outcome"] = "LOSS"; e["r_achieved"] = -1.0; changed = True; break
                if b["low"] <= tp1: ht1 = True; e["hit_tp1"] = True; changed = True; break
    if changed: _atomic_write_json(j, cfg.journal_file)

def send_telegram(msg, cfg):
    tok = os.getenv("TELEGRAM_TOKEN","").strip(); chat = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not tok or not chat: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage", json={"chat_id":chat,"text":msg,"parse_mode":"HTML"}, timeout=10)
        return r.status_code == 200
    except: return False

def should_notify(state, signal_type, now_utc, cfg):
    if signal_type == "NEUTRAL": return False
    if state.get("last_signal_type") != signal_type: return True
    lts = state.get("last_signal_timestamp","")
    if not lts: return True
    try:
        ld = datetime.strptime(lts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return (now_utc - ld) >= timedelta(minutes=cfg.cooldown_minutes)
    except Exception: return True

def run_once(cfg):
    now = datetime.now(timezone.utc); now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    st = load_state(cfg)
    st["run_count"] = int(st.get("run_count",0))+1
    _atomic_write_json(st, cfg.state_file)
    logging.info(f"=== Run #{st['run_count']} ===")
    try: maybe_send_heartbeat(cfg)
    except Exception: pass
    if not is_market_open(now): return
    if not is_institutional_killzone(now): return
    try: blocked, _ = check_news_shield(cfg)
    except Exception: blocked = True
    if blocked: return
    try:
        r5, src = fetch_ohlcv_cascade(cfg, "5min", 200)
        df5 = validate_ohlcv(r5, "5min", 60, cfg).iloc[:-1]
        r15, _ = fetch_ohlcv_cascade(cfg, "15min", 150)
        df15 = validate_ohlcv(r15, "15min", 60, cfg).iloc[:-1]
        rh1, _ = fetch_ohlcv_cascade(cfg, "1h", 150)
        dfh1 = validate_ohlcv(rh1, "1h", 60, cfg).iloc[:-1]
    except Exception as e:
        logging.error(f"Data fail: {e}")
        return
    try: update_journal_outcomes(dfh1, cfg)
    except Exception: pass
    price = float(df5["close"].iloc[-1])
    res = generate_signal(df5, df15, dfh1, cfg)
    notify = False
    if res.signal != "NEUTRAL" and should_notify(st, res.signal, now, cfg):
        st["last_signal_type"] = res.signal; st["last_signal_timestamp"] = now_str
        _atomic_write_json(st, cfg.state_file); notify = True
    sl, tp1, tp2, tp3, lot = calculate_multi_target_tpsl(df5, res.signal, cfg)
    append_report({"timestamp":now_str,"final_signal":res.signal,"notified":notify,"entry_price":price}, cfg)
    if notify:
        msg = f"<b>APEX v{VERSION}</b>\n\n{res.signal} @ {price:.3f}\nSL: {sl}\nTP1: {tp1}\nTP2: {tp2}\nTP3: {tp3}\nLot: {lot}"
        if send_telegram(msg, cfg):
            append_journal({"id":now_str,"timestamp":now_str,"signal":res.signal,"entry":price,"sl":sl,"tp1":tp1,"outcome":"PENDING","hit_tp1":False,"hit_sl":False}, cfg)
            logging.info("Signal sent to Telegram!")
    logging.info("=== Done ===")

def cmd_selftest(cfg):
    print(f"=== SELFTEST APEX v{VERSION} ===\n")
    tok = os.getenv("TELEGRAM_TOKEN","").strip(); chat = os.getenv("TELEGRAM_CHAT_ID","").strip()
    print(f"Env: Token={'OK' if tok else 'EMPTY'}, Chat={'OK' if chat else 'EMPTY'}")
    if send_telegram("<b>TEST SUKSES</b>", cfg): print("Telegram OK")
    else: print("Telegram GAGAL")

def cmd_status(cfg):
    st = load_state(cfg)
    print(f"=== STATUS APEX v{VERSION} ===\n")
    print(f"Run count       : {st.get('run_count',0)}")
    print(f"Last signal     : {st.get('last_signal_type')} @ {st.get('last_signal_timestamp') or '-'}")
