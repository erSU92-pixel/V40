"""
APEX PREDATOR v10.1 — Vol 80 GitHub Actions Edition
=====================================================
- Deriv SDK resmi (async, endpoint publik)
- State persisten via Turso
- One-shot execution (cocok untuk cron GitHub Actions)
- SMC, Heikin Ashi, ADX, RSI, ATR
"""
import os, re, json, logging, html
import asyncio, requests, numpy as np, pandas as pd
from dataclasses import dataclass
from datetime import datetime, timezone
from deriv_sdk import DerivClient

VERSION = "10.1-Vol80-GHA"

@dataclass
class Config:
    deriv_symbol: str = "R_80"
    fallback_symbols: tuple = ("R_80", "R_75", "R_100")
    price_decimals: int = 2
    min_bars_m5: int = 60
    min_bars_m15: int = 60
    min_bars_h1: int = 60
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
    drawdown_max_losses: int = 3
    drawdown_pause_hours: int = 12
    dry_run: bool = False

    @classmethod
    def from_env(cls):
        cfg = cls()
        for f in cls.__dataclass_fields__:
            v = os.getenv(f"APEX_{f.upper()}")
            if v is None: continue
            cur = getattr(cfg, f)
            try:
                if isinstance(cur, bool): setattr(cfg, f, v.lower() in ("1","true","yes"))
                elif isinstance(cur, int): setattr(cfg, f, int(v))
                elif isinstance(cur, float): setattr(cfg, f, float(v))
                else: setattr(cfg, f, v)
            except ValueError: pass
        return cfg

def setup_logging():
    logging.basicConfig(level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [ApexVol80] %(message)s")

def _redact(t): return re.sub(r"token=[^&\s'\"]+", "token=***", str(t))

# ---------- Turso (opsional, untuk persistensi state) ----------
class TursoDB:
    def __init__(self):
        self.url = os.getenv("TURSO_DB_URL", "").rstrip("/")
        self.token = os.getenv("TURSO_AUTH_TOKEN", "")
        self.enabled = bool(self.url and self.token)

    def _exec(self, sql, args=None):
        if not self.enabled: return None
        payload = {"requests": [{"type": "execute", "stmt": {
            "sql": sql,
            "args": [{"type": "text" if isinstance(a, str) else "float", "value": str(a)} for a in (args or [])]
        }}]}
        try:
            r = requests.post(f"{self.url}/v2/pipeline", json=payload, headers={
                "Authorization": f"Bearer {self.token}", "Content-Type": "application/json"
            }, timeout=10)
            return r.json() if r.status_code == 200 else None
        except Exception as e:
            logging.warning(f"Turso error: {_redact(e)}")
            return None

    def init(self):
        if not self.enabled: return
        self._exec("CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT)")
        self._exec("""CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT,
            signal TEXT, notified INTEGER, entry REAL, sl REAL, tp1 REAL, tp2 REAL,
            tp3 REAL, lot REAL, confidence TEXT, adx REAL, ha TEXT, ob TEXT, path TEXT)""")

    def get_state(self, key, default):
        if not self.enabled: return default
        res = self._exec(f"SELECT value FROM state WHERE key='{key}'")
        try:
            rows = res["results"][0]["response"]["result"]["rows"]
            return json.loads(rows[0][0]) if rows else default
        except Exception: return default

    def set_state(self, key, val):
        if not self.enabled: return
        self._exec("INSERT OR REPLACE INTO state (key,value) VALUES (?,?)", [key, json.dumps(val, default=str)])

    def insert_report(self, r):
        if not self.enabled: return
        self._exec("""INSERT INTO reports (timestamp,symbol,signal,notified,entry,sl,tp1,tp2,tp3,lot,confidence,adx,ha,ob,path)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [r["timestamp"], r["symbol"], r["signal"], r["notified"], r["entry"],
             r["sl"], r["tp1"], r["tp2"], r["tp3"], r["lot"],
             json.dumps(r["confidence"]), r["adx"], r["ha"], r["ob"], json.dumps(r["path"])])

    def daily_signal_count(self):
        if not self.enabled: return 0
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        res = self._exec(f"SELECT COUNT(*) FROM reports WHERE timestamp LIKE '{today}%' AND signal IN ('BUY','SELL') AND notified=1")
        try: return int(res["results"][0]["response"]["result"]["rows"][0][0])
        except Exception: return 0

    def drawdown_paused(self):
        st = self.get_state("drawdown", {"pause_until": None, "losses": 0})
        until = st.get("pause_until")
        if until:
            try:
                if datetime.fromisoformat(until) > datetime.now(timezone.utc): return True
            except Exception: pass
        return False

# ---------- Indikator ----------
def atr(df, period=14):
    h,l,c = df["high"], df["low"], df["close"]
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    v = tr.ewm(alpha=1/period, adjust=False).mean().iloc[-1]
    return 0.0 if pd.isna(v) else float(v)

def adx(df, period=14):
    h,l = df["high"], df["low"]
    up, dn = h.diff(), -l.diff()
    pdm = up.where((up>dn)&(up>0), 0.0); mdm = dn.where((dn>up)&(dn>0), 0.0)
    tr = pd.concat([h-l, (h-l.shift()).abs(), (l-l.shift()).abs()], axis=1).max(axis=1)
    a = tr.ewm(alpha=1/period, adjust=False).mean().replace(0, np.nan)
    pdi = 100*pdm.ewm(alpha=1/period, adjust=False).mean()/a
    mdi = 100*mdm.ewm(alpha=1/period, adjust=False).mean()/a
    dx = (pdi-mdi).abs()/((pdi+mdi).replace(0, np.nan))*100
    v = dx.ewm(alpha=1/period, adjust=False).mean().iloc[-1]
    return 0.0 if pd.isna(v) else float(v)

def rsi(close, period=14):
    d = close.diff()
    g = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    v = (100 - 100/(1 + g/(l+1e-9))).iloc[-1]
    return 50.0 if pd.isna(v) else float(v)

def heikin_trend(df):
    d = df.tail(200).reset_index(drop=True)
    if len(d) < 3: return "NEUTRAL"
    hac = (d["open"]+d["high"]+d["low"]+d["close"])/4.0
    hao = pd.Series(index=d.index, dtype=float)
    hao.iloc[0] = (d["open"].iloc[0]+d["close"].iloc[0])/2
    for i in range(1, len(d)): hao.iloc[i] = (hao.iloc[i-1]+hac.iloc[i-1])/2
    return "BULLISH" if float(hac.iloc[-1]-hao.iloc[-1]) > 0 else "BEARISH"

def tf_trend(df):
    if df is None or len(df) < 50: return "NEUTRAL"
    c = df["close"].astype(float)
    e20, e50 = c.ewm(span=20, adjust=False).mean(), c.ewm(span=50, adjust=False).mean()
    if c.iloc[-1] > e50.iloc[-1] and e20.iloc[-1] > e50.iloc[-1]: return "BULLISH"
    if c.iloc[-1] < e50.iloc[-1] and e20.iloc[-1] < e50.iloc[-1]: return "BEARISH"
    return "NEUTRAL"

def order_block(df):
    d = df.tail(40).reset_index(drop=True)
    if len(d) < 5: return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
    bodies = (d["close"]-d["open"]).abs().iloc[:-2]
    idx = int(bodies.idxmax()); imp, ob = d.iloc[idx], d.iloc[idx-1]
    ib, obb = imp["close"] > imp["open"], ob["close"] > ob["open"]
    hi, lo = float(max(ob["open"], ob["close"])), float(min(ob["open"], ob["close"]))
    cc = float(d["close"].iloc[-1])
    if ib and not obb and cc > hi: return {"detected": True, "type": "BULLISH_OB", "high": hi, "low": lo}
    if not ib and obb and cc < lo: return {"detected": True, "type": "BEARISH_OB", "high": hi, "low": lo}
    return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}

def confluence(df5, df15, dfh1, ob, adx_v, ha):
    c = df5["close"].astype(float)
    tp = sum(1 for x in [tf_trend(df5), tf_trend(df15), tf_trend(dfh1)] if x=="BULLISH") - \
         sum(1 for x in [tf_trend(df5), tf_trend(df15), tf_trend(dfh1)] if x=="BEARISH")
    e9, e21 = c.ewm(span=9, adjust=False).mean(), c.ewm(span=21, adjust=False).mean()
    m = 1 if e9.iloc[-1] > e21.iloc[-1] else -1
    s = (2 if ob["type"]=="BULLISH_OB" else -2) if ob["detected"] else 0
    ap = 1 if adx_v >= 25 else 0
    hp = 1 if ha=="BULLISH" else (-1 if ha=="BEARISH" else 0)
    return {"total": tp+m+s+ap+hp, "rsi": round(rsi(c),1)}

def generate(df5, df15, dfh1, cfg):
    adx_v = adx(df5)
    if adx_v < cfg.adx_min: return "NEUTRAL", {}, adx_v, "NEUTRAL", {}
    ob = order_block(df5); ha = heikin_trend(df5)
    conf = confluence(df5, df15, dfh1, ob, adx_v, ha)
    if conf["total"] >= cfg.score_threshold and ha=="BULLISH": return "BUY", conf, adx_v, ha, ob
    if conf["total"] <= -cfg.score_threshold and ha=="BEARISH": return "SELL", conf, adx_v, ha, ob
    return "NEUTRAL", conf, adx_v, ha, ob

def tpsl(df5, signal, cfg, balance):
    a = atr(df5); p = float(df5["close"].iloc[-1])
    if a <= 0 or signal=="NEUTRAL": return 0,0,0,0,0
    sl_d = a * cfg.atr_sl_mult
    if signal=="BUY": sl, tp1, tp2, tp3 = p-sl_d, p+a*cfg.atr_tp1_mult, p+a*cfg.atr_tp2_mult, p+a*cfg.atr_tp3_mult
    else: sl, tp1, tp2, tp3 = p+sl_d, p-a*cfg.atr_tp1_mult, p-a*cfg.atr_tp2_mult, p-a*cfg.atr_tp3_mult
    risk = balance * (cfg.risk_percentage/100)
    lot = round(max(cfg.min_lot, min(cfg.max_lot, risk/sl_d if sl_d>0 else 0.5)), 2)
    d = cfg.price_decimals
    return round(sl,d), round(tp1,d), round(tp2,d), round(tp3,d), lot

def send_telegram(msg, cfg):
    if cfg.dry_run: return True
    tok = os.getenv("TELEGRAM_TOKEN","").strip(); chat = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not tok or not chat: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": chat, "text": msg, "parse_mode": "HTML"}, timeout=10)
        return r.status_code == 200
    except Exception: return False

# ---------- Runner (menggunakan deriv-sdk resmi) ----------
async def run_once(client: DerivClient, cfg, db, balance):
    now = datetime.now(timezone.utc)
    if db.drawdown_paused(): logging.warning("Drawdown guard aktif."); return
    if db.daily_signal_count() >= cfg.max_daily_signals: logging.info("Batas harian."); return

    try:
        # Candle M5
        candles5 = await client.market.ticks_history(cfg.deriv_symbol, count=200, granularity=300, style="candles")
        df5 = pd.DataFrame(candles5)
        df5["datetime"] = pd.to_datetime(df5["epoch"], unit="s", utc=True)
        for c in ["open","high","low","close"]: df5[c] = pd.to_numeric(df5[c], errors="coerce")
        df5 = df5[["datetime","open","high","low","close"]].dropna().reset_index(drop=True)

        # Candle M15
        candles15 = await client.market.ticks_history(cfg.deriv_symbol, count=150, granularity=900, style="candles")
        df15 = pd.DataFrame(candles15)
        df15["datetime"] = pd.to_datetime(df15["epoch"], unit="s", utc=True)
        for c in ["open","high","low","close"]: df15[c] = pd.to_numeric(df15[c], errors="coerce")
        df15 = df15[["datetime","open","high","low","close"]].dropna().reset_index(drop=True)

        # Candle H1
        candlesh1 = await client.market.ticks_history(cfg.deriv_symbol, count=150, granularity=3600, style="candles")
        dfh1 = pd.DataFrame(candlesh1)
        dfh1["datetime"] = pd.to_datetime(dfh1["epoch"], unit="s", utc=True)
        for c in ["open","high","low","close"]: dfh1[c] = pd.to_numeric(dfh1[c], errors="coerce")
        dfh1 = dfh1[["datetime","open","high","low","close"]].dropna().reset_index(drop=True)
    except Exception as e:
        logging.error(f"Fetch gagal: {_redact(e)}"); return

    if min(len(df5), len(df15), len(dfh1)) < 60: logging.warning("Bar kurang."); return

    price = float(df5["close"].iloc[-1])
    signal, conf, adx_v, ha, ob = generate(df5, df15, dfh1, cfg)
    sl, tp1, tp2, tp3, lot = tpsl(df5, signal, cfg, balance)

    last_sig = db.get_state("last_signal", {"type": "NEUTRAL", "ts": ""})
    notify = False
    if signal != "NEUTRAL" and last_sig.get("type") != signal:
        last_sig = {"type": signal, "ts": now.isoformat()}
        db.set_state("last_signal", last_sig)
        notify = True

    db.insert_report({"timestamp": now.isoformat(), "symbol": cfg.deriv_symbol,
        "signal": signal, "notified": notify, "entry": price, "sl": sl,
        "tp1": tp1, "tp2": tp2, "tp3": tp3, "lot": lot, "confidence": conf,
        "adx": adx_v, "ha": ha, "ob": ob.get("type"), "path": conf})

    if notify:
        msg = (f"<b>APEX VOL80 v{VERSION}</b>\n\n<b>{signal}</b> @ <code>{price}</code>\n"
               f"SL: {sl} | TP1: {tp1} | TP2: {tp2} | TP3: {tp3}\nLot: {lot}\n"
               f"ADX: {adx_v:.1f} | HA: {ha} | OB: {ob.get('type')}")
        logging.info(f"SINYAL {signal} terkirim={send_telegram(msg, cfg)}")

async def main():
    setup_logging()
    cfg = Config.from_env()
    db = TursoDB(); db.init()

    # DerivClient publik (tanpa token) untuk market data
    client = DerivClient(app_id="1089")
    await client.start()
    try:
        # Validasi simbol
        symbols = await client.market.active_symbols()
        names = {s["symbol"]: s.get("display_name", "") for s in symbols}
        found = None
        for c in [cfg.deriv_symbol] + list(cfg.fallback_symbols):
            if c in names: found = c; break
        if not found:
            for s, name in names.items():
                if "80" in name and "Volatility" in name: found = s; break
        if not found: raise RuntimeError("Simbol Volatility 80 tidak ditemukan")
        cfg.deriv_symbol = found
        logging.info(f"Simbol aktif: {cfg.deriv_symbol}")

        balance = float(os.getenv("APEX_BALANCE_USD", "1000"))
        await run_once(client, cfg, db, balance)
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
