"""
APEX PREDATOR v6.1 — Engine (XAUUSD Signal Bot)
Sumber: Deriv (opsional) → Yahoo GC=F + offset → TwelveData
"""

import os, re, json, time, sys, logging
import requests, numpy as np, pandas as pd
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager

try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False

try:
    import websocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False


@dataclass
class Config:
    state_file: str = "signal_state.json"
    report_json: str = "xauusd_apex_report.json"
    journal_file: str = "trade_journal.json"
    symbol: str = "XAUUSD"
    deriv_symbol: str = "frxXAUUSD"
    contract_size: float = 100.0
    price_decimals: int = 3
    cooldown_minutes: int = 60
    min_bars_m5: int = 60
    min_bars_m15: int = 60
    min_bars_h1: int = 60
    max_stale_mult: float = 3.0
    max_gap_ratio: float = 0.10
    account_balance_usd: float = 10000.0
    risk_percentage: float = 1.0
    max_lot: float = 0.5
    min_lot: float = 0.01
    atr_sl_mult: float = 1.5
    atr_tp1_mult: float = 1.0
    atr_tp2_mult: float = 2.0
    atr_tp3_mult: float = 3.5
    spread_buffer_dollars: float = 0.30
    score_threshold: int = 8
    adx_min: float = 20.0
    adx_strong: float = 25.0
    max_daily_signals: int = 5
    news_blackout_minutes: int = 30
    news_cache_seconds: int = 1800
    news_fail_mode: str = "closed"
    report_max_entries: int = 500
    dry_run: bool = False

    @classmethod
    def from_env(cls) -> "Config":
        cfg = cls()
        for f in cls.__dataclass_fields__:
            env_val = os.getenv(f"APEX_{f.upper()}")
            if env_val is None:
                continue
            current = getattr(cfg, f)
            try:
                if isinstance(current, bool):
                    setattr(cfg, f, env_val.lower() in ("1", "true", "yes", "on"))
                elif isinstance(current, int):
                    setattr(cfg, f, int(env_val))
                elif isinstance(current, float):
                    setattr(cfg, f, float(env_val))
                else:
                    setattr(cfg, f, env_val)
            except ValueError:
                logging.warning(f"Env override invalid APEX_{f.upper()}={env_val}")
        return cfg


def _redact(text: str) -> str:
    text = re.sub(r"token=[^&\s'\"]+", "token=***", text)
    text = re.sub(r"/bot[^/\s'\"]+/", "/bot***/", text)
    text = re.sub(r"apikey=[^&\s'\"]+", "apikey=***", text)
    return text


class DataFetchError(Exception):
    pass


@contextmanager
def file_lock(path: str):
    if not HAS_FCNTL:
        yield
        return
    lock_path = path + ".lock"
    with open(lock_path, "w") as lf:
        try:
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass


def _atomic_write_json(data, path: str) -> None:
    temp = path + ".tmp"
    try:
        with open(temp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(temp, path)
    except Exception as e:
        logging.error(f"Atomic write gagal {path}: {_redact(str(e))}")


def _safe_read_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path) as f:
            content = f.read().strip()
            if not content:
                return default
            return json.loads(content)
    except Exception as e:
        logging.warning(f"Read {path} gagal: {_redact(str(e))}")
        return default


def _default_state() -> dict:
    return {
        "run_count": 0,
        "last_signal_type": "NEUTRAL",
        "last_signal_timestamp": "",
        "consecutive_errors": 0,
    }


def load_state(cfg: Config) -> dict:
    data = _safe_read_json(cfg.state_file, _default_state())
    if not isinstance(data, dict):
        return _default_state()
    return {**_default_state(), **data}


def save_state(state: dict, cfg: Config) -> None:
    with file_lock(cfg.state_file):
        _atomic_write_json(state, cfg.state_file)


def append_report(report: dict, cfg: Config) -> None:
    with file_lock(cfg.report_json):
        history = _safe_read_json(cfg.report_json, [])
        if isinstance(history, dict):
            history = [history]
        if not isinstance(history, list):
            history = []
        history.append(report)
        history = history[-cfg.report_max_entries:]
        _atomic_write_json(history, cfg.report_json)


def append_journal(entry: dict, cfg: Config) -> None:
    with file_lock(cfg.journal_file):
        journal = _safe_read_json(cfg.journal_file, [])
        if not isinstance(journal, list):
            journal = []
        journal.append(entry)
        _atomic_write_json(journal, cfg.journal_file)


def count_daily_signals(cfg: Config) -> int:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    reports = _safe_read_json(cfg.report_json, [])
    if not isinstance(reports, list):
        return 0
    return sum(
        1 for r in reports
        if isinstance(r, dict)
        and r.get("timestamp", "").startswith(today)
        and r.get("final_signal") in ("BUY", "SELL")
    )


_CIRCUIT = {"failures": 0, "opened_until": None}


def circuit_allow() -> bool:
    if _CIRCUIT["opened_until"] and datetime.now(timezone.utc) < _CIRCUIT["opened_until"]:
        return False
    return True


def circuit_record_failure(threshold: int = 3, cooldown_min: int = 15) -> None:
    _CIRCUIT["failures"] += 1
    if _CIRCUIT["failures"] >= threshold:
        _CIRCUIT["opened_until"] = datetime.now(timezone.utc) + timedelta(minutes=cooldown_min)
        logging.warning(f"⚡ Circuit breaker OPEN — istirahat {cooldown_min} menit")
        _CIRCUIT["failures"] = 0


def circuit_record_success() -> None:
    _CIRCUIT["failures"] = 0
    _CIRCUIT["opened_until"] = None


def _candles_to_df(candles: list) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df["datetime"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = 100.0
    return df[["datetime", "open", "high", "low", "close", "volume"]].reset_index(drop=True)


def fetch_deriv_ohlcv(cfg: Config, interval_str: str, count: int = 200, retries: int = 2) -> pd.DataFrame:
    if not HAS_WEBSOCKET:
        raise DataFetchError("websocket-client belum terpasang")
    gran_map = {"5min": 300, "15min": 900, "1h": 3600}
    granularity = gran_map.get(interval_str)
    if granularity is None:
        raise DataFetchError(f"Interval tidak didukung: {interval_str}")
    payload = {
        "ticks_history": cfg.deriv_symbol,
        "count": count,
        "end": "latest",
        "granularity": granularity,
        "style": "candles",
    }
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    last_err = None
    for attempt in range(retries + 1):
        ws = None
        try:
            ws = websocket.create_connection(url, timeout=10, enable_multithread=True)
            ws.send(json.dumps(payload))
            deadline = time.time() + 15
            candles = None
            while time.time() < deadline:
                raw = ws.recv()
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if "error" in data:
                    raise DataFetchError(f"Deriv API: {data['error'].get('message', 'unknown')}")
                if data.get("msg_type") == "candles" or "candles" in data:
                    candles = data.get("candles", [])
                    break
            if not candles:
                raise DataFetchError("Deriv: candles timeout")
            return _candles_to_df(candles)
        except Exception as e:
            last_err = e
            logging.warning(f"Deriv attempt {attempt + 1}: {_redact(str(e))}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
    raise DataFetchError(f"Deriv gagal {retries + 1}×: {_redact(str(last_err))}")


def fetch_yahoo_ohlcv(cfg: Config, interval_str: str, count: int = 200) -> pd.DataFrame:
    interval_map = {"5min": "5m", "15min": "15m", "1h": "1h"}
    interval = interval_map.get(interval_str)
    if interval is None:
        raise DataFetchError(f"Interval tidak didukung Yahoo: {interval_str}")

    headers = {"User-Agent": "Mozilla/5.0 (compatible; ApexBot/6.1)"}

    def _fetch_symbol(symbol: str) -> pd.DataFrame:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": interval, "range": "5d"},
            headers=headers,
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        result = data.get("chart", {}).get("result")
        if not result:
            raise DataFetchError(f"Yahoo {symbol}: no result")
        res = result[0]
        timestamps = res.get("timestamp", [])
        quote = res.get("indicators", {}).get("quote", [{}])[0]
        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])
        rows = []
        for i, ts in enumerate(timestamps):
            if i >= len(opens) or opens[i] is None or closes[i] is None:
                continue
            rows.append({
                "epoch": int(ts),
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i],
                "volume": volumes[i] if i < len(volumes) and volumes[i] else 100.0,
            })
        if not rows:
            raise DataFetchError(f"Yahoo {symbol}: no valid rows")
        return _candles_to_df(rows)

    # Lapis 1: spot XAUUSD=X
    try:
        df = _fetch_symbol("XAUUSD=X")
        logging.info(f"Yahoo XAUUSD=X (spot): {len(df)} bar OK")
        return df.tail(count).reset_index(drop=True)
    except Exception as e:
        logging.warning(f"Yahoo spot XAUUSD=X gagal: {_redact(str(e))}")

    # Lapis 2: futures GC=F + offset dinamis
    try:
        df_fut = _fetch_symbol("GC=F")
        spot_price = None
        try:
            spot_df = _fetch_symbol("XAUUSD=X")
            spot_price = float(spot_df["close"].iloc[-1])
        except Exception:
            try:
                spot_r = requests.get("https://xaus.com/api/v1/spot", timeout=5)
                spot_r.raise_for_status()
                spot_price = float(spot_r.json().get("spot_usd_oz", 0))
            except Exception:
                env_off = float(os.getenv("APEX_YAHOO_OFFSET", "0"))
                spot_price = float(df_fut["close"].iloc[-1]) + env_off

        if spot_price and spot_price > 0:
            offset = spot_price - float(df_fut["close"].iloc[-1])
            logging.info(f"Yahoo GC=F + offset dinamis {offset:+.3f} (spot={spot_price:.3f})")
            for col in ["open", "high", "low", "close"]:
                df_fut[col] = df_fut[col] + offset

        logging.info(f"Yahoo GC=F (futures+offset): {len(df_fut)} bar OK")
        return df_fut.tail(count).reset_index(drop=True)
    except Exception as e:
        raise DataFetchError(f"Yahoo semua symbol gagal: {_redact(str(e))}")


def fetch_twelvedata_ohlcv(cfg: Config, interval_str: str, count: int = 200) -> pd.DataFrame:
    api_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    if not api_key:
        raise DataFetchError("TWELVEDATA_API_KEY tidak diset")
    interval_map = {"5min": "5min", "15min": "15min", "1h": "1h"}
    interval = interval_map.get(interval_str)
    if interval is None:
        raise DataFetchError(f"Interval tidak didukung: {interval_str}")
    r = requests.get(
        "https://api.twelvedata.com/time_series",
        params={"symbol": "XAU/USD", "interval": interval, "outputsize": count, "apikey": api_key},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    if "values" not in data:
        raise DataFetchError(f"TwelveData: {data.get('message', 'unknown')}")
    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = 100.0
    df = df.sort_values("datetime").reset_index(drop=True)
    return df[["datetime", "open", "high", "low", "close", "volume"]]


def fetch_ohlcv_cascade(cfg: Config, interval_str: str, count: int = 200) -> tuple:
    if not circuit_allow():
        raise DataFetchError("Circuit breaker aktif")
    errors = []
    sources = []
    if os.getenv("APEX_ENABLE_DERIV", "1") == "1":
        sources.append(("Deriv", fetch_deriv_ohlcv))
    sources.append(("Yahoo", fetch_yahoo_ohlcv))
    sources.append(("TwelveData", fetch_twelvedata_ohlcv))
    for name, fn in sources:
        try:
            df = fn(cfg, interval_str, count)
            circuit_record_success()
            logging.info(f"✅ Data OK dari {name} ({len(df)} bar)")
            return df, name
        except Exception as e:
            errors.append(f"{name}: {_redact(str(e))}")
            logging.warning(f"❌ Feed '{name}' gagal: {_redact(str(e))}")
    circuit_record_failure()
    raise DataFetchError("SEMUA FEED GAGAL | " + " || ".join(errors))


_INTERVAL_SECONDS = {"5min": 300, "15min": 900, "1h": 3600}


def validate_ohlcv(df: pd.DataFrame, interval_str: str, min_bars: int, cfg: Config) -> pd.DataFrame:
    if df is None or len(df) == 0:
        raise ValueError("no data returned")
    df = df.dropna(subset=["open", "high", "low", "close"]).copy()
    if len(df) < min_bars:
        raise ValueError(f"insufficient bars: {len(df)} < {min_bars}")
    interval_sec = _INTERVAL_SECONDS.get(interval_str, 300)
    last_time = df["datetime"].iloc[-1]
    if last_time.tzinfo is None:
        last_time = last_time.replace(tzinfo=timezone.utc)
    age_sec = (datetime.now(timezone.utc) - last_time).total_seconds()
    max_age = interval_sec * cfg.max_stale_mult
    if age_sec > max_age:
        raise ValueError(f"stale data: {age_sec:.0f}s (max {max_age:.0f}s)")
    recent = df.tail(50)
    if len(recent) >= 10:
        diffs = recent["datetime"].diff().dt.total_seconds().dropna()
        gaps = (diffs > interval_sec * 1.5).sum()
        if len(diffs) > 0 and (gaps / len(diffs)) > cfg.max_gap_ratio:
            raise ValueError(f"too many gaps: {gaps}/{len(diffs)}")
    return df.reset_index(drop=True)


def is_market_open(now_utc: datetime) -> bool:
    wd, h = now_utc.weekday(), now_utc.hour
    if wd == 5:
        return False
    if wd == 6:
        return h >= 23
    if wd == 4:
        return h < 21
    return True


def is_institutional_killzone(now_utc: datetime) -> bool:
    h = now_utc.hour
    return (7 <= h <= 10) or (12 <= h <= 16)


_NEWS_CACHE = {"ok": False, "ts": None, "blocked": True, "reason": ""}


def _is_nfp_window(now_utc: datetime) -> bool:
    if now_utc.weekday() != 4:
        return False
    if now_utc.day > 7:
        return False
    return 12 <= now_utc.hour <= 13


def check_news_shield(cfg: Config) -> tuple:
    now = datetime.now(timezone.utc)
    if (_NEWS_CACHE["ok"] and _NEWS_CACHE["ts"]
            and (now - _NEWS_CACHE["ts"]).total_seconds() < cfg.news_cache_seconds):
        return _NEWS_CACHE["blocked"], _NEWS_CACHE["reason"]
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=5)
        r.raise_for_status()
        events = r.json()
        for e in events:
            if e.get("country") != "USD" or e.get("impact") != "High":
                continue
            try:
                ev_dt = datetime.strptime(e["date"], "%Y-%m-%dT%H:%M:%S%z")
            except Exception:
                continue
            diff_min = abs((ev_dt - now).total_seconds() / 60)
            if diff_min <= cfg.news_blackout_minutes:
                reason = f"news '{e.get('title', '?')}' dalam {diff_min:.0f}m"
                _NEWS_CACHE.update(ok=True, ts=now, blocked=True, reason=reason)
                return True, reason
        _NEWS_CACHE.update(ok=True, ts=now, blocked=False, reason="")
        return False, ""
    except Exception as ex:
        logging.error(f"News shield gagal: {_redact(str(ex))}")
        if _is_nfp_window(now):
            reason = "NFP window"
            _NEWS_CACHE.update(ok=True, ts=now, blocked=True, reason=reason)
            return True, reason
        if cfg.news_fail_mode == "closed":
            reason = "news API down — fail-closed"
            _NEWS_CACHE.update(ok=True, ts=now, blocked=True, reason=reason)
            return True, reason
        _NEWS_CACHE.update(ok=True, ts=now, blocked=False, reason="fail-open")
        return False, ""


def compute_true_range(df: pd.DataFrame) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    return pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)


def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    tr = compute_true_range(df)
    atr = tr.rolling(period).mean()
    val = atr.iloc[-1]
    return 0.0 if pd.isna(val) else float(val)


def calculate_adx(df: pd.DataFrame, period: int = 14) -> float:
    try:
        h, l = df["high"], df["low"]
        up, down = h.diff(), -l.diff()
        plus_dm = up.where((up > down) & (up > 0), 0.0)
        minus_dm = down.where((down > up) & (down > 0), 0.0)
        atr = compute_true_range(df).ewm(alpha=1 / period, adjust=False).mean().replace(0, np.nan)
        plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr
        minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr
        denom = (plus_di + minus_di).replace(0, np.nan)
        dx = (plus_di - minus_di).abs() / denom * 100
        adx = dx.ewm(alpha=1 / period, adjust=False).mean()
        val = adx.iloc[-1]
        return 0.0 if pd.isna(val) else float(val)
    except Exception:
        return 0.0


def get_heikin_ashi_trend(df: pd.DataFrame, lookback: int = 200) -> str:
    try:
        data = df.tail(lookback).reset_index(drop=True)
        if len(data) < 3:
            return "NEUTRAL"
        ha_close = (data["open"] + data["high"] + data["low"] + data["close"]) / 4.0
        ha_open = pd.Series(index=data.index, dtype=float)
        ha_open.iloc[0] = (data["open"].iloc[0] + data["close"].iloc[0]) / 2.0
        for i in range(1, len(data)):
            ha_open.iloc[i] = (ha_open.iloc[i - 1] + ha_close.iloc[i - 1]) / 2.0
        diff = float(ha_close.iloc[-1] - ha_open.iloc[-1])
        if abs(diff) < 1e-9:
            return "NEUTRAL"
        return "BULLISH" if diff > 0 else "BEARISH"
    except Exception:
        return "NEUTRAL"


def get_timeframe_trend(df: pd.DataFrame) -> str:
    if df is None or len(df) < 50:
        return "NEUTRAL"
    close = df["close"].astype(float)
    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    if close.iloc[-1] > ema50.iloc[-1] and ema20.iloc[-1] > ema50.iloc[-1]:
        return "BULLISH"
    if close.iloc[-1] < ema50.iloc[-1] and ema20.iloc[-1] < ema50.iloc[-1]:
        return "BEARISH"
    return "NEUTRAL"


def calculate_rsi(close: pd.Series, period: int = 14) -> float:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rsi = 100 - (100 / (1 + gain / (loss + 1e-9)))
    val = rsi.iloc[-1]
    return 50.0 if pd.isna(val) else float(val)


def detect_fair_value_gap(df: pd.DataFrame) -> dict:
    try:
        if len(df) < 3:
            return {"detected": False, "type": "NONE", "level": 0.0}
        c1, c3 = df.iloc[-3], df.iloc[-1]
        if c3["low"] > c1["high"]:
            return {"detected": True, "type": "BULLISH_FVG", "level": float(c1["high"])}
        if c3["high"] < c1["low"]:
            return {"detected": True, "type": "BEARISH_FVG", "level": float(c1["low"])}
        return {"detected": False, "type": "NONE", "level": 0.0}
    except Exception:
        return {"detected": False, "type": "NONE", "level": 0.0}


def detect_institutional_order_blocks(df: pd.DataFrame) -> dict:
    try:
        d = df.tail(30).reset_index(drop=True)
        if len(d) < 3:
            return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
        atr_val = compute_atr(df, 14)
        if atr_val <= 0:
            return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
        bodies = (d["close"] - d["open"]).abs().iloc[:-1]
        if bodies.empty:
            return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
        impulse_idx = int(bodies.idxmax())
        if impulse_idx == 0 or impulse_idx < len(d) - 15:
            return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
        impulse = d.iloc[impulse_idx]
        ob = d.iloc[impulse_idx - 1]
        if abs(float(impulse["close"] - impulse["open"])) < atr_val:
            return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
        impulse_bull = impulse["close"] > impulse["open"]
        ob_bull = ob["close"] > ob["open"]
        hi = float(max(ob["open"], ob["close"]))
        lo = float(min(ob["open"], ob["close"]))
        if impulse_bull and not ob_bull:
            return {"detected": True, "type": "BULLISH_OB", "high": hi, "low": lo}
        if (not impulse_bull) and ob_bull:
            return {"detected": True, "type": "BEARISH_OB", "high": hi, "low": lo}
        return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
    except Exception:
        return {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}


def compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx_val, ha_trend) -> dict:
    close = df_m5["close"].astype(float)
    t = [get_timeframe_trend(df_m5), get_timeframe_trend(df_m15), get_timeframe_trend(df_h1)]
    trend_pts = sum(1 for x in t if x == "BULLISH") - sum(1 for x in t if x == "BEARISH")
    ema9 = close.ewm(span=9, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()
    rsi = calculate_rsi(close)
    momo = 0
    momo += 1 if ema9.iloc[-1] > ema21.iloc[-1] else -1
    if rsi < 40:
        momo += 1
    elif rsi > 60:
        momo -= 1
    smc = 0
    if ob["detected"]:
        smc += 2 if ob["type"] == "BULLISH_OB" else -2
    elif fvg["detected"]:
        smc += 1 if fvg["type"] == "BULLISH_FVG" else -1
    if adx_val >= 25:
        adx_pts = 1
    elif adx_val >= 20:
        adx_pts = 0
    else:
        adx_pts = -1
    ha_pts = 1 if ha_trend == "BULLISH" else (-1 if ha_trend == "BEARISH" else 0)
    total = trend_pts + momo + smc + adx_pts + ha_pts
    return {
        "total": total, "trend": trend_pts, "momentum": momo,
        "smc": smc, "adx": adx_pts, "ha": ha_pts,
        "rsi": round(rsi, 1), "adx_value": round(adx_val, 1),
    }


@dataclass
class DecisionLog:
    passed: list = field(default_factory=list)
    failed: list = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        label = f"{name}" + (f" ({detail})" if detail else "")
        (self.passed if ok else self.failed).append(("✓ " if ok else "✗ ") + label)
        return ok

    def to_dict(self) -> dict:
        return {"passed": self.passed, "failed": self.failed}


def generate_signal(df_m5, df_m15, df_h1, cfg: Config) -> tuple:
    log = DecisionLog()
    adx_val = calculate_adx(df_m5)
    if not log.check("ADX", adx_val >= cfg.adx_min, f"{adx_val:.1f} ≥ {cfg.adx_min}"):
        return "NEUTRAL", {"reason": f"ADX {adx_val:.1f} < {cfg.adx_min}"}, log, None
    ob = detect_institutional_order_blocks(df_m5)
    fvg = detect_fair_value_gap(df_m5)
    if not log.check("SMC", ob["detected"] or fvg["detected"],
                     ob["type"] if ob["detected"] else fvg["type"]):
        return "NEUTRAL", {"reason": "no OB/FVG"}, log, None
    ha = get_heikin_ashi_trend(df_m5)
    conf = compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx_val, ha)
    if (conf["total"] >= cfg.score_threshold and ha == "BULLISH"
            and ob["type"] in ("BULLISH_OB", "NONE")):
        log.check("Confluence", True, f"+{conf['total']} ≥ {cfg.score_threshold}")
        log.check("HA", True, ha)
        return "BUY", {}, log, conf
    if (conf["total"] <= -cfg.score_threshold and ha == "BEARISH"
            and ob["type"] in ("BEARISH_OB", "NONE")):
        log.check("Confluence", True, f"{conf['total']} ≤ -{cfg.score_threshold}")
        log.check("HA", True, ha)
        return "SELL", {}, log, conf
    log.check("Confluence", False,
              f"{conf['total']:+d} vs ±{cfg.score_threshold}; HA={ha}")
    return "NEUTRAL", {"reason": "confluence low"}, log, conf


def calculate_multi_target_tpsl(df_m5, signal_type: str, cfg: Config):
    atr_val = compute_atr(df_m5, 14)
    price = float(df_m5["close"].iloc[-1])
    if atr_val <= 0 or signal_type == "NEUTRAL":
        return 0.0, 0.0, 0.0, 0.0, 0.0
    sl_dist = atr_val * cfg.atr_sl_mult
    tp1_dist = max(atr_val * cfg.atr_tp1_mult, cfg.spread_buffer_dollars * 2)
    tp2_dist = atr_val * cfg.atr_tp2_mult
    tp3_dist = atr_val * cfg.atr_tp3_mult
    if signal_type == "BUY":
        sl = price - sl_dist
        tp1, tp2, tp3 = price + tp1_dist, price + tp2_dist, price + tp3_dist
    else:
        sl = price + sl_dist
        tp1, tp2, tp3 = price - tp1_dist, price - tp2_dist, price - tp3_dist
    risk_usd = cfg.account_balance_usd * (cfg.risk_percentage / 100.0)
    raw_lot = risk_usd / (sl_dist * cfg.contract_size) if sl_dist > 0 else 0.0
    lot = round(max(cfg.min_lot, min(cfg.max_lot, raw_lot)), 2)
    p = cfg.price_decimals
    return round(sl, p), round(tp1, p), round(tp2, p), round(tp3, p), lot


_ERROR_ALERT = {"last": None}


def send_telegram(message: str, cfg: Config) -> bool:
    if cfg.dry_run:
        logging.info(f"[DRY-RUN] Telegram: {message[:120]}...")
        return True
    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        logging.debug("Telegram tidak dikonfigurasi")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        logging.error(f"Telegram gagal: {_redact(str(e))}")
        return False


def alert_error(msg: str, cfg: Config) -> None:
    now = datetime.now(timezone.utc)
    if _ERROR_ALERT["last"] and (now - _ERROR_ALERT["last"]).total_seconds() < 300:
        return
    _ERROR_ALERT["last"] = now
    send_telegram(f"⚠️ *APEX ERROR*\n\n`{_redact(msg)}`", cfg)


def should_notify(state: dict, signal_type: str, now_utc: datetime, cfg: Config) -> bool:
    if signal_type == "NEUTRAL":
        return False
    if state.get("last_signal_type") != signal_type:
        return True
    last_ts = state.get("last_signal_timestamp", "")
    if not last_ts:
        return True
    try:
        last_dt = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return (now_utc - last_dt) >= timedelta(minutes=cfg.cooldown_minutes)
    except Exception:
        return True


def build_message(cfg, run_num, now_str, source, signal_type, price,
                  sl, tp1, tp2, tp3, lot, conf, ob, fvg, ha, adx_val) -> str:
    smc = ob["type"] if ob["detected"] else fvg["type"]
    p = cfg.price_decimals
    breakdown = (f"`T{conf['trend']:+d} M{conf['momentum']:+d} "
                 f"S{conf['smc']:+d} A{conf['adx']:+d} H{conf['ha']:+d}`")
    return (
        f"⚛️ *APEX PREDATOR v6.1* ⚛️\n\n"
        f"📊 *{cfg.symbol}* | 🔌 `{source}`\n"
        f"🎯 *{signal_type}* @ `{price:.{p}f}`\n\n"
        f"🛑 SL: `{sl}`\n✅ TP1: `{tp1}` (spread-aware)\n"
        f"✅ TP2: `{tp2}`\n✅ TP3: `{tp3}`\n"
        f"📐 Lot: `{lot}` (risk {cfg.risk_percentage}%)\n\n"
        f"🏛️ SMC: `{smc}`\n"
        f"📈 H1/HA: `{ha}` | ADX: `{adx_val:.1f}` | RSI: `{conf['rsi']}`\n"
        f"🧮 Confluence: `{conf['total']:+d}/±9` {breakdown}\n"
        f"🔄 Run `#{run_num}` | ⏰ `{now_str} UTC`\n\n"
        f"_Manual execution & scaling out via MT5._"
    )


def run_once(cfg: Config) -> None:
    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")

    with file_lock(cfg.state_file):
        state = load_state(cfg)
        state["run_count"] = int(state.get("run_count", 0)) + 1
        run_num = state["run_count"]
        _atomic_write_json(state, cfg.state_file)

    logging.info(f"═══ APEX v6.1 Run #{run_num} @ {now_str} UTC ═══")

    if not is_market_open(now_utc):
        logging.info("Market closed — skip."); return
    if not is_institutional_killzone(now_utc):
        logging.info("Di luar killzone — siaga."); return

    blocked, reason = check_news_shield(cfg)
    if blocked:
        logging.info(f"🚨 News block: {reason}"); return

    if count_daily_signals(cfg) >= cfg.max_daily_signals:
        logging.info(f"Daily limit {cfg.max_daily_signals} tercapai.")
        return

    try:
        raw_m5, source = fetch_ohlcv_cascade(cfg, "5min", 200)
        df_m5 = validate_ohlcv(raw_m5, "5min", cfg.min_bars_m5, cfg).iloc[:-1].reset_index(drop=True)
        raw_m15, _ = fetch_ohlcv_cascade(cfg, "15min", 150)
        df_m15 = validate_ohlcv(raw_m15, "15min", cfg.min_bars_m15, cfg).iloc[:-1].reset_index(drop=True)
        raw_h1, _ = fetch_ohlcv_cascade(cfg, "1h", 100)
        df_h1 = validate_ohlcv(raw_h1, "1h", cfg.min_bars_h1, cfg).iloc[:-1].reset_index(drop=True)
    except Exception as e:
        logging.error(f"Data pipeline gagal: {_redact(str(e))}")
        alert_error(f"Data pipeline: {e}", cfg)
        return

    current_price = float(df_m5["close"].iloc[-1])
    signal_type, meta, log, conf = generate_signal(df_m5, df_m15, df_h1, cfg)
    adx_val = calculate_adx(df_m5)
    ha = get_heikin_ashi_trend(df_m5)
    ob = detect_institutional_order_blocks(df_m5)
    fvg = detect_fair_value_gap(df_m5)

    with file_lock(cfg.state_file):
        state = load_state(cfg)
        notify = should_notify(state, signal_type, now_utc, cfg)
        if signal_type != "NEUTRAL":
            state["last_signal_type"] = signal_type
            state["last_signal_timestamp"] = now_str
        _atomic_write_json(state, cfg.state_file)

    sl, tp1, tp2, tp3, lot = calculate_multi_target_tpsl(df_m5, signal_type, cfg)

    report = {
        "timestamp": now_str, "run_count": run_num, "active_source": source,
        "final_signal": signal_type,
        "entry_price": round(current_price, cfg.price_decimals),
        "confidence": conf, "adx": round(adx_val, 2), "heikin_ashi": ha,
        "ob_type": ob["type"] if ob["detected"] else "NONE",
        "fvg_type": fvg["type"] if fvg["detected"] else "NONE",
        "recommended_sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "suggested_lot": lot, "decision_path": log.to_dict(),
    }
    append_report(report, cfg)

    if signal_type != "NEUTRAL" and notify:
        msg = build_message(cfg, run_num, now_str, source, signal_type,
                            current_price, sl, tp1, tp2, tp3, lot,
                            conf, ob, fvg, ha, adx_val)
        if send_telegram(msg, cfg):
            append_journal({
                "id": f"{now_str}_{signal_type}",
                "timestamp": now_str, "symbol": cfg.symbol,
                "signal": signal_type,
                "entry": round(current_price, cfg.price_decimals),
                "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "lot": lot,
                "confluence": conf, "outcome": "PENDING", "pnl_usd": None, "notes": "",
            }, cfg)

    logging.info("═══ Cycle Complete ═══")


def cmd_status(cfg: Config) -> None:
    state = load_state(cfg)
    reports = _safe_read_json(cfg.report_json, [])
    journal = _safe_read_json(cfg.journal_file, [])
    signals_today = count_daily_signals(cfg)
    recent = reports[-5:] if isinstance(reports, list) else []
    pending = [j for j in journal if isinstance(j, dict) and j.get("outcome") == "PENDING"] if isinstance(journal, list) else []
    print("═══ STATUS APEX v6.1 ═══")
    print(f"Symbol         : {cfg.symbol}")
    print(f"Run count      : {state.get('run_count', 0)}")
    print(f"Last signal    : {state.get('last_signal_type')} @ {state.get('last_signal_timestamp') or '-'}")
    print(f"Signals today  : {signals_today}/{cfg.max_daily_signals}")
    print(f"Journal pending: {len(pending)}")
    print(f"Circuit breaker: {'OPEN' if not circuit_allow() else 'CLOSED'}")
    print("\n--- 5 report terakhir ---")
    for r in recent:
        if isinstance(r, dict):
            conf = r.get("confidence") or {}
            total = conf.get("total", 0) if isinstance(conf, dict) else 0
            print(f"  {r.get('timestamp')} | {r.get('final_signal', '?'):7s} | "
                  f"conf={total:+d} | src={r.get('active_source')}")


def cmd_reset(cfg: Config) -> None:
    if os.path.exists(cfg.state_file):
        os.remove(cfg.state_file)
        print(f"State di-reset: {cfg.state_file}")
    else:
        print("State file tidak ada.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [Apex-v6.1] - %(message)s",
    )
    run_once(Config.from_env())
