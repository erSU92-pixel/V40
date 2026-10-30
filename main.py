"""
APEX PREDATOR v7.0 — Battle-Ready XAUUSD Signal Bot
═══════════════════════════════════════════════════

COMMANDS:
    python apex.py selftest       ←  WAJIB dijalankan pertama kali
    python apex.py                ←  normal (untuk cron tiap 5 menit)
    python apex.py status         ←  aktivitas singkat
    python apex.py health         ←  diagnosa lengkap
    python apex.py performance    ←  win rate, R, drawdown
    python apex.py export         ←  ekspor journal ke CSV
    python apex.py reset-drawdown ←  resume setelah kena drawdown pause
    python apex.py reset          ←  reset state (journal tetap)
    python apex.py help           ←  bantuan

FITUR BARU v7.0:
    [+]  Outcome tracking otomatis (TP/SL hit detection)
    [+]  Notifikasi follow-up saat TP/SL kena
    [+]  Performance dashboard (win rate, R, drawdown, streak)
    [+]  Drawdown guard — pause 24h setelah 3 loss berturut
    [+]  Daily heartbeat — bot lapor hidup sekali sehari
    [+]  Health command — diagnosa komprehensif
    [+]  CSV export untuk analisis Excel
"""

import os, re, json, time, sys, csv, logging, html
import requests, numpy as np, pandas as pd
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

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

VERSION = "7.0"
MAX_CONFLUENCE = 9


# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

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
    # v7.0 new:
    track_outcomes: bool = True
    notify_on_tp_hit: bool = True
    drawdown_guard_enabled: bool = True
    drawdown_max_consecutive_losses: int = 3
    drawdown_pause_hours: int = 24
    heartbeat_enabled: bool = True
    heartbeat_hour_utc: int = 8

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
                    setattr(cfg, f, env_val.strip().lower() in ("1", "true", "yes", "on"))
                elif isinstance(current, int):
                    setattr(cfg, f, int(env_val))
                elif isinstance(current, float):
                    v = float(env_val)
                    if not np.isfinite(v):
                        logging.warning(f"APEX_{f.upper()} tidak valid — pakai default")
                        continue
                    setattr(cfg, f, v)
                else:
                    setattr(cfg, f, env_val)
            except ValueError:
                logging.warning(f"APEX_{f.upper()}={env_val} tidak valid — pakai default")
        cfg._validate()
        return cfg

    def _validate(self) -> None:
        errors = []
        if self.news_fail_mode not in ("closed", "open"):
            errors.append(f"news_fail_mode harus 'closed' atau 'open'")
        if self.account_balance_usd <= 0:
            errors.append(f"account_balance_usd harus > 0")
        if not (0 < self.risk_percentage <= 10):
            errors.append(f"risk_percentage harus antara 0-10")
        if self.min_lot <= 0 or self.max_lot < self.min_lot:
            errors.append(f"lot config tidak valid")
        if not (0 < self.score_threshold <= MAX_CONFLUENCE):
            errors.append(f"score_threshold harus antara 1-{MAX_CONFLUENCE}")
        if self.max_daily_signals < 1:
            errors.append(f"max_daily_signals harus >= 1")
        if self.drawdown_max_consecutive_losses < 1:
            errors.append(f"drawdown_max_consecutive_losses harus >= 1")
        if errors:
            raise ValueError("KONFIGURASI TIDAK VALID:\n  • " + "\n  • ".join(errors))


# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

def setup_logging(cfg: Config, verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    fmt = logging.Formatter(f"%(asctime)s [%(levelname)s] [Apex-v{VERSION}] - %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)
    try:
        fh = RotatingFileHandler(cfg.log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _redact(text: str) -> str:
    text = str(text).replace("\n", " ").replace("\r", " ")
    text = re.sub(r"token=[^&\s'\"]+", "token=***", text)
    text = re.sub(r"/bot[^/\s'\"]+/", "/bot***/", text)
    text = re.sub(r"apikey=[^&\s'\"]+", "apikey=***", text)
    return text


def _h(s) -> str:
    return html.escape(str(s), quote=False)


class DataFetchError(Exception):
    pass


@contextmanager
def file_lock(path: str):
    if not HAS_FCNTL:
        yield
        return
    lock_path = path + ".lock"
    try:
        with open(lock_path, "w") as lf:
            try:
                os.chmod(lock_path, 0o600)
            except Exception:
                pass
            try:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                try:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass
    except Exception:
        yield


def _atomic_write_json(data, path: str) -> None:
    temp = path + ".tmp"
    try:
        with open(temp, "w") as f:
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(temp, 0o600)
        except Exception:
            pass
        os.replace(temp, path)
    except Exception as e:
        logging.error(f"Gagal menulis {path}: {_redact(str(e))}")


def _safe_read_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path) as f:
            content = f.read().strip()
            if not content:
                return default
            return json.loads(content)
    except json.JSONDecodeError:
        backup = f"{path}.corrupt.{int(time.time())}"
        try:
            os.rename(path, backup)
            logging.warning(f"File {path} rusak — dibackup ke {backup}")
        except Exception:
            pass
        return default
    except Exception as e:
        logging.warning(f"Baca {path} gagal: {_redact(str(e))}")
        return default


# ═══════════════════════════════════════════════════════════════
# STATE / REPORT / JOURNAL
# ═══════════════════════════════════════════════════════════════

def _default_state() -> dict:
    return {"run_count": 0, "last_signal_type": "NEUTRAL", "last_signal_timestamp": "", "consecutive_errors": 0}


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


def update_journal_status(journal_id: str, updates: dict, cfg: Config) -> None:
    with file_lock(cfg.journal_file):
        journal = _safe_read_json(cfg.journal_file, [])
        if not isinstance(journal, list):
            return
        for entry in journal:
            if isinstance(entry, dict) and entry.get("id") == journal_id:
                entry.update(updates)
                break
        _atomic_write_json(journal, cfg.journal_file)


def count_daily_signals(cfg: Config) -> int:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    reports = _safe_read_json(cfg.report_json, [])
    if not isinstance(reports, list):
        return 0
    return sum(1 for r in reports
               if isinstance(r, dict)
               and r.get("timestamp", "").startswith(today)
               and r.get("final_signal") in ("BUY", "SELL")
               and r.get("notified", False))


# ═══════════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════════

def _circuit_path(cfg: Config) -> str:
    return cfg.state_file + ".breaker"


def _load_circuit(cfg: Config) -> dict:
    data = _safe_read_json(_circuit_path(cfg), {"failures": 0, "opened_until": None})
    if not isinstance(data, dict):
        return {"failures": 0, "opened_until": None}
    return data


def circuit_allow(cfg: Config) -> bool:
    c = _load_circuit(cfg)
    if c.get("opened_until"):
        try:
            until = datetime.fromisoformat(c["opened_until"])
            if datetime.now(timezone.utc) < until:
                return False
        except Exception:
            pass
    return True


def circuit_record_failure(cfg: Config, threshold: int = 3, cooldown_min: int = 15) -> None:
    c = _load_circuit(cfg)
    failures = int(c.get("failures", 0)) + 1
    opened_until = c.get("opened_until")
    if failures >= threshold:
        until = datetime.now(timezone.utc) + timedelta(minutes=cooldown_min)
        opened_until = until.isoformat()
        failures = 0
        logging.warning(f"Circuit breaker OPEN — istirahat {cooldown_min} menit")
    with file_lock(_circuit_path(cfg)):
        _atomic_write_json({"failures": failures, "opened_until": opened_until}, _circuit_path(cfg))


def circuit_record_success(cfg: Config) -> None:
    with file_lock(_circuit_path(cfg)):
        _atomic_write_json({"failures": 0, "opened_until": None}, _circuit_path(cfg))


# ═══════════════════════════════════════════════════════════════
# DRAWDOWN GUARD
# ═══════════════════════════════════════════════════════════════

def _pause_path(cfg: Config) -> str:
    return cfg.state_file + ".pause"


def should_block_due_to_drawdown(cfg: Config) -> tuple:
    if not cfg.drawdown_guard_enabled:
        return False, ""
    pause_data = _safe_read_json(_pause_path(cfg), {})
    if pause_data.get("until"):
        try:
            until = datetime.fromisoformat(pause_data["until"])
            if datetime.now(timezone.utc) < until:
                return True, f"pause sampai {until.strftime('%Y-%m-%d %H:%M')} UTC"
        except Exception:
            pass
    journal = _safe_read_json(cfg.journal_file, [])
    if not isinstance(journal, list):
        return False, ""
    closed = [j for j in journal if isinstance(j, dict) and j.get("outcome") not in (None, "PENDING")]
    closed.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    consec = 0
    for j in closed:
        if j.get("outcome") == "LOSS":
            consec += 1
        else:
            break
    if consec >= cfg.drawdown_max_consecutive_losses:
        until = datetime.now(timezone.utc) + timedelta(hours=cfg.drawdown_pause_hours)
        with file_lock(_pause_path(cfg)):
            _atomic_write_json({
                "until": until.isoformat(),
                "reason": f"{consec} consecutive losses",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }, _pause_path(cfg))
        try:
            send_telegram(
                f"<b>DRAWDOWN GUARD</b>\n\n"
                f"{consec} loss berturut-turut terdeteksi.\n"
                f"Bot PAUSE {cfg.drawdown_pause_hours} jam.\n\n"
                f"Review performance Anda sebelum resume:\n"
                f"<code>python apex.py performance</code>\n\n"
                f"Resume manual: <code>python apex.py reset-drawdown</code>",
                cfg,
            )
        except Exception:
            pass
        return True, f"{consec} loss berturut-turut — pause"
    return False, ""


# ═══════════════════════════════════════════════════════════════
# HEARTBEAT
# ═══════════════════════════════════════════════════════════════

def _heartbeat_path(cfg: Config) -> str:
    return cfg.state_file + ".heartbeat"


def maybe_send_heartbeat(cfg: Config) -> None:
    if not cfg.heartbeat_enabled:
        return
    now = datetime.now(timezone.utc)
    if now.hour != cfg.heartbeat_hour_utc:
        return
    today = now.strftime("%Y-%m-%d")
    hb = _safe_read_json(_heartbeat_path(cfg), {})
    if hb.get("last_sent_date") == today:
        return
    reports = _safe_read_json(cfg.report_json, [])
    today_reports = [r for r in reports if isinstance(r, dict) and r.get("timestamp", "").startswith(today)] if isinstance(reports, list) else []
    signals_today = sum(1 for r in today_reports if r.get("notified"))
    journal = _safe_read_json(cfg.journal_file, [])
    pending = [j for j in journal if isinstance(j, dict) and j.get("outcome") == "PENDING"] if isinstance(journal, list) else []
    circuit = "OPEN" if not circuit_allow(cfg) else "CLOSED"
    pause_active = bool(_safe_read_json(_pause_path(cfg), {}).get("until"))
    msg = (
        f"<b>APEX DAILY HEARTBEAT</b>\n\n"
        f"Bot hidup, semua sistem normal.\n\n"
        f"Runs hari ini: <code>{len(today_reports)}</code>\n"
        f"Sinyal dikirim: <code>{signals_today}</code>\n"
        f"Pending trade: <code>{len(pending)}</code>\n"
        f"Circuit breaker: <code>{circuit}</code>\n"
        f"Drawdown pause: <code>{'AKTIF' if pause_active else 'off'}</code>\n\n"
        f"<code>{now.strftime('%Y-%m-%d %H:%M')} UTC</code>"
    )
    if send_telegram(msg, cfg):
        with file_lock(_heartbeat_path(cfg)):
            _atomic_write_json({"last_sent_date": today, "last_sent_at": now.isoformat()}, _heartbeat_path(cfg))


# ═══════════════════════════════════════════════════════════════
# DATA FETCHERS
# ═══════════════════════════════════════════════════════════════

def _candles_to_df(candles: list) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df["datetime"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(100.0)
    else:
        df["volume"] = 100.0
    return df[["datetime", "open", "high", "low", "close", "volume"]].reset_index(drop=True)


def fetch_deriv_ohlcv(cfg: Config, interval_str: str, count: int = 200, retries: int = 2) -> pd.DataFrame:
    if not HAS_WEBSOCKET:
        raise DataFetchError("websocket-client belum terpasang")
    gran_map = {"5min": 300, "15min": 900, "1h": 3600}
    granularity = gran_map.get(interval_str)
    if granularity is None:
        raise DataFetchError(f"Interval tidak didukung: {interval_str}")
    payload = {"ticks_history": cfg.deriv_symbol, "count": count, "end": "latest",
               "granularity": granularity, "style": "candles"}
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
                if data.get("msg_type") == "candles":
                    c = data.get("candles", [])
                    if c:
                        candles = c
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
    headers = {"User-Agent": f"Mozilla/5.0 (compatible; ApexBot/{VERSION})"}

    def _fetch_symbol(symbol: str) -> pd.DataFrame:
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                         params={"interval": interval, "range": "5d"}, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        result = data.get("chart", {}).get("result")
        if not result:
            raise DataFetchError(f"Yahoo {symbol}: no result")
        res = result[0]
        timestamps = res.get("timestamp", [])
        quote = res.get("indicators", {}).get("quote", [{}])[0]
        opens, highs, lows, closes, volumes = (quote.get(k, []) for k in ("open", "high", "low", "close", "volume"))
        rows = []
        for i, ts in enumerate(timestamps):
            if i >= len(opens) or opens[i] is None or closes[i] is None:
                continue
            rows.append({"epoch": int(ts), "open": opens[i], "high": highs[i], "low": lows[i],
                         "close": closes[i], "volume": volumes[i] if i < len(volumes) and volumes[i] else 100.0})
        if not rows:
            raise DataFetchError(f"Yahoo {symbol}: no valid rows")
        return _candles_to_df(rows)

    spot_cache = {"df": None}
    try:
        spot_cache["df"] = _fetch_symbol("XAUUSD=X")
        logging.info(f"Yahoo XAUUSD=X (spot): {len(spot_cache['df'])} bar OK")
        return spot_cache["df"].tail(count).reset_index(drop=True)
    except Exception as e:
        logging.warning(f"Yahoo spot XAUUSD=X gagal: {_redact(str(e))}")

    try:
        df_fut = _fetch_symbol("GC=F")
        spot_price = None
        if spot_cache["df"] is not None:
            spot_price = float(spot_cache["df"]["close"].iloc[-1])
        else:
            try:
                spot_r = requests.get("https://xaus.com/api/v1/spot", timeout=5)
                spot_r.raise_for_status()
                spot_price = float(spot_r.json().get("spot_usd_oz", 0))
            except Exception:
                spot_price = float(df_fut["close"].iloc[-1]) + float(os.getenv("APEX_YAHOO_OFFSET", "0"))
        if spot_price and spot_price > 0:
            offset = spot_price - float(df_fut["close"].iloc[-1])
            logging.info(f"Yahoo GC=F + offset dinamis {offset:+.3f} (spot={spot_price:.3f})")
            for col in ["open", "high", "low", "close"]:
                df_fut[col] = df_fut[col] + offset
        logging.info(f"Yahoo GC=F (futures+offset): {len(df_fut)} bar OK")
        return df_fut.tail(count).reset_index(drop=True)
    except Exception as e:
        raise DataFetchError(f"Yahoo semua symbol gagal: {_redact(str(e))}")


def fetch_twelvedata_ohlcv(cfg: Config, interval_str: str, count: int = 200, retries: int = 2) -> pd.DataFrame:
    api_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    if not api_key:
        raise DataFetchError("TWELVEDATA_API_KEY tidak diset")
    interval_map = {"5min": "5min", "15min": "15min", "1h": "1h"}
    interval = interval_map.get(interval_str)
    if interval is None:
        raise DataFetchError(f"Interval tidak didukung: {interval_str}")
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get("https://api.twelvedata.com/time_series",
                             params={"symbol": "XAU/USD", "interval": interval,
                                     "outputsize": count, "apikey": api_key}, timeout=10)
            if r.status_code in (401, 403):
                raise DataFetchError("TwelveData: API key tidak valid")
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
        except DataFetchError:
            raise
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 ** attempt)
    raise DataFetchError(f"TwelveData gagal: {_redact(str(last_err))}")


def fetch_ohlcv_cascade(cfg: Config, interval_str: str, count: int = 200) -> tuple:
    if not circuit_allow(cfg):
        raise DataFetchError("Circuit breaker aktif")
    errors = []
    sources = []
    if cfg.enable_deriv:
        sources.append(("Deriv", fetch_deriv_ohlcv))
    sources.append(("Yahoo", fetch_yahoo_ohlcv))
    sources.append(("TwelveData", fetch_twelvedata_ohlcv))
    for name, fn in sources:
        try:
            df = fn(cfg, interval_str, count)
            circuit_record_success(cfg)
            logging.info(f"Data OK dari {name} ({len(df)} bar)")
            return df, name
        except Exception as e:
            errors.append(f"{name}: {_redact(str(e))}")
            logging.warning(f"Feed '{name}' gagal: {_redact(str(e))}")
    circuit_record_failure(cfg)
    raise DataFetchError("SEMUA FEED GAGAL | " + " || ".join(errors))


# ═══════════════════════════════════════════════════════════════
# VALIDATION & MARKET GATES
# ═══════════════════════════════════════════════════════════════

_INTERVAL_SECONDS = {"5min": 300, "15min": 900, "1h": 3600}


def validate_ohlcv(df: pd.DataFrame, interval_str: str, min_bars: int, cfg: Config) -> pd.DataFrame:
    if df is None or len(df) == 0:
        raise ValueError("Data kosong")
    df = df.dropna(subset=["open", "high", "low", "close"]).copy()
    if len(df) < min_bars:
        raise ValueError(f"Bar tidak cukup: {len(df)} < {min_bars}")
    interval_sec = _INTERVAL_SECONDS.get(interval_str, 300)
    last_time = df["datetime"].iloc[-1]
    if last_time.tzinfo is None:
        last_time = last_time.replace(tzinfo=timezone.utc)
    age_sec = (datetime.now(timezone.utc) - last_time).total_seconds()
    if age_sec > interval_sec * cfg.max_stale_mult:
        raise ValueError(f"Data basi: {age_sec:.0f}s")
    recent = df.tail(50)
    if len(recent) >= 10:
        diffs = recent["datetime"].diff().dt.total_seconds().dropna()
        gaps = (diffs > interval_sec * 1.5).sum()
        if len(diffs) > 0 and (gaps / len(diffs)) > cfg.max_gap_ratio:
            raise ValueError(f"Terlalu banyak gap: {gaps}/{len(diffs)}")
    return df.reset_index(drop=True)


def is_market_open(now_utc: datetime) -> bool:
    wd, h = now_utc.weekday(), now_utc.hour
    if wd == 5:
        return False
    if wd == 6:
        return h >= 22
    if wd == 4:
        return h < 21
    return True


def is_institutional_killzone(now_utc: datetime) -> bool:
    h = now_utc.hour
    return (7 <= h <= 11) or (13 <= h <= 17)


# ═══════════════════════════════════════════════════════════════
# NEWS SHIELD
# ═══════════════════════════════════════════════════════════════

def _news_cache_path(cfg: Config) -> str:
    return cfg.state_file + ".news"


def _load_news_cache(cfg: Config) -> dict:
    data = _safe_read_json(_news_cache_path(cfg), {"ok": False, "ts": None, "blocked": True, "reason": ""})
    if not isinstance(data, dict):
        return {"ok": False, "ts": None, "blocked": True, "reason": ""}
    if data.get("ts"):
        try:
            data["ts"] = datetime.fromisoformat(data["ts"])
        except Exception:
            data["ts"] = None
    return data


def _save_news_cache(cfg: Config, cache: dict) -> None:
    c = dict(cache)
    if c.get("ts"):
        try:
            c["ts"] = c["ts"].isoformat()
        except Exception:
            c["ts"] = None
    with file_lock(_news_cache_path(cfg)):
        _atomic_write_json(c, _news_cache_path(cfg))


def _is_high_impact_window(now_utc: datetime) -> bool:
    wd, h, d = now_utc.weekday(), now_utc.hour, now_utc.day
    if wd == 4 and 11 <= h <= 14:
        return True
    if wd in (1, 2) and 8 <= d <= 20 and 12 <= h <= 14:
        return True
    if wd == 2 and 18 <= h <= 20:
        return True
    if wd == 3 and 12 <= h <= 14:
        return True
    return False


def check_news_shield(cfg: Config) -> tuple:
    now = datetime.now(timezone.utc)
    cache = _load_news_cache(cfg)
    if cache["ok"] and cache["ts"] and (now - cache["ts"]).total_seconds() < cfg.news_cache_seconds:
        return cache["blocked"], cache["reason"]
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=5)
        r.raise_for_status()
        events = r.json()
        parsed = 0
        for e in events:
            if e.get("country") != "USD" or e.get("impact") != "High":
                continue
            try:
                ev_dt = datetime.strptime(e["date"], "%Y-%m-%dT%H:%M:%S%z")
            except Exception:
                try:
                    ev_dt = datetime.fromisoformat(e["date"].replace("Z", "+00:00"))
                except Exception:
                    continue
            parsed += 1
            diff_min = abs((ev_dt - now).total_seconds() / 60)
            if diff_min <= cfg.news_blackout_minutes:
                reason = f"news '{e.get('title', '?')}' dalam {diff_min:.0f}m"
                _save_news_cache(cfg, {"ok": True, "ts": now, "blocked": True, "reason": reason})
                return True, reason
        if parsed == 0 and events and cfg.news_fail_mode == "closed":
            reason = "news parse anomaly"
            _save_news_cache(cfg, {"ok": True, "ts": now, "blocked": True, "reason": reason})
            return True, reason
        _save_news_cache(cfg, {"ok": True, "ts": now, "blocked": False, "reason": ""})
        return False, ""
    except Exception as ex:
        logging.error(f"News shield gagal: {_redact(str(ex))}")
        if _is_high_impact_window(now):
            reason = "high-impact window (API down)"
            _save_news_cache(cfg, {"ok": True, "ts": now, "blocked": True, "reason": reason})
            return True, reason
        if cfg.news_fail_mode == "closed":
            reason = "news API down — fail-closed"
            _save_news_cache(cfg, {"ok": True, "ts": now, "blocked": True, "reason": reason})
            return True, reason
        _save_news_cache(cfg, {"ok": True, "ts": now, "blocked": False, "reason": "fail-open"})
        return False, ""


# ═══════════════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════════════

def compute_true_range(df: pd.DataFrame) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    return pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)


def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    tr = compute_true_range(df)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
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
    gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    rsi = 100 - (100 / (1 + gain / (loss + 1e-9)))
    val = rsi.iloc[-1]
    return 50.0 if pd.isna(val) else float(val)


# ═══════════════════════════════════════════════════════════════
# SMC PATTERNS
# ═══════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════
# CONFLUENCE & SIGNAL
# ═══════════════════════════════════════════════════════════════

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
    return {"total": total, "trend": trend_pts, "momentum": momo,
            "smc": smc, "adx": adx_pts, "ha": ha_pts,
            "rsi": round(rsi, 1), "adx_value": round(adx_val, 1)}


@dataclass
class DecisionLog:
    passed: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        label = f"{name}" + (f" ({detail})" if detail else "")
        (self.passed if ok else self.failed).append(("OK " if ok else "NO ") + label)
        return ok
    def to_dict(self) -> dict:
        return {"passed": self.passed, "failed": self.failed}


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


_EMPTY_OB = {"detected": False, "type": "NONE", "high": 0.0, "low": 0.0}
_EMPTY_FVG = {"detected": False, "type": "NONE", "level": 0.0}


def generate_signal(df_m5, df_m15, df_h1, cfg: Config) -> SignalResult:
    log = DecisionLog()
    adx_val = calculate_adx(df_m5)
    if not log.check("ADX", adx_val >= cfg.adx_min, f"{adx_val:.1f} >= {cfg.adx_min}"):
        return SignalResult("NEUTRAL", {"reason": f"ADX {adx_val:.1f} < {cfg.adx_min}"}, log, None, adx_val, "NEUTRAL", _EMPTY_OB, _EMPTY_FVG)
    ob = detect_institutional_order_blocks(df_m5)
    fvg = detect_fair_value_gap(df_m5)
    if not log.check("SMC", ob["detected"] or fvg["detected"], ob["type"] if ob["detected"] else fvg["type"]):
        return SignalResult("NEUTRAL", {"reason": "tidak ada OB/FVG"}, log, None, adx_val, "NEUTRAL", ob, fvg)
    ha = get_heikin_ashi_trend(df_m5)
    conf = compute_confluence(df_m5, df_m15, df_h1, ob, fvg, adx_val, ha)
    if conf["total"] >= cfg.score_threshold and ha == "BULLISH" and ob["type"] in ("BULLISH_OB", "NONE"):
        log.check("Confluence", True, f"+{conf['total']} >= {cfg.score_threshold}")
        log.check("HA", True, ha)
        return SignalResult("BUY", {}, log, conf, adx_val, ha, ob, fvg)
    if conf["total"] <= -cfg.score_threshold and ha == "BEARISH" and ob["type"] in ("BEARISH_OB", "NONE"):
        log.check("Confluence", True, f"{conf['total']} <= -{cfg.score_threshold}")
        log.check("HA", True, ha)
        return SignalResult("SELL", {}, log, conf, adx_val, ha, ob, fvg)
    log.check("Confluence", False, f"{conf['total']:+d} vs +/-{cfg.score_threshold}; HA={ha}")
    return SignalResult("NEUTRAL", {"reason": "confluence rendah"}, log, conf, adx_val, ha, ob, fvg)


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


# ═══════════════════════════════════════════════════════════════
# OUTCOME TRACKING (NEW)
# ═══════════════════════════════════════════════════════════════

def _update_single_outcome(entry: dict, df_h1: pd.DataFrame) -> bool:
    try:
        entry_ts = pd.Timestamp(entry["timestamp"], tz="UTC")
    except Exception:
        return False
    entry_hour = entry_ts.floor("h")
    bars = df_h1[df_h1["datetime"] > entry_hour]
    if len(bars) == 0:
        return False
    direction = entry.get("signal")
    entry_price = float(entry.get("entry", 0))
    sl = float(entry.get("sl", 0))
    tp1 = float(entry.get("tp1", 0))
    tp2 = float(entry.get("tp2", 0))
    tp3 = float(entry.get("tp3", 0))
    if entry_price <= 0 or sl <= 0 or tp1 <= 0:
        return False
    hit_tp1 = entry.get("hit_tp1", False)
    hit_tp2 = entry.get("hit_tp2", False)
    hit_tp3 = entry.get("hit_tp3", False)
    hit_sl = entry.get("hit_sl", False)
    hit_be = entry.get("hit_be", False)
    changed = False
    for _, bar in bars.iterrows():
        if hit_tp3 or hit_sl or hit_be:
            break
        current_sl = entry_price if hit_tp1 else sl
        if direction == "BUY":
            if bar["low"] <= current_sl:
                if hit_tp1:
                    hit_be = True
                    entry["hit_be"] = True
                    entry["hit_be_at"] = str(bar["datetime"])
                else:
                    hit_sl = True
                    entry["hit_sl"] = True
                    entry["hit_sl_at"] = str(bar["datetime"])
                changed = True
                continue
            if not hit_tp1 and bar["high"] >= tp1:
                hit_tp1 = True
                entry["hit_tp1"] = True
                entry["hit_tp1_at"] = str(bar["datetime"])
                changed = True
            if not hit_tp2 and bar["high"] >= tp2:
                hit_tp2 = True
                entry["hit_tp2"] = True
                entry["hit_tp2_at"] = str(bar["datetime"])
                changed = True
            if not hit_tp3 and bar["high"] >= tp3:
                hit_tp3 = True
                entry["hit_tp3"] = True
                entry["hit_tp3_at"] = str(bar["datetime"])
                changed = True
        else:
            if bar["high"] >= current_sl:
                if hit_tp1:
                    hit_be = True
                    entry["hit_be"] = True
                    entry["hit_be_at"] = str(bar["datetime"])
                else:
                    hit_sl = True
                    entry["hit_sl"] = True
                    entry["hit_sl_at"] = str(bar["datetime"])
                changed = True
                continue
            if not hit_tp1 and bar["low"] <= tp1:
                hit_tp1 = True
                entry["hit_tp1"] = True
                entry["hit_tp1_at"] = str(bar["datetime"])
                changed = True
            if not hit_tp2 and bar["low"] <= tp2:
                hit_tp2 = True
                entry["hit_tp2"] = True
                entry["hit_tp2_at"] = str(bar["datetime"])
                changed = True
            if not hit_tp3 and bar["low"] <= tp3:
                hit_tp3 = True
                entry["hit_tp3"] = True
                entry["hit_tp3_at"] = str(bar["datetime"])
                changed = True
    if entry.get("outcome") == "PENDING":
        if hit_tp3:
            entry["outcome"] = "WIN_TP3"
            entry["r_achieved"] = 3.5
            changed = True
        elif hit_be:
            entry["outcome"] = "WIN_BE"
            if hit_tp2:
                entry["r_achieved"] = 1.0
            elif hit_tp1:
                entry["r_achieved"] = 0.33
            else:
                entry["r_achieved"] = 0.0
            changed = True
        elif hit_sl:
            entry["outcome"] = "LOSS"
            entry["r_achieved"] = -1.0
            changed = True
    return changed


def update_journal_outcomes(df_h1_full: pd.DataFrame, cfg: Config) -> None:
    if not cfg.track_outcomes:
        return
    journal = _safe_read_json(cfg.journal_file, [])
    if not isinstance(journal, list):
        return
    pending = [j for j in journal if isinstance(j, dict) and j.get("outcome") == "PENDING"]
    if not pending:
        return
    changed = False
    events = []
    for entry in journal:
        if not isinstance(entry, dict) or entry.get("outcome") != "PENDING":
            continue
        was_tp1 = entry.get("hit_tp1", False)
        was_outcome = entry.get("outcome")
        did = _update_single_outcome(entry, df_h1_full)
        if did:
            changed = True
            if not was_tp1 and entry.get("hit_tp1"):
                events.append(("TP1_HIT", entry))
            if was_outcome == "PENDING" and entry.get("outcome") != "PENDING":
                events.append(("CLOSED", entry))
    if changed:
        with file_lock(cfg.journal_file):
            _atomic_write_json(journal, cfg.journal_file)
        if cfg.notify_on_tp_hit:
            for kind, entry in events:
                try:
                    _send_outcome_update(kind, entry, cfg)
                except Exception as e:
                    logging.warning(f"Gagal kirim update outcome: {_redact(str(e))}")


def _send_outcome_update(kind: str, entry: dict, cfg: Config) -> None:
    sig = entry.get("signal")
    entry_price = entry.get("entry")
    p = cfg.price_decimals
    if kind == "TP1_HIT":
        msg = (
            f"<b>TP1 HIT</b>\n\n"
            f"{_h(sig)} @ <code>{entry_price}</code>\n"
            f"Target 1 tercapai.\n\n"
            f"<b>Aksi:</b> ambil partial 1/3, pindah SL ke <code>{entry_price}</code> (breakeven).\n"
            f"<i>Bot akan track TP2 & TP3 otomatis.</i>"
        )
    elif kind == "CLOSED":
        outcome = entry.get("outcome", "?")
        r = entry.get("r_achieved", 0)
        if outcome.startswith("WIN"):
            emoji = "TP3" if outcome == "WIN_TP3" else "TP1+BE"
            msg = (
                f"<b>TRADE CLOSED — WIN ({emoji})</b>\n\n"
                f"{_h(sig)} entry <code>{entry_price}</code>\n"
                f"R achieved: <code>{r:+.2f}R</code>\n\n"
                f"<i>Journal updated. Cek performance: python apex.py performance</i>"
            )
        else:
            msg = (
                f"<b>TRADE CLOSED — LOSS</b>\n\n"
                f"{_h(sig)} entry <code>{entry_price}</code>\n"
                f"SL kena. R: <code>{r:+.2f}R</code>\n\n"
                f"<i>Journal updated. Cek drawdown status: python apex.py health</i>"
            )
    else:
        return
    send_telegram(msg, cfg)


# ═══════════════════════════════════════════════════════════════
# NOTIFICATION
# ═══════════════════════════════════════════════════════════════

def _alert_path(cfg: Config) -> str:
    return cfg.state_file + ".alert"


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
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                          timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.error(f"Telegram gagal: {_redact(str(e))}")
        return False


def alert_error(msg: str, cfg: Config) -> None:
    now = datetime.now(timezone.utc)
    last_data = _safe_read_json(_alert_path(cfg), {"last": None})
    last = None
    if isinstance(last_data, dict) and last_data.get("last"):
        try:
            last = datetime.fromisoformat(last_data["last"])
        except Exception:
            last = None
    if last and (now - last).total_seconds() < 300:
        return
    try:
        with file_lock(_alert_path(cfg)):
            _atomic_write_json({"last": now.isoformat()}, _alert_path(cfg))
    except Exception:
        pass
    send_telegram(f"<b>APEX ERROR</b>\n\n<code>{_h(_redact(msg))}</code>", cfg)


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
    breakdown = f"T{conf['trend']:+d} M{conf['momentum']:+d} S{conf['smc']:+d} A{conf['adx']:+d} H{conf['ha']:+d}"
    return (
        f"<b>APEX PREDATOR v{VERSION}</b>\n\n"
        f"<b>{_h(cfg.symbol)}</b> | <code>{_h(source)}</code>\n"
        f"<b>{_h(signal_type)}</b> @ <code>{price:.{p}f}</code>\n\n"
        f"SL: <code>{_h(sl)}</code>\n"
        f"TP1: <code>{_h(tp1)}</code> (spread-aware)\n"
        f"TP2: <code>{_h(tp2)}</code>\n"
        f"TP3: <code>{_h(tp3)}</code>\n"
        f"Lot: <code>{_h(lot)}</code> (risk {cfg.risk_percentage}%)\n\n"
        f"SMC: <code>{_h(smc)}</code>\n"
        f"HA: <code>{_h(ha)}</code> | ADX: <code>{adx_val:.1f}</code> | RSI: <code>{conf['rsi']}</code>\n"
        f"Confluence: <code>{conf['total']:+d}/+/-{MAX_CONFLUENCE}</code> <code>{_h(breakdown)}</code>\n"
        f"Run <code>#{run_num}</code> | <code>{_h(now_str)} UTC</code>\n\n"
        f"<i>Bot akan track outcome otomatis.\nManual execution via MT5.</i>"
    )


# ═══════════════════════════════════════════════════════════════
# MAIN CYCLE
# ═══════════════════════════════════════════════════════════════

def run_once(cfg: Config) -> None:
    try:
        _run_once_inner(cfg)
    except KeyboardInterrupt:
        raise
    except Exception as e:
        logging.exception(f"FATAL: {_redact(str(e))}")
        try:
            alert_error(f"FATAL: {type(e).__name__}: {e}", cfg)
        except Exception:
            pass


def _run_once_inner(cfg: Config) -> None:
    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")

    with file_lock(cfg.state_file):
        state = load_state(cfg)
        state["run_count"] = int(state.get("run_count", 0)) + 1
        run_num = state["run_count"]
        _atomic_write_json(state, cfg.state_file)

    logging.info(f"=== APEX v{VERSION} Run #{run_num} @ {now_str} UTC ===")

    # Heartbeat check (does not depend on market open)
    try:
        maybe_send_heartbeat(cfg)
    except Exception as e:
        logging.warning(f"Heartbeat gagal: {_redact(str(e))}")

    if not is_market_open(now_utc):
        logging.info("Market tutup - skip.")
        return

    # Drawdown guard (checked before killzone/news to persist pause)
    blocked_dd, reason_dd = should_block_due_to_drawdown(cfg)
    if blocked_dd:
        logging.warning(f"DRAWDOWN GUARD: {reason_dd}")
        return

    if not is_institutional_killzone(now_utc):
        logging.info("Di luar killzone - siaga.")
        return

    try:
        blocked, reason = check_news_shield(cfg)
    except Exception as e:
        logging.error(f"News shield crash: {_redact(str(e))}")
        blocked = (cfg.news_fail_mode == "closed")
        reason = "news shield crash"
    if blocked:
        logging.info(f"News block: {reason}")
        return

    if count_daily_signals(cfg) >= cfg.max_daily_signals:
        logging.info(f"Daily limit {cfg.max_daily_signals} tercapai.")
        return

    try:
        raw_m5, source = fetch_ohlcv_cascade(cfg, "5min", 200)
        df_m5 = validate_ohlcv(raw_m5, "5min", cfg.min_bars_m5, cfg).iloc[:-1].reset_index(drop=True)
        raw_m15, _ = fetch_ohlcv_cascade(cfg, "15min", 150)
        df_m15 = validate_ohlcv(raw_m15, "15min", cfg.min_bars_m15, cfg).iloc[:-1].reset_index(drop=True)
        raw_h1, _ = fetch_ohlcv_cascade(cfg, "1h", cfg.outcome_bars_h1)
        df_h1_full = validate_ohlcv(raw_h1, "1h", cfg.min_bars_h1, cfg)
        df_h1 = df_h1_full.iloc[:-1].reset_index(drop=True)
    except Exception as e:
        logging.error(f"Data pipeline gagal: {_redact(str(e))}")
        alert_error(f"Data pipeline: {e}", cfg)
        return

    # Update outcomes BEFORE generating new signal (uses same H1 data)
    try:
        update_journal_outcomes(df_h1_full, cfg)
    except Exception as e:
        logging.warning(f"Outcome tracking gagal: {_redact(str(e))}")

    current_price = float(df_m5["close"].iloc[-1])
    res = generate_signal(df_m5, df_m15, df_h1, cfg)

    notify = False
    with file_lock(cfg.state_file):
        state = load_state(cfg)
        if res.signal != "NEUTRAL" and should_notify(state, res.signal, now_utc, cfg):
            state["last_signal_type"] = res.signal
            state["last_signal_timestamp"] = now_str
            state["notify_claimed_at"] = now_str
            _atomic_write_json(state, cfg.state_file)
            notify = True

    sl, tp1, tp2, tp3, lot = calculate_multi_target_tpsl(df_m5, res.signal, cfg)

    report = {
        "timestamp": now_str, "run_count": run_num, "active_source": source,
        "final_signal": res.signal, "notified": notify,
        "entry_price": round(current_price, cfg.price_decimals),
        "confidence": res.conf, "adx": round(res.adx, 2), "heikin_ashi": res.ha,
        "ob_type": res.ob["type"] if res.ob["detected"] else "NONE",
        "fvg_type": res.fvg["type"] if res.fvg["detected"] else "NONE",
        "recommended_sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "suggested_lot": lot, "decision_path": res.log.to_dict(),
    }
    append_report(report, cfg)

    if notify and res.signal != "NEUTRAL":
        journal_id = f"{now_str}_{res.signal}"
        entry = {
            "id": journal_id, "timestamp": now_str, "symbol": cfg.symbol,
            "signal": res.signal, "entry": round(current_price, cfg.price_decimals),
            "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "lot": lot,
            "confluence": res.conf, "outcome": "PENDING", "pnl_usd": None,
            "notes": "", "notify_status": "PENDING",
            "hit_tp1": False, "hit_tp2": False, "hit_tp3": False,
            "hit_sl": False, "hit_be": False, "r_achieved": 0.0,
        }
        append_journal(entry, cfg)
        msg = build_message(cfg, run_num, now_str, source, res.signal,
                            current_price, sl, tp1, tp2, tp3, lot,
                            res.conf, res.ob, res.fvg, res.ha, res.adx)
        ok = send_telegram(msg, cfg)
        update_journal_status(journal_id, {"notify_status": "SENT" if ok else "FAILED"}, cfg)
        if not ok:
            logging.warning(f"Telegram gagal kirim signal {res.signal}")

    logging.info("=== Cycle Complete ===")


# ═══════════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════════

def cmd_selftest(cfg: Config) -> int:
    print(f"=== SELF TEST APEX v{VERSION} ===\n")
    problems = []
    print("[1/6] Paket Python...")
    for pkg, name in [("pandas", "pandas"), ("numpy", "numpy"), ("requests", "requests")]:
        try:
            __import__(pkg)
            print(f"  OK   {name}")
        except ImportError:
            print(f"  FAIL {name} tidak terpasang")
            problems.append(f"{name} belum diinstall")
    print(f"  {'OK' if HAS_WEBSOCKET else 'WARN'} websocket-client {'terpasang' if HAS_WEBSOCKET else 'tidak ada — Deriv skip'}")

    print("\n[2/6] Environment variables...")
    tok = os.getenv("TELEGRAM_TOKEN", "").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if tok and chat:
        print(f"  OK   TELEGRAM_TOKEN ({len(tok)} chars)")
        print(f"  OK   TELEGRAM_CHAT_ID ({chat[:6]}...)")
    else:
        print("  FAIL TELEGRAM_TOKEN atau TELEGRAM_CHAT_ID kosong")
        problems.append("Telegram env belum diset")
    td = os.getenv("TWELVEDATA_API_KEY", "").strip()
    print(f"  {'OK' if td else 'WARN'} TWELVEDATA_API_KEY {'ada' if td else 'kosong'}")

    print("\n[3/6] Write permission...")
    try:
        with open(".apex_write_test", "w") as f:
            f.write("x")
        os.remove(".apex_write_test")
        print("  OK   folder writable")
    except Exception as e:
        print(f"  FAIL tidak bisa tulis: {e}")
        problems.append("Folder tidak writable")

    print("\n[4/6] Telegram test...")
    if tok and chat and not cfg.dry_run:
        if send_telegram(f"<b>APEX v{VERSION} SELFTEST</b>\n\nSetup benar. Bot siap tempur.", cfg):
            print("  OK   test message terkirim")
        else:
            print("  FAIL kirim gagal")
            problems.append("Telegram tidak bisa kirim")
    else:
        print("  SKIP (dry-run atau env kosong)")

    print("\n[5/6] Data feeds...")
    for name, fn in [("Deriv", fetch_deriv_ohlcv if HAS_WEBSOCKET and cfg.enable_deriv else None),
                     ("Yahoo", fetch_yahoo_ohlcv),
                     ("TwelveData", fetch_twelvedata_ohlcv if td else None)]:
        if fn is None:
            print(f"  SKIP {name}")
            continue
        try:
            df = fn(cfg, "5min", 50)
            print(f"  OK   {name}: {len(df)} bar, terakhir {df['datetime'].iloc[-1]}")
        except Exception as e:
            print(f"  FAIL {name}: {_redact(str(e))[:80]}")

    print("\n[6/6] Config aktif:")
    print(f"  Symbol          : {cfg.symbol}")
    print(f"  Score threshold : {cfg.score_threshold}/{MAX_CONFLUENCE}")
    print(f"  ADX min         : {cfg.adx_min}")
    print(f"  ATR SL mult     : {cfg.atr_sl_mult}")
    print(f"  Risk per trade  : {cfg.risk_percentage}% dari ${cfg.account_balance_usd:.0f}")
    print(f"  Cooldown        : {cfg.cooldown_minutes}m | Max/hari: {cfg.max_daily_signals}")
    print(f"  Drawdown guard  : {'ON' if cfg.drawdown_guard_enabled else 'OFF'} ({cfg.drawdown_max_consecutive_losses} loss → pause {cfg.drawdown_pause_hours}h)")
    print(f"  Heartbeat       : {'ON' if cfg.heartbeat_enabled else 'OFF'} @ {cfg.heartbeat_hour_utc}:00 UTC")
    print(f"  News fail mode  : {cfg.news_fail_mode}")
    print(f"  Dry run         : {cfg.dry_run}")

    print()
    if problems:
        print(f"ADA {len(problems)} MASALAH:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print("SEMUA OK. Bot siap tempur.")
    return 0


def cmd_status(cfg: Config) -> None:
    state = load_state(cfg)
    reports = _safe_read_json(cfg.report_json, [])
    journal = _safe_read_json(cfg.journal_file, [])
    signals_today = count_daily_signals(cfg)
    recent = reports[-5:] if isinstance(reports, list) else []
    pending = [j for j in journal if isinstance(j, dict) and j.get("outcome") == "PENDING"] if isinstance(journal, list) else []
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    week_sent = sum(1 for j in journal if isinstance(j, dict) and j.get("notify_status") == "SENT" and j.get("timestamp", "") >= week_ago) if isinstance(journal, list) else 0
    pause_active = bool(_safe_read_json(_pause_path(cfg), {}).get("until"))
    print(f"=== STATUS APEX v{VERSION} ===\n")
    print(f"Symbol          : {cfg.symbol}")
    print(f"Run count       : {state.get('run_count', 0)}")
    print(f"Last signal     : {state.get('last_signal_type')} @ {state.get('last_signal_timestamp') or '-'}")
    print(f"Signals today   : {signals_today}/{cfg.max_daily_signals}")
    print(f"Signals 7 hari  : {week_sent}")
    print(f"Pending trade   : {len(pending)}")
    print(f"Circuit breaker : {'OPEN' if not circuit_allow(cfg) else 'CLOSED'}")
    print(f"Drawdown pause  : {'AKTIF' if pause_active else 'off'}")
    print(f"\n--- 5 report terakhir ---")
    for r in recent:
        if isinstance(r, dict):
            conf = r.get("confidence") or {}
            total = conf.get("total", 0) if isinstance(conf, dict) else 0
            flag = "SENT" if r.get("notified") else "    "
            print(f"  {r.get('timestamp')} | {r.get('final_signal','?'):7s} | conf={total:+d} | {flag} | src={r.get('active_source')}")


def cmd_health(cfg: Config) -> None:
    print(f"=== HEALTH CHECK APEX v{VERSION} ===\n")
    state = load_state(cfg)
    last_run = state.get("last_signal_timestamp") or "belum pernah"
    runs = state.get("run_count", 0)
    print(f"Total runs      : {runs}")
    print(f"Last signal     : {last_run}")
    report_mtime = os.path.getmtime(cfg.report_json) if os.path.exists(cfg.report_json) else 0
    if report_mtime > 0:
        age_min = (time.time() - report_mtime) / 60
        print(f"Report terakhir : {age_min:.1f} menit lalu")
        if age_min > 15:
            print(f"  WARNING: Bot mungkin mati. Cek cron.")
    else:
        print(f"Report terakhir : BELUM ADA")
    circuit = _load_circuit(cfg)
    print(f"Circuit breaker : failures={circuit.get('failures',0)}, opened_until={circuit.get('opened_until') or '-'}")
    pause = _safe_read_json(_pause_path(cfg), {})
    print(f"Drawdown pause  : {pause.get('until') or 'off'}")
    news = _load_news_cache(cfg)
    if news.get("ts"):
        age = (datetime.now(timezone.utc) - news["ts"]).total_seconds()
        print(f"News cache      : {age:.0f}s lalu, blocked={news.get('blocked')}")
    else:
        print(f"News cache      : kosong")
    for f in [cfg.state_file, cfg.report_json, cfg.journal_file]:
        size = os.path.getsize(f) if os.path.exists(f) else 0
        print(f"{f:35s}: {size:>8d} bytes")


def cmd_performance(cfg: Config) -> None:
    journal = _safe_read_json(cfg.journal_file, [])
    if not isinstance(journal, list) or len(journal) == 0:
        print("Belum ada trade di journal.")
        return
    closed = [j for j in journal if isinstance(j, dict) and j.get("outcome") not in (None, "PENDING")]
    pending = [j for j in journal if isinstance(j, dict) and j.get("outcome") == "PENDING"]
    if not closed:
        print(f"Belum ada trade closed. Pending: {len(pending)}")
        return

    wins = [j for j in closed if str(j.get("outcome", "")).startswith("WIN")]
    losses = [j for j in closed if j.get("outcome") == "LOSS"]
    total_r = sum(float(j.get("r_achieved", 0)) for j in closed)
    win_rate = len(wins) / len(closed) * 100 if closed else 0

    closed_sorted = sorted(closed, key=lambda x: x.get("timestamp", ""), reverse=True)
    consec = 0
    for j in closed_sorted:
        if j.get("outcome") == "LOSS":
            consec += 1
        else:
            break

    max_consec = 0
    current_consec = 0
    for j in sorted(closed, key=lambda x: x.get("timestamp", "")):
        if j.get("outcome") == "LOSS":
            current_consec += 1
            max_consec = max(max_consec, current_consec)
        else:
            current_consec = 0

    ev_per_trade = total_r / len(closed) if closed else 0

    print(f"=== PERFORMANCE APEX v{VERSION} ===\n")
    print(f"Total trades closed : {len(closed)}")
    print(f"Pending             : {len(pending)}")
    print(f"Win rate            : {len(wins)}/{len(closed)} = {win_rate:.1f}%")
    print(f"  TP3 wins          : {sum(1 for j in wins if j.get('outcome') == 'WIN_TP3')}")
    print(f"  BE wins (TP1 hit) : {sum(1 for j in wins if j.get('outcome') == 'WIN_BE')}")
    print(f"Losses              : {len(losses)}")
    print(f"Total R             : {total_r:+.2f}R")
    print(f"R per trade (EV)    : {ev_per_trade:+.3f}R")
    print(f"Max consecutive loss: {max_consec}")
    print(f"Current streak loss : {consec}")
    if cfg.drawdown_guard_enabled and consec >= cfg.drawdown_max_consecutive_losses:
        print(f"  WARNING: Drawdown guard AKTIF - resume via: python apex.py reset-drawdown")
    print()

    if len(closed) >= 20:
        if ev_per_trade > 0.2:
            print("Status: PROFITABLE (EV > 0.2R per trade)")
        elif ev_per_trade > 0:
            print("Status: MARGINAL (EV positif tapi tipis)")
        else:
            print("Status: RUGI - review parameter sebelum lanjut")
    else:
        print(f"Sample kecil ({len(closed)} trade). Butuh minimal 20 untuk evaluasi valid.")

    print(f"\n--- 5 trade terakhir ---")
    for j in closed_sorted[:5]:
        sig = j.get("signal", "?")
        ts = j.get("timestamp", "?")[:16]
        outcome = j.get("outcome", "?")
        r = float(j.get("r_achieved", 0))
        print(f"  {ts} | {sig:4s} | {outcome:10s} | R:{r:+.2f}")


def cmd_export(cfg: Config) -> None:
    journal = _safe_read_json(cfg.journal_file, [])
    if not isinstance(journal, list) or not journal:
        print("Journal kosong.")
        return
    out_file = f"journal_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    fields = ["id", "timestamp", "symbol", "signal", "entry", "sl", "tp1", "tp2", "tp3", "lot",
              "outcome", "r_achieved", "hit_tp1", "hit_tp2", "hit_tp3", "hit_sl", "hit_be",
              "notify_status"]
    try:
        with open(out_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for entry in journal:
                if isinstance(entry, dict):
                    writer.writerow({k: entry.get(k, "") for k in fields})
        print(f"Exported {len(journal)} entries -> {out_file}")
        print("Buka dengan Excel untuk analisis lebih lanjut.")
    except Exception as e:
        print(f"Export gagal: {_redact(str(e))}")


def cmd_reset_drawdown(cfg: Config) -> None:
    pause_path = _pause_path(cfg)
    if os.path.exists(pause_path):
        os.remove(pause_path)
        print("Drawdown pause di-reset. Bot akan trade lagi di run berikutnya.")
        send_telegram("<b>DRAWDOWN RESET</b>\n\nBot di-resume manual oleh user.", cfg)
    else:
        print("Tidak ada pause aktif.")


def cmd_reset(cfg: Config) -> None:
    for path in [cfg.state_file, cfg.state_file + ".breaker", cfg.state_file + ".news",
                 cfg.state_file + ".alert", cfg.state_file + ".pause",
                 cfg.state_file + ".heartbeat"]:
        if os.path.exists(path):
            os.remove(path)
            print(f"Dihapus: {path}")
    print("State di-reset. Report & journal tetap utuh (untuk audit).")


def main():
    try:
        cfg = Config.from_env()
    except ValueError as e:
        print(f"\n{e}\n")
        print("Perbaiki konfigurasi di atas, lalu jalankan ulang.")
        sys.exit(2)

    setup_logging(cfg)

    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "selftest":
            sys.exit(cmd_selftest(cfg))
        elif cmd == "status":
            cmd_status(cfg)
            sys.exit(0)
        elif cmd == "health":
            cmd_health(cfg)
            sys.exit(0)
        elif cmd == "performance":
            cmd_performance(cfg)
            sys.exit(0)
        elif cmd == "export":
            cmd_export(cfg)
            sys.exit(0)
        elif cmd == "reset-drawdown":
            cmd_reset_drawdown(cfg)
            sys.exit(0)
        elif cmd == "reset":
            cmd_reset(cfg)
            sys.exit(0)
        elif cmd in ("help", "-h", "--help"):
            print(__doc__)
            sys.exit(0)
        else:
            print(f"Command tidak dikenal: {cmd}")
            print("Gunakan: selftest | status | health | performance | export | reset-drawdown | reset | help")
            sys.exit(1)

    run_once(cfg)


if __name__ == "__main__":
    main()
