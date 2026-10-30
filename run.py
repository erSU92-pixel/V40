"""
APEX PREDATOR v6.1 — Penggerak
Launcher untuk engine `apex.py`. Auto-load .env, cek dep, CLI.
"""

import argparse
import logging
import os
import signal as _signal
import sys
import time


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception as e:
        print(f"[WARN] Gagal load .env: {e}")


load_dotenv()


def check_deps() -> None:
    missing = []
    for mod, pkg in [("requests", "requests"), ("numpy", "numpy"),
                     ("pandas", "pandas"), ("websocket", "websocket-client")]:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"[ERR] Module belum terpasang: {', '.join(missing)}")
        print(f"      Install sekali: pip install {' '.join(missing)}")
        sys.exit(1)


check_deps()


try:
    import apex
except ImportError as e:
    print(f"[ERR] Gagal import apex.py: {e}")
    print("      Pastikan apex.py ada di folder yang sama dengan run.py")
    sys.exit(1)


_STOP = {"requested": False}


def _install_signal_handlers() -> None:
    def handler(signum, frame):
        logging.info(f"Signal {signum} — graceful shutdown diminta")
        _STOP["requested"] = True
    for sig in (_signal.SIGTERM, _signal.SIGINT):
        try:
            _signal.signal(sig, handler)
        except Exception:
            pass


def setup_logging(log_file: str | None = None) -> None:
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [Apex-run] - %(message)s",
        handlers=handlers,
        force=True,
    )


def print_banner(cfg, mode: str) -> None:
    has_tg = bool(os.getenv("TELEGRAM_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))
    has_td = bool(os.getenv("TWELVEDATA_API_KEY"))
    print("╔═══════════════════════════════════════════════╗")
    print("║     ⚛️  APEX PREDATOR v6.1 — Penggerak        ║")
    print("╚═══════════════════════════════════════════════╝")
    print(f"  Mode            : {mode}")
    print(f"  Symbol          : {cfg.symbol}")
    print(f"  Balance / Risk  : ${cfg.account_balance_usd:,.0f} / {cfg.risk_percentage}%")
    print(f"  Max lot         : {cfg.max_lot}")
    print(f"  Score threshold : {cfg.score_threshold}")
    print(f"  Daily limit     : {cfg.max_daily_signals}")
    print(f"  Telegram        : {'✓ set' if has_tg else '✗ tidak diset'}")
    print(f"  TwelveData      : {'✓ set' if has_td else '✗ tidak diset (fallback off)'}")
    print(f"  Dry-run         : {'YA' if cfg.dry_run else 'tidak'}")
    print()


def cmd_once(cfg) -> int:
    try:
        apex.run_once(cfg)
        return 0
    except Exception as e:
        logging.exception(f"Error: {e}")
        apex.alert_error(f"run_once: {e}", cfg)
        return 1


def cmd_loop(cfg, interval: int) -> int:
    logging.info(f"Loop mode: setiap {interval}s. Ctrl+C untuk stop.")
    cycle = 0
    while not _STOP["requested"]:
        cycle += 1
        logging.info(f"── Loop cycle #{cycle} ──")
        try:
            apex.run_once(cfg)
        except Exception as e:
            logging.exception(f"Error di cycle #{cycle}: {e}")
            apex.alert_error(f"loop: {e}", cfg)
        for _ in range(interval):
            if _STOP["requested"]:
                break
            time.sleep(1)
    logging.info("Loop berhenti dengan bersih.")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="APEX PREDATOR v6.1 — Penggerak",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Contoh:
  python run.py                          # sekali jalan
  python run.py --dry-run                # sekali jalan tanpa Telegram
  python run.py --loop 300               # loop tiap 5 menit
  python run.py --status                 # statistik
  python run.py --reset                  # hapus state
        """,
    )
    p.add_argument("--once", action="store_true", help="Sekali jalan (default)")
    p.add_argument("--loop", type=int, metavar="SEC",
                   help="Loop setiap SEC detik (misal 300 = 5 menit)")
    p.add_argument("--dry-run", action="store_true", help="Tidak kirim Telegram")
    p.add_argument("--status", action="store_true", help="Tampilkan statistik")
    p.add_argument("--reset", action="store_true", help="Hapus state file")
    p.add_argument("--log", type=str, metavar="FILE",
                   help="Simpan log ke file")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    _install_signal_handlers()
    setup_logging(args.log)

    cfg = apex.Config.from_env()
    if args.dry_run:
        cfg.dry_run = True

    if args.status:
        apex.cmd_status(cfg)
        return 0
    if args.reset:
        apex.cmd_reset(cfg)
        return 0

    if args.loop:
        print_banner(cfg, f"LOOP setiap {args.loop}s")
        return cmd_loop(cfg, args.loop)

    print_banner(cfg, "ONCE")
    return cmd_once(cfg)


if __name__ == "__main__":
    sys.exit(main())
