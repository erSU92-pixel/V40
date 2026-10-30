import sys
import bot

def main():
    cfg = bot.Config.from_env()
    bot.setup_logging(cfg)
    
    cmd = sys.argv[1].lower() if len(sys.argv) > 1 else "run"
    
    if cmd in ("run", "start"):
        bot.run_once(cfg)
    elif cmd == "selftest":
        bot.cmd_selftest(cfg)
    elif cmd == "status":
        bot.cmd_status(cfg)
    else:
        print(f"Unknown command: {cmd}")

if __name__ == "__main__":
    main()
