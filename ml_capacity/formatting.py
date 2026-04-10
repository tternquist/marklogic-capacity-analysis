"""ANSI color formatting and display helpers."""

YELLOW = "\033[33m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

BAR_WIDTH = 30


def color(text, c):
    return f"{c}{text}{RESET}"


def header(title):
    width = 62
    print()
    print(color("=" * width, DIM))
    print(color(f"  {title}", BOLD + CYAN))
    print(color("=" * width, DIM))


def sub_header(title):
    print()
    print(color(f"  --- {title} ---", BOLD))


def kv(key, value, indent=4):
    pad = " " * indent
    print(f"{pad}{color(key + ':', DIM):.<48s} {value}")


def bar(pct, warn_threshold=70, crit_threshold=90):
    filled = int(round(pct / 100 * BAR_WIDTH))
    filled = max(0, min(BAR_WIDTH, filled))
    empty = BAR_WIDTH - filled
    if pct >= crit_threshold:
        c = RED
    elif pct >= warn_threshold:
        c = YELLOW
    else:
        c = GREEN
    return f"{c}{'█' * filled}{'░' * empty}{RESET} {pct:.1f}%"


def fmt_mb(mb):
    if mb is None:
        return "N/A"
    mb = float(mb)
    if mb >= 1024 * 1024:
        return f"{mb / (1024 * 1024):.2f} TB"
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.1f} MB"


def status_badge(ok, ok_text="OK", bad_text="WARNING"):
    if ok:
        return color(f"[{ok_text}]", GREEN)
    return color(f"[{bad_text}]", RED)
