import os

AIOSTOMP_ENABLE_STATS = bool(os.environ.get("AIOSTOMP_ENABLE_STATS", False))
AIOSTOMP_STATS_INTERVAL = int(os.environ.get("AIOSTOMP_STATS_INTERVAL", 10))