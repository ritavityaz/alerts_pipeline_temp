import signal
import time
from datetime import datetime

from .config import PIPELINE_RUN_INTERVAL_SECONDS
from .orchestrator import run_pipeline

stop = False


def handle_signal(signum, frame):
    global stop
    print(f"\nReceived signal {signum}, shutting down...")
    stop = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

while not stop:
    try:
        run_pipeline()
    except Exception as e:
        print(f"[{datetime.now()}] Pipeline error: {e}")

    if stop:
        break

    print(f"Sleeping {PIPELINE_RUN_INTERVAL_SECONDS}s until next run...")
    for _ in range(PIPELINE_RUN_INTERVAL_SECONDS):
        if stop:
            break
        time.sleep(1)

print("Shutdown complete.")
