"""One-off backfill: fetch per-city history and merge missing alerts into daily files."""
import asyncio
from pipeline import (
    load_cities, fetch_all_cities, partition_by_day,
    load_day_local, load_day_s3, save_day,
)

async def main():
    cities = load_cities()
    print(f"Fetching history for {len(cities)} cities...")
    all_alerts = await fetch_all_cities(cities)

    by_day = partition_by_day(all_alerts)
    for date_str in sorted(by_day):
        new_day = by_day[date_str]
        existing = load_day_local(date_str) or load_day_s3(date_str)
        by_rid = {a["rid"]: a for a in existing}
        new_count = 0
        for a in new_day:
            if a["rid"] not in by_rid:
                by_rid[a["rid"]] = a
                new_count += 1
        if new_count > 0:
            save_day(date_str, list(by_rid.values()))
            print(f"  {date_str}: added {new_count} new alerts (total {len(by_rid)})")
        else:
            print(f"  {date_str}: no new alerts")

    print("Backfill complete.")

if __name__ == "__main__":
    asyncio.run(main())
