import os
import csv
import time
import requests
from pathlib import Path

# --- CONFIGURATION ---
API_KEYS = [k.strip() for k in os.environ.get("AHREFS_API_KEYS", "").split(",") if k.strip()]
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", "0"))
SHARD_COUNT = int(os.environ.get("SHARD_COUNT", "1"))
INPUT_FILE = "links.txt"
OUTPUT_FILE = f"results/shard_{SHARD_INDEX}.csv"
DR_VERSION = "dr3"
RATE_LIMIT_DELAY = 1.1   # ~54 requests/min — safely under the 60/min limit

if SHARD_INDEX >= len(API_KEYS):
    print(f"Shard {SHARD_INDEX} has no assigned API key. Exiting.")
    raise SystemExit(0)

API_KEY = API_KEYS[SHARD_INDEX]


def load_processed():
    """Return the set of targets already saved in this shard's CSV."""
    done = set()
    if Path(OUTPUT_FILE).exists():
        with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            next(r, None)  # skip header
            for row in r:
                if row:
                    done.add(row[0])
    return done


def check_dr(target):
    url = "https://api.ahrefs.com/v3/public/domain-rating-free"
    params = {"target": target, "version": DR_VERSION, "output": "json"}
    headers = {"Authorization": f"Bearer {API_KEY}"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        if r.status_code == 200:
            dr = r.json().get("domain_rating", {}).get("domain_rating")
            return (target, dr, "")
        if r.status_code == 429:
            print("Rate limited — backing off 65s...")
            time.sleep(65)
            r = requests.get(url, params=params, headers=headers, timeout=20)
            if r.status_code == 200:
                dr = r.json().get("domain_rating", {}).get("domain_rating")
                return (target, dr, "")
        return (target, "", f"HTTP {r.status_code}")
    except Exception as e:
        return (target, "", str(e))


def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        all_targets = [l.strip() for l in f if l.strip()]

    # Deterministically assign this shard every Nth line — no overlap with other shards
    my_targets = [t for i, t in enumerate(all_targets) if i % SHARD_COUNT == SHARD_INDEX]

    done = load_processed()
    todo = [t for t in my_targets if t not in done]

    print(f"Shard {SHARD_INDEX}/{SHARD_COUNT - 1}: assigned={len(my_targets)} done={len(done)} todo={len(todo)}")
    if not todo:
        print("Nothing to do.")
        return

    Path("results").mkdir(exist_ok=True)
    file_exists = Path(OUTPUT_FILE).exists()

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["target", "domain_rating", "error"])
        for i, t in enumerate(todo, 1):
            w.writerow(check_dr(t))
            f.flush()
            os.fsync(f.fileno())
            if i % 100 == 0:
                print(f"Shard {SHARD_INDEX}: {i}/{len(todo)}")
            time.sleep(RATE_LIMIT_DELAY)


if __name__ == "__main__":
    main()
