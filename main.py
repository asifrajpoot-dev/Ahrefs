import os
import csv
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# --- CONFIGURATION ---
# Reads the comma-separated keys from the secret
API_KEYS = [k.strip() for k in os.environ.get("AHREFS_API_KEYS", "").split(",") if k.strip()]
INPUT_FILE = "links.txt"
OUTPUT_FILE = "dr_results.csv"
DR_VERSION = "dr3" 
RATE_LIMIT_DELAY = 1.1  # ~54 requests/min per key, safely under the 60/min limit

def load_processed_targets():
    """Reads the output CSV to find which targets have already been processed."""
    processed = set()
    if Path(OUTPUT_FILE).exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                if row:
                    processed.add(row[0])
    return processed

def check_dr(target, api_key):
    """Worker function to check DR for a single target using a specific API key."""
    url = "https://api.ahrefs.com/v3/public/domain-rating-free"
    params = {"target": target, "version": DR_VERSION, "output": "json"}
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            dr = data.get("domain_rating", {}).get("domain_rating")
            return (target, dr, "")
        elif resp.status_code == 429:
            print(f"Rate limited on {target}. Backing off for 60s...")
            time.sleep(60)
            # Retry once
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                dr = data.get("domain_rating", {}).get("domain_rating")
                return (target, dr, "")
            else:
                return (target, "", f"HTTP {resp.status_code} (retry)")
        else:
            return (target, "", f"HTTP {resp.status_code}")
    except Exception as e:
        return (target, "", str(e))

def main():
    if not API_KEYS:
        print("Error: AHREFS_API_KEYS secret is not set.")
        return

    # Load all targets
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        all_targets = [line.strip() for line in f if line.strip()]

    # Determine which targets still need processing
    processed_targets = load_processed_targets()
    targets_to_process = [t for t in all_targets if t not in processed_targets]

    print(f"Total targets: {len(all_targets)}")
    print(f"Already processed: {len(processed_targets)}")
    print(f"Remaining to process: {len(targets_to_process)}")
    print(f"Using {len(API_KEYS)} API keys in parallel.")

    if not targets_to_process:
        print("All targets have been processed. Nothing to do.")
        return

    # Open CSV in append mode
    file_exists = Path(OUTPUT_FILE).exists()
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["target", "domain_rating", "error"])

        # Distribute targets across workers
        with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
            futures = []
            # Simple round-robin distribution: assign each target to a key
            for i, target in enumerate(targets_to_process):
                api_key = API_KEYS[i % len(API_KEYS)]
                futures.append(executor.submit(check_dr, target, api_key))
            
            # Process results as they complete
            for future in as_completed(futures):
                target, dr, error = future.result()
                writer.writerow([target, dr, error])
                # Flush and sync to ensure data is written even if the process is killed
                csvfile.flush()
                os.fsync(csvfile.fileno())

    print(f"Batch complete. Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
