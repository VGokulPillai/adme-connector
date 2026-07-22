# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Insert 1M Records into ADME — Benchmark v2
# MAGIC
# MAGIC **Config:** THREADS=8, BATCH_SIZE=50, MAX_RETRIES=5, CHUNK_SIZE=100K
# MAGIC
# MAGIC Inserts 1,000,000 synthetic OSDU records with retry logic and chunked processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

ADME_BASE_URL = "https://admesbxscusins1.energy.azure.com"
DATA_PARTITION = "opendes"
ADME_API_CLIENT_ID = "e37a6c70-7cbc-4593-80fc-01c1f20203f7"

WELLBORE_COUNT = 500_000
RESERVOIR_COUNT = 300_000
SAMPLE_COUNT = 200_000
TOTAL = WELLBORE_COUNT + RESERVOIR_COUNT + SAMPLE_COUNT

THREADS = 8
BATCH_SIZE = 50
MAX_RETRIES = 5
CHUNK_SIZE = 100_000

DBU_PER_HOUR = 2.0
DBU_PRICE = 0.55

print(f"Plan: {TOTAL:,} records")
print(f"Threads: {THREADS}, Batch size: {BATCH_SIZE}, Max retries: {MAX_RETRIES}, Chunk size: {CHUNK_SIZE:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authenticate

# COMMAND ----------

import time, requests
from azure.identity import ManagedIdentityCredential

credential = ManagedIdentityCredential()
token_obj = credential.get_token(f"{ADME_API_CLIENT_ID}/.default")
adme_token = token_obj.token

HEADERS = {
    "Authorization": f"Bearer {adme_token}",
    "data-partition-id": DATA_PARTITION,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

legal_resp = requests.get(f"{ADME_BASE_URL}/api/legal/v1/legaltags", headers=HEADERS, timeout=30)
tags = legal_resp.json().get("legalTags", []) if legal_resp.status_code == 200 else []
LEGAL_TAG = tags[0]["name"] if tags else f"{DATA_PARTITION}-public-usa-dataset"

ACL_OWNERS = f"data.default.owners@{DATA_PARTITION}.dataservices.energy"
ACL_VIEWERS = f"data.default.viewers@{DATA_PARTITION}.dataservices.energy"

print(f"Authenticated (token {len(adme_token)} chars)")
print(f"Legal tag: {LEGAL_TAG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate 1M Records

# COMMAND ----------

import random

FORMATIONS = [
    "Permian Basin", "Eagle Ford", "Bakken", "Marcellus", "Wolfcamp",
    "Spraberry", "Bone Spring", "Delaware Basin", "Haynesville", "Utica",
    "Barnett", "Woodford", "Niobrara", "Monterey", "Tuscaloosa Marine Shale",
    "Midland Basin", "DJ Basin", "Powder River", "Williston", "Appalachian",
    "Anadarko", "Arkoma", "San Juan", "Piceance", "Green River",
]
STATUSES = [
    "Active", "Drilling", "Completing", "Producing", "Plugged and Abandoned",
    "Suspended", "Testing", "Shut-in", "Abandoned", "Temporarily Abandoned",
]
RESERVOIR_TYPES = [
    "Sandstone", "Carbonate", "Shale", "Tight Sand", "Chalk",
    "Dolomite", "Limestone", "Conglomerate", "Fractured Basement", "Volcanic",
]
TOOL_KINDS = [
    "Rotary Sidewall Coring", "Percussion Sidewall Coring", "Conventional Core",
    "Wireline Core", "Core Plug", "Drill Cutting", "Fluid Sample",
    "Gas Sample", "Water Sample", "Oil Sample",
]
OPERATORS = [
    "Devon Energy", "Pioneer Natural", "ConocoPhillips", "EOG Resources",
    "Diamondback Energy", "Continental Resources", "Marathon Oil",
    "Apache Corp", "Cimarex Energy", "Parsley Energy", "Hess Corp",
    "Chevron", "ExxonMobil", "Occidental", "APA Corporation",
]

ts = time.strftime("%Y%m%d-%H%M%S")

def make_base(kind):
    return {
        "kind": kind,
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {"legaltags": [LEGAL_TAG], "otherRelevantDataCountries": ["US"], "status": "compliant"},
    }

gen_start = time.time()

wellbore_records = []
for i in range(WELLBORE_COUNT):
    rec = make_base("osdu:wks:master-data--Wellbore:1.0.0")
    rec["data"] = {
        "FacilityName": f"{random.choice(OPERATORS)} {random.choice(FORMATIONS)} #{i+1}",
        "FacilityID": f"WB-v2-{i+1:07d}-{ts}",
        "FacilityTypeID": "Wellbore",
        "WellID": f"{DATA_PARTITION}:master-data--Well:v2-{i+1:07d}-{ts}",
        "StatusSummary": random.choice(STATUSES),
        "TargetFormation": random.choice(FORMATIONS),
    }
    wellbore_records.append(rec)

reservoir_records = []
for i in range(RESERVOIR_COUNT):
    rec = make_base("osdu:wks:master-data--Reservoir:1.0.0")
    rec["data"] = {
        "ReservoirName": f"{random.choice(FORMATIONS)} Unit {i+1}",
        "ReservoirID": f"RES-v2-{i+1:07d}-{ts}",
        "ReservoirType": random.choice(RESERVOIR_TYPES),
        "ReservoirDescription": f"Reservoir {i+1} in {random.choice(FORMATIONS)}",
    }
    reservoir_records.append(rec)

sample_records = []
for i in range(SAMPLE_COUNT):
    rec = make_base("osdu:wks:master-data--Sample:1.0.0")
    depth = round(random.uniform(1000, 12000), 1)
    rec["data"] = {
        "SampleAcquisition": {
            "SampleAcquisitionJobID": f"SAJ-v2-{i+1:07d}-{ts}",
            "SampleAcquisitionTypeID": random.choice(TOOL_KINDS),
            "AcquisitionStartDate": "2026-06-06T00:00:00.000Z",
            "SampleAcquisitionDetail": {
                "WellboreID": f"{DATA_PARTITION}:master-data--Wellbore:v2-{i+1:07d}-{ts}",
                "ToolKind": random.choice(TOOL_KINDS),
                "RunNumber": str(random.randint(1, 10)),
                "TopDepth": depth,
                "BaseDepth": round(depth + random.uniform(5, 150), 1),
            },
        },
    }
    sample_records.append(rec)

gen_elapsed = time.time() - gen_start
print(f"Generated {TOTAL:,} records in {gen_elapsed:.1f}s")
print(f"  Wellbore:       {len(wellbore_records):>10,}")
print(f"  Reservoir:      {len(reservoir_records):>10,}")
print(f"  Rock_and_Fluid: {len(sample_records):>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert into ADME (chunked, with retries)

# COMMAND ----------

import threading, json as _json
from concurrent.futures import ThreadPoolExecutor, as_completed

lock = threading.Lock()
stats = {
    "inserted": 0, "failed": 0, "retried": 0,
    "batches_done": 0, "batches_total": 0, "bytes_sent": 0,
}
batch_latencies = []

url = f"{ADME_BASE_URL}/api/storage/v2/records"

token_lock = threading.Lock()
token_state = {"token": adme_token, "expires_at": time.time() + 3000}

def get_headers():
    with token_lock:
        if time.time() > token_state["expires_at"] - 120:
            new_tok = credential.get_token(f"{ADME_API_CLIENT_ID}/.default")
            token_state["token"] = new_tok.token
            token_state["expires_at"] = time.time() + 3000
            print(f"  [Token refreshed at {time.strftime('%H:%M:%S')}]")
        return {
            "Authorization": f"Bearer {token_state['token']}",
            "data-partition-id": DATA_PARTITION,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

def insert_batch_with_retry(batch):
    body = _json.dumps(batch)
    body_bytes = len(body.encode("utf-8"))

    for attempt in range(1, MAX_RETRIES + 1):
        hdrs = get_headers()
        t0 = time.time()
        try:
            resp = requests.put(url, headers=hdrs, data=body, timeout=180)
            elapsed = time.time() - t0
        except Exception:
            elapsed = time.time() - t0
            if attempt < MAX_RETRIES:
                with lock:
                    stats["retried"] += len(batch)
                time.sleep(2 ** attempt)
                continue
            with lock:
                stats["failed"] += len(batch)
                stats["batches_done"] += 1
                stats["bytes_sent"] += body_bytes
                batch_latencies.append(elapsed)
            return 0

        if resp.status_code in (200, 201):
            count = len(resp.json().get("recordIds", []))
            with lock:
                stats["inserted"] += count
                stats["batches_done"] += 1
                stats["bytes_sent"] += body_bytes
                batch_latencies.append(elapsed)
            return count

        if resp.status_code == 429 or resp.status_code >= 500:
            with lock:
                stats["retried"] += len(batch)
            time.sleep(2 ** attempt)
            continue

        with lock:
            stats["failed"] += len(batch)
            stats["batches_done"] += 1
            stats["bytes_sent"] += body_bytes
            batch_latencies.append(elapsed)
        return 0

    with lock:
        stats["failed"] += len(batch)
        stats["batches_done"] += 1
        stats["bytes_sent"] += body_bytes
    return 0

def process_chunk(label, chunk_records, chunk_num, total_chunks):
    batches = [chunk_records[i:i+BATCH_SIZE] for i in range(0, len(chunk_records), BATCH_SIZE)]
    with lock:
        stats["batches_total"] += len(batches)

    chunk_start = time.time()
    chunk_inserted = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(insert_batch_with_retry, b) for b in batches]
        for f in as_completed(futures):
            chunk_inserted += f.result()
            done = stats["batches_done"]
            if done % 200 == 0:
                elapsed = time.time() - overall_start
                rate = stats["inserted"] / elapsed if elapsed > 0 else 0
                print(f"  [{time.strftime('%H:%M:%S')}] {stats['inserted']:,} inserted | {done}/{stats['batches_total']} batches | {rate:.0f} rec/s | retried: {stats['retried']:,} | failed: {stats['failed']:,}")

    chunk_elapsed = time.time() - chunk_start
    rate = chunk_inserted / chunk_elapsed if chunk_elapsed > 0 else 0
    print(f"  {label} chunk {chunk_num}/{total_chunks}: {chunk_inserted:,}/{len(chunk_records):,} in {chunk_elapsed:.1f}s ({rate:.0f} rec/s)")

def run_domain(label, records):
    chunks = [records[i:i+CHUNK_SIZE] for i in range(0, len(records), CHUNK_SIZE)]
    total_chunks = len(chunks)
    domain_start = time.time()
    domain_before = stats["inserted"]

    for idx, chunk in enumerate(chunks, 1):
        process_chunk(label, chunk, idx, total_chunks)

    domain_elapsed = time.time() - domain_start
    domain_inserted = stats["inserted"] - domain_before
    rate = domain_inserted / domain_elapsed if domain_elapsed > 0 else 0
    print(f"  >>> {label} DONE: {domain_inserted:,}/{len(records):,} in {domain_elapsed:.1f}s ({rate:.0f} rec/s)")
    return domain_elapsed

print(f"Starting insertion of {TOTAL:,} records at {time.strftime('%H:%M:%S')}...")
print(f"Threads: {THREADS}, Batch size: {BATCH_SIZE}, Max retries: {MAX_RETRIES}, Chunk size: {CHUNK_SIZE:,}")
print()

overall_start = time.time()

wb_time = run_domain("Wellbore", wellbore_records)
res_time = run_domain("Reservoir", reservoir_records)
rf_time = run_domain("Rock_and_Fluid", sample_records)

overall_elapsed = time.time() - overall_start

# COMMAND ----------

# MAGIC %md
# MAGIC ## Throughput & Cost Report

# COMMAND ----------

print("=" * 70)
print("  ADME INGESTION BENCHMARK v2 — 1M RECORDS")
print("  Config: THREADS=8, BATCH_SIZE=50, MAX_RETRIES=5, CHUNK_SIZE=100K")
print("=" * 70)
print()
print(f"  Total records attempted:  {TOTAL:>12,}")
print(f"  Total records inserted:   {stats['inserted']:>12,}")
print(f"  Total records failed:     {stats['failed']:>12,}")
print(f"  Total records retried:    {stats['retried']:>12,}")
print(f"  Total batches:            {stats['batches_done']:>12,}")
print(f"  Data sent:                {stats['bytes_sent']/1024/1024:>11.1f} MB")
print()
print("  THROUGHPUT")
print("  " + "-" * 50)
print(f"  Overall throughput:       {stats['inserted']/overall_elapsed:>11.0f} records/sec")
print(f"  Overall time:             {overall_elapsed:>11.1f}s ({overall_elapsed/60:.1f} min)")
if batch_latencies:
    sorted_lat = sorted(batch_latencies)
    print(f"  Avg batch latency:        {sum(sorted_lat)/len(sorted_lat):>11.2f}s")
    print(f"  P50 batch latency:        {sorted_lat[len(sorted_lat)//2]:>11.2f}s")
    print(f"  P95 batch latency:        {sorted_lat[int(len(sorted_lat)*0.95)]:>11.2f}s")
    print(f"  P99 batch latency:        {sorted_lat[int(len(sorted_lat)*0.99)]:>11.2f}s")
    print(f"  Min batch latency:        {min(sorted_lat):>11.2f}s")
    print(f"  Max batch latency:        {max(sorted_lat):>11.2f}s")
print(f"  Threads:                  {THREADS:>11}")
print(f"  Batch size:               {BATCH_SIZE:>11}")
print(f"  Max retries:              {MAX_RETRIES:>11}")
print(f"  Chunk size:               {CHUNK_SIZE:>11,}")
print()
print("  PER-DOMAIN BREAKDOWN")
print("  " + "-" * 50)
print(f"  Wellbore ({WELLBORE_COUNT:,}):    {wb_time:>8.1f}s  ({WELLBORE_COUNT/wb_time:.0f} rec/s)")
print(f"  Reservoir ({RESERVOIR_COUNT:,}):   {res_time:>8.1f}s  ({RESERVOIR_COUNT/res_time:.0f} rec/s)")
print(f"  Rock_and_Fluid ({SAMPLE_COUNT:,}): {rf_time:>8.1f}s  ({SAMPLE_COUNT/rf_time:.0f} rec/s)")
print()
print("  ESTIMATED COMPUTE COST")
print("  " + "-" * 50)
hours = overall_elapsed / 3600
dbu_consumed = hours * DBU_PER_HOUR
cost = dbu_consumed * DBU_PRICE
print(f"  Cluster runtime:          {overall_elapsed:>11.0f}s ({hours*60:.1f} min)")
print(f"  Node type:                Standard_D4ads_v6 (single node)")
print(f"  DBU rate:                 {DBU_PER_HOUR} DBU/hr")
print(f"  DBU consumed:             {dbu_consumed:>11.3f} DBU")
print(f"  Estimated cost:           ${cost:>10.4f}")
print(f"  Cost per 1K records:      ${cost/max(stats['inserted'],1)*1000:>10.6f}")
print()
print("  COMPARISON vs v1 (THREADS=16, BATCH=100, no retries)")
print("  " + "-" * 50)
print(f"  v1: 465,700/1M inserted in ~19.5 min (399 rec/s) — 53% loss")
print(f"  v2: {stats['inserted']:,}/1M inserted in {overall_elapsed/60:.1f} min ({stats['inserted']/overall_elapsed:.0f} rec/s) — {stats['failed']/TOTAL*100:.1f}% loss")
print()
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save fresh token

# COMMAND ----------

fresh_token = credential.get_token(f"{ADME_API_CLIENT_ID}/.default").token
dbutils.fs.put("/tmp/adme_fresh_token.txt", fresh_token, True)
print("Fresh ADME token saved to DBFS")

# COMMAND ----------

dbutils.notebook.exit(f"DONE: {stats['inserted']:,}/{TOTAL:,} inserted in {overall_elapsed:.0f}s ({stats['inserted']/overall_elapsed:.0f} rec/s) | failed: {stats['failed']:,} | retried: {stats['retried']:,}")
