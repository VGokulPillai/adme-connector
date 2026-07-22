# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Insert 1M Records into ADME — Speed & Cost Benchmark
# MAGIC
# MAGIC **What this does:**
# MAGIC - Generates 1,000,000 synthetic OSDU records (500K Wellbore, 300K Reservoir, 200K Sample)
# MAGIC - Inserts them into ADME via the Storage API using concurrent threads
# MAGIC - Reports throughput metrics (records/sec, batch latency, data volume)
# MAGIC - Estimates compute cost for the insertion
# MAGIC
# MAGIC **After this notebook completes**, trigger the DLT pipeline to pull data from ADME into Databricks.
# MAGIC See the last cell for instructions.

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

BATCH_SIZE = 100
MAX_WORKERS = 16  # concurrent threads for API calls

# Cost assumptions (Azure Databricks, Standard_D4ads_v6 single node)
DBU_PER_HOUR = 2.0   # DBU rate for this VM
DBU_PRICE = 0.55      # $/DBU for Jobs compute (pay-as-you-go)

print(f"Plan: {TOTAL:,} records ({WELLBORE_COUNT:,} Wellbore, {RESERVOIR_COUNT:,} Reservoir, {SAMPLE_COUNT:,} Sample)")
print(f"Batch size: {BATCH_SIZE}, Concurrency: {MAX_WORKERS} threads")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authenticate to ADME

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
        "FacilityID": f"WB-1M-{i+1:07d}-{ts}",
        "FacilityTypeID": "Wellbore",
        "WellID": f"{DATA_PARTITION}:master-data--Well:1m-{i+1:07d}-{ts}",
        "StatusSummary": random.choice(STATUSES),
        "TargetFormation": random.choice(FORMATIONS),
    }
    wellbore_records.append(rec)

reservoir_records = []
for i in range(RESERVOIR_COUNT):
    rec = make_base("osdu:wks:master-data--Reservoir:1.0.0")
    rec["data"] = {
        "ReservoirName": f"{random.choice(FORMATIONS)} Unit {i+1}",
        "ReservoirID": f"RES-1M-{i+1:07d}-{ts}",
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
            "SampleAcquisitionJobID": f"SAJ-1M-{i+1:07d}-{ts}",
            "SampleAcquisitionTypeID": random.choice(TOOL_KINDS),
            "AcquisitionStartDate": "2026-06-06T00:00:00.000Z",
            "SampleAcquisitionDetail": {
                "WellboreID": f"{DATA_PARTITION}:master-data--Wellbore:1m-{i+1:07d}-{ts}",
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
# MAGIC ## Insert into ADME (with throughput tracking)

# COMMAND ----------

import threading, json as _json
from concurrent.futures import ThreadPoolExecutor, as_completed

lock = threading.Lock()
stats = {"inserted": 0, "failed": 0, "batches_done": 0, "batches_total": 0, "bytes_sent": 0}
batch_latencies = []

url = f"{ADME_BASE_URL}/api/storage/v2/records"

# Token refresh: ADME tokens expire in ~1 hour; refresh proactively
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

def insert_batch(batch):
    body = _json.dumps(batch)
    body_bytes = len(body.encode("utf-8"))
    hdrs = get_headers()
    t0 = time.time()
    resp = requests.put(url, headers=hdrs, data=body, timeout=180)
    elapsed = time.time() - t0
    
    with lock:
        stats["batches_done"] += 1
        stats["bytes_sent"] += body_bytes
        if resp.status_code in (200, 201):
            count = len(resp.json().get("recordIds", []))
            stats["inserted"] += count
            batch_latencies.append(elapsed)
        else:
            stats["failed"] += len(batch)
            batch_latencies.append(elapsed)
    return resp.status_code

def run_domain(label, records):
    batches = [records[i:i+BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
    stats["batches_total"] += len(batches)
    
    domain_start = time.time()
    domain_inserted_before = stats["inserted"]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(insert_batch, b) for b in batches]
        failed_batches = 0
        for f in as_completed(futures):
            code = f.result()
            if code not in (200, 201):
                failed_batches += 1
            # Progress every 1000 batches
            done = stats["batches_done"]
            if done % 500 == 0:
                elapsed = time.time() - overall_start
                rate = stats["inserted"] / elapsed if elapsed > 0 else 0
                print(f"  Progress: {stats['inserted']:,} inserted, {done}/{stats['batches_total']} batches, {rate:.0f} rec/s")
    
    domain_elapsed = time.time() - domain_start
    domain_inserted = stats["inserted"] - domain_inserted_before
    rate = domain_inserted / domain_elapsed if domain_elapsed > 0 else 0
    print(f"  {label}: {domain_inserted:,}/{len(records):,} in {domain_elapsed:.1f}s ({rate:.0f} rec/s)")
    return domain_elapsed

print(f"Starting insertion of {TOTAL:,} records at {time.strftime('%H:%M:%S')}...")
print(f"Concurrency: {MAX_WORKERS} threads, Batch size: {BATCH_SIZE}")
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
print("  ADME INGESTION BENCHMARK — 1M RECORDS")
print("=" * 70)
print()
print(f"  Total records attempted:  {TOTAL:>12,}")
print(f"  Total records inserted:   {stats['inserted']:>12,}")
print(f"  Total records failed:     {stats['failed']:>12,}")
print(f"  Total batches:            {stats['batches_done']:>12,}")
print(f"  Data sent:                {stats['bytes_sent']/1024/1024:>11.1f} MB")
print()
print("  THROUGHPUT")
print("  " + "-" * 50)
print(f"  Overall throughput:       {stats['inserted']/overall_elapsed:>11.0f} records/sec")
print(f"  Overall time:             {overall_elapsed:>11.1f}s ({overall_elapsed/60:.1f} min)")
print(f"  Avg batch latency:        {sum(batch_latencies)/len(batch_latencies):>11.2f}s")
print(f"  P50 batch latency:        {sorted(batch_latencies)[len(batch_latencies)//2]:>11.2f}s")
print(f"  P95 batch latency:        {sorted(batch_latencies)[int(len(batch_latencies)*0.95)]:>11.2f}s")
print(f"  P99 batch latency:        {sorted(batch_latencies)[int(len(batch_latencies)*0.99)]:>11.2f}s")
print(f"  Min batch latency:        {min(batch_latencies):>11.2f}s")
print(f"  Max batch latency:        {max(batch_latencies):>11.2f}s")
print(f"  Concurrency:              {MAX_WORKERS:>11} threads")
print(f"  Batch size:               {BATCH_SIZE:>11} records")
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
print(f"  Cost per 1K records:      ${cost/TOTAL*1000:>10.6f}")
print()
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save token for pipeline trigger

# COMMAND ----------

fresh_token = credential.get_token(f"{ADME_API_CLIENT_ID}/.default").token
dbutils.fs.put("/tmp/adme_fresh_token.txt", fresh_token, True)
print("Fresh ADME token saved to DBFS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to trigger the pipeline (ADME → Databricks)
# MAGIC
# MAGIC After this notebook completes, run these steps to pull the data from ADME into Databricks:
# MAGIC
# MAGIC ### Option 1: From the Databricks UI
# MAGIC 1. Go to **Workflows → Delta Live Tables → `adme_pipeline`**
# MAGIC 2. Click **Start** → choose **Full refresh all tables**
# MAGIC 3. Wait for it to complete (~5-15 min for 1M records)
# MAGIC
# MAGIC ### Option 2: From a notebook cell
# MAGIC Uncomment and run the cell below.

# COMMAND ----------

# Uncomment the code below to trigger the pipeline automatically:

# import requests
# 
# PIPELINE_ID = "7ca53e2c-0071-4f24-a1ca-399b0e20a239"
# db_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# host = spark.conf.get("spark.databricks.workspaceUrl")
# 
# # Step 1: Refresh the UC connection token
# fresh_token = credential.get_token(f"{ADME_API_CLIENT_ID}/.default").token
# conn_payload = {
#     "options": {
#         "sourceName": "adme",
#         "base_url": ADME_BASE_URL,
#         "data_partition_id": DATA_PARTITION,
#         "auth_mode": "static_token",
#         "tenant_id": "72f988bf-86f1-41af-91ab-2d7cd011db47",
#         "adme_api_client_id": ADME_API_CLIENT_ID,
#         "access_token": fresh_token,
#     }
# }
# resp = requests.patch(
#     f"https://{host}/api/2.1/unity-catalog/connections/adme_production",
#     headers={"Authorization": f"Bearer {db_token}", "Content-Type": "application/json"},
#     json=conn_payload, timeout=30,
# )
# print(f"Connection update: {resp.status_code}")
# 
# # Step 2: Trigger the pipeline
# resp = requests.post(
#     f"https://{host}/api/2.0/pipelines/{PIPELINE_ID}/updates",
#     headers={"Authorization": f"Bearer {db_token}", "Content-Type": "application/json"},
#     json={"full_refresh": True}, timeout=30,
# )
# print(f"Pipeline triggered: {resp.json().get('update_id', '')[:8]}")
# print("Monitor at: https://adb-4173618801742158.18.azuredatabricks.net/#joblist/pipelines/7ca53e2c-0071-4f24-a1ca-399b0e20a239")

# COMMAND ----------

dbutils.notebook.exit(f"DONE: {stats['inserted']:,} records inserted in {overall_elapsed:.0f}s ({stats['inserted']/overall_elapsed:.0f} rec/s)")
