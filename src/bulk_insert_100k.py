# Databricks notebook source
# MAGIC %md
# MAGIC # Insert 100K Records into ADME
# MAGIC Config: THREADS=8, BATCH_SIZE=50, MAX_RETRIES=5, CHUNK_SIZE=100K

# COMMAND ----------

ADME_BASE_URL = "https://admesbxscusins1.energy.azure.com"
DATA_PARTITION = "opendes"
ADME_API_CLIENT_ID = "e37a6c70-7cbc-4593-80fc-01c1f20203f7"

WELLBORE_COUNT = 50_000
RESERVOIR_COUNT = 30_000
SAMPLE_COUNT = 20_000
TOTAL = WELLBORE_COUNT + RESERVOIR_COUNT + SAMPLE_COUNT

THREADS = 8
BATCH_SIZE = 50
MAX_RETRIES = 5

DBU_PER_HOUR = 2.0
DBU_PRICE = 0.55

print(f"Plan: {TOTAL:,} records ({WELLBORE_COUNT:,} Wellbore, {RESERVOIR_COUNT:,} Reservoir, {SAMPLE_COUNT:,} Sample)")

# COMMAND ----------

import time, requests, random, threading, json as _json
from azure.identity import ManagedIdentityCredential
from concurrent.futures import ThreadPoolExecutor, as_completed

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

print(f"Authenticated | Legal tag: {LEGAL_TAG}")

# COMMAND ----------

FORMATIONS = ["Permian Basin","Eagle Ford","Bakken","Marcellus","Wolfcamp","Spraberry","Bone Spring","Delaware Basin","Haynesville","Utica","Barnett","Woodford","Niobrara","Monterey","Tuscaloosa Marine Shale","Midland Basin","DJ Basin","Powder River","Williston","Appalachian","Anadarko","Arkoma","San Juan","Piceance","Green River"]
STATUSES = ["Active","Drilling","Completing","Producing","Plugged and Abandoned","Suspended","Testing","Shut-in","Abandoned","Temporarily Abandoned"]
RESERVOIR_TYPES = ["Sandstone","Carbonate","Shale","Tight Sand","Chalk","Dolomite","Limestone","Conglomerate","Fractured Basement","Volcanic"]
TOOL_KINDS = ["Rotary Sidewall Coring","Percussion Sidewall Coring","Conventional Core","Wireline Core","Core Plug","Drill Cutting","Fluid Sample","Gas Sample","Water Sample","Oil Sample"]
OPERATORS = ["Devon Energy","Pioneer Natural","ConocoPhillips","EOG Resources","Diamondback Energy","Continental Resources","Marathon Oil","Apache Corp","Cimarex Energy","Parsley Energy","Hess Corp","Chevron","ExxonMobil","Occidental","APA Corporation"]

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
        "FacilityID": f"WB-100k-{i+1:06d}-{ts}",
        "FacilityTypeID": "Wellbore",
        "WellID": f"{DATA_PARTITION}:master-data--Well:100k-{i+1:06d}-{ts}",
        "StatusSummary": random.choice(STATUSES),
        "TargetFormation": random.choice(FORMATIONS),
    }
    wellbore_records.append(rec)

reservoir_records = []
for i in range(RESERVOIR_COUNT):
    rec = make_base("osdu:wks:master-data--Reservoir:1.0.0")
    rec["data"] = {
        "ReservoirName": f"{random.choice(FORMATIONS)} Unit {i+1}",
        "ReservoirID": f"RES-100k-{i+1:06d}-{ts}",
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
            "SampleAcquisitionJobID": f"SAJ-100k-{i+1:06d}-{ts}",
            "SampleAcquisitionTypeID": random.choice(TOOL_KINDS),
            "AcquisitionStartDate": "2026-07-15T00:00:00.000Z",
            "SampleAcquisitionDetail": {
                "WellboreID": f"{DATA_PARTITION}:master-data--Wellbore:100k-{i+1:06d}-{ts}",
                "ToolKind": random.choice(TOOL_KINDS),
                "RunNumber": str(random.randint(1, 10)),
                "TopDepth": depth,
                "BaseDepth": round(depth + random.uniform(5, 150), 1),
            },
        },
    }
    sample_records.append(rec)

print(f"Generated {TOTAL:,} records in {time.time()-gen_start:.1f}s")

# COMMAND ----------

lock = threading.Lock()
stats = {"inserted": 0, "failed": 0, "retried": 0, "batches_done": 0, "batches_total": 0, "bytes_sent": 0}
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
                with lock: stats["retried"] += len(batch)
                time.sleep(2 ** attempt)
                continue
            with lock:
                stats["failed"] += len(batch); stats["batches_done"] += 1
                stats["bytes_sent"] += body_bytes; batch_latencies.append(elapsed)
            return 0
        if resp.status_code in (200, 201):
            count = len(resp.json().get("recordIds", []))
            with lock:
                stats["inserted"] += count; stats["batches_done"] += 1
                stats["bytes_sent"] += body_bytes; batch_latencies.append(elapsed)
            return count
        if resp.status_code == 429 or resp.status_code >= 500:
            with lock: stats["retried"] += len(batch)
            time.sleep(2 ** attempt)
            continue
        with lock:
            stats["failed"] += len(batch); stats["batches_done"] += 1
            stats["bytes_sent"] += body_bytes; batch_latencies.append(elapsed)
        return 0
    with lock:
        stats["failed"] += len(batch); stats["batches_done"] += 1; stats["bytes_sent"] += body_bytes
    return 0

def run_domain(label, records):
    batches = [records[i:i+BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
    with lock: stats["batches_total"] += len(batches)
    domain_start = time.time()
    domain_before = stats["inserted"]
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(insert_batch_with_retry, b) for b in batches]
        for f in as_completed(futures):
            f.result()
            done = stats["batches_done"]
            if done % 100 == 0:
                elapsed = time.time() - overall_start
                rate = stats["inserted"] / elapsed if elapsed > 0 else 0
                print(f"  [{time.strftime('%H:%M:%S')}] {stats['inserted']:,} inserted | {done}/{stats['batches_total']} batches | {rate:.0f} rec/s")
    domain_elapsed = time.time() - domain_start
    domain_inserted = stats["inserted"] - domain_before
    rate = domain_inserted / domain_elapsed if domain_elapsed > 0 else 0
    print(f"  >>> {label} DONE: {domain_inserted:,}/{len(records):,} in {domain_elapsed:.1f}s ({rate:.0f} rec/s)")
    return domain_elapsed

print(f"Starting insertion at {time.strftime('%H:%M:%S')}...")
overall_start = time.time()
wb_time = run_domain("Wellbore", wellbore_records)
res_time = run_domain("Reservoir", reservoir_records)
rf_time = run_domain("Rock_and_Fluid", sample_records)
overall_elapsed = time.time() - overall_start

# COMMAND ----------

print("=" * 60)
print(f"  100K ADME INSERTION COMPLETE")
print("=" * 60)
print(f"  Inserted:    {stats['inserted']:>10,} / {TOTAL:,}")
print(f"  Failed:      {stats['failed']:>10,}")
print(f"  Retried:     {stats['retried']:>10,}")
print(f"  Throughput:  {stats['inserted']/overall_elapsed:>10.0f} rec/s")
print(f"  Duration:    {overall_elapsed:>10.1f}s ({overall_elapsed/60:.1f} min)")
if batch_latencies:
    s = sorted(batch_latencies)
    print(f"  P50 latency: {s[len(s)//2]:>10.2f}s")
    print(f"  P95 latency: {s[int(len(s)*0.95)]:>10.2f}s")
hours = overall_elapsed / 3600
cost = hours * DBU_PER_HOUR * DBU_PRICE
print(f"  Est. cost:   ${cost:>9.4f}")
print("=" * 60)

# COMMAND ----------

dbutils.notebook.exit(f"DONE: {stats['inserted']:,}/{TOTAL:,} in {overall_elapsed:.0f}s ({stats['inserted']/overall_elapsed:.0f} rec/s) | failed:{stats['failed']:,} retried:{stats['retried']:,}")
