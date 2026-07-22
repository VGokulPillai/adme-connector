# Databricks notebook source
# MAGIC %md
# MAGIC # Insert 1000 Records into ADME + Trigger Pipeline

# COMMAND ----------

ADME_BASE_URL = "https://admesbxscusins1.energy.azure.com"
DATA_PARTITION = "opendes"
ADME_API_CLIENT_ID = "e37a6c70-7cbc-4593-80fc-01c1f20203f7"

WELLBORE_COUNT = 500
RESERVOIR_COUNT = 300
SAMPLE_COUNT = 200
TOTAL = WELLBORE_COUNT + RESERVOIR_COUNT + SAMPLE_COUNT

BATCH_SIZE = 50
MAX_RETRIES = 5

print(f"Plan: {TOTAL:,} records ({WELLBORE_COUNT} Wellbore, {RESERVOIR_COUNT} Reservoir, {SAMPLE_COUNT} Sample)")

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

FORMATIONS = ["Permian Basin","Eagle Ford","Bakken","Marcellus","Wolfcamp","Spraberry","Bone Spring","Delaware Basin","Haynesville","Utica"]
STATUSES = ["Active","Drilling","Completing","Producing","Plugged and Abandoned","Suspended","Testing","Shut-in"]
RESERVOIR_TYPES = ["Sandstone","Carbonate","Shale","Tight Sand","Chalk","Dolomite","Limestone","Conglomerate"]
TOOL_KINDS = ["Rotary Sidewall Coring","Percussion Sidewall Coring","Conventional Core","Wireline Core","Core Plug","Drill Cutting","Fluid Sample","Gas Sample"]
OPERATORS = ["Devon Energy","Pioneer Natural","ConocoPhillips","EOG Resources","Diamondback Energy","Continental Resources","Marathon Oil","Chevron"]

ts = time.strftime("%Y%m%d-%H%M%S")

def make_base(kind):
    return {
        "kind": kind,
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {"legaltags": [LEGAL_TAG], "otherRelevantDataCountries": ["US"], "status": "compliant"},
    }

records = {"Wellbore": [], "Reservoir": [], "Sample": []}

for i in range(WELLBORE_COUNT):
    rec = make_base("osdu:wks:master-data--Wellbore:1.0.0")
    rec["data"] = {
        "FacilityName": f"{random.choice(OPERATORS)} {random.choice(FORMATIONS)} #{i+1}",
        "FacilityID": f"WB-1k-{i+1:05d}-{ts}",
        "FacilityTypeID": "Wellbore",
        "WellID": f"{DATA_PARTITION}:master-data--Well:1k-{i+1:05d}-{ts}",
        "StatusSummary": random.choice(STATUSES),
        "TargetFormation": random.choice(FORMATIONS),
    }
    records["Wellbore"].append(rec)

for i in range(RESERVOIR_COUNT):
    rec = make_base("osdu:wks:master-data--Reservoir:1.0.0")
    rec["data"] = {
        "ReservoirName": f"{random.choice(FORMATIONS)} Unit {i+1}",
        "ReservoirID": f"RES-1k-{i+1:05d}-{ts}",
        "ReservoirType": random.choice(RESERVOIR_TYPES),
        "ReservoirDescription": f"Reservoir {i+1} in {random.choice(FORMATIONS)}",
    }
    records["Reservoir"].append(rec)

for i in range(SAMPLE_COUNT):
    rec = make_base("osdu:wks:master-data--Sample:1.0.0")
    depth = round(random.uniform(1000, 12000), 1)
    rec["data"] = {
        "SampleAcquisition": {
            "SampleAcquisitionJobID": f"SAJ-1k-{i+1:05d}-{ts}",
            "SampleAcquisitionTypeID": random.choice(TOOL_KINDS),
            "AcquisitionStartDate": "2026-07-08T00:00:00.000Z",
            "SampleAcquisitionDetail": {
                "WellboreID": f"{DATA_PARTITION}:master-data--Wellbore:1k-{i+1:05d}-{ts}",
                "ToolKind": random.choice(TOOL_KINDS),
                "RunNumber": str(random.randint(1, 10)),
                "TopDepth": depth,
                "BaseDepth": round(depth + random.uniform(5, 150), 1),
            },
        },
    }
    records["Sample"].append(rec)

print(f"Generated {TOTAL} records")

# COMMAND ----------

url = f"{ADME_BASE_URL}/api/storage/v2/records"
inserted = 0
failed = 0

for domain, recs in records.items():
    batches = [recs[i:i+BATCH_SIZE] for i in range(0, len(recs), BATCH_SIZE)]
    for batch in batches:
        for attempt in range(1, MAX_RETRIES + 1):
            resp = requests.put(url, headers=HEADERS, json=batch, timeout=120)
            if resp.status_code in (200, 201):
                inserted += len(resp.json().get("recordIds", []))
                break
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            failed += len(batch)
            break
        else:
            failed += len(batch)
    print(f"  {domain}: done")

print(f"\nInserted: {inserted}/{TOTAL} | Failed: {failed}")

# COMMAND ----------

dbutils.notebook.exit(f"DONE: {inserted}/{TOTAL} inserted | failed: {failed}")
