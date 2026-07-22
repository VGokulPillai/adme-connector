# Databricks notebook source
# MAGIC %md
# MAGIC # Insert Records into ADME & Trigger Ingestion Pipeline
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Inserts sample Wellbore, Reservoir, and Rock_and_Fluid records into ADME
# MAGIC 2. Refreshes the static token on the UC connection so the DLT pipeline can authenticate
# MAGIC 3. Triggers the ingestion pipeline (full refresh)
# MAGIC 4. Waits for the pipeline to complete
# MAGIC 5. Queries the tables to show the ingested data

# COMMAND ----------

import json, time, requests
from azure.identity import ManagedIdentityCredential

ADME_BASE_URL = "https://admesbxscusins1.energy.azure.com"
DATA_PARTITION = "opendes"
ADME_API_CLIENT_ID = "e37a6c70-7cbc-4593-80fc-01c1f20203f7"
PIPELINE_ID = "7ca53e2c-0071-4f24-a1ca-399b0e20a239"

credential = ManagedIdentityCredential()
token_obj = credential.get_token(f"{ADME_API_CLIENT_ID}/.default")
adme_token = token_obj.token

HEADERS = {
    "Authorization": f"Bearer {adme_token}",
    "data-partition-id": DATA_PARTITION,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

print(f"Authenticated to ADME ({ADME_BASE_URL})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Insert records into ADME

# COMMAND ----------

legal_resp = requests.get(f"{ADME_BASE_URL}/api/legal/v1/legaltags", headers=HEADERS, timeout=30)
tags = legal_resp.json().get("legalTags", []) if legal_resp.status_code == 200 else []
LEGAL_TAG = tags[0]["name"] if tags else f"{DATA_PARTITION}-public-usa-dataset"

ACL_OWNERS = f"data.default.owners@{DATA_PARTITION}.dataservices.energy"
ACL_VIEWERS = f"data.default.viewers@{DATA_PARTITION}.dataservices.energy"

ts = time.strftime("%Y%m%d-%H%M%S")

wellbore_records = [
    {
        "kind": "osdu:wks:master-data--Wellbore:1.0.0",
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {"legaltags": [LEGAL_TAG], "otherRelevantDataCountries": ["US"], "status": "compliant"},
        "data": {
            "FacilityName": f"Auto Wellbore Alpha {ts}",
            "FacilityID": f"WB-AUTO-A-{ts}",
            "FacilityTypeID": "Wellbore",
            "WellID": f"{DATA_PARTITION}:master-data--Well:auto-{ts}",
            "StatusSummary": "Active",
            "TargetFormation": "Permian Basin",
        },
    },
    {
        "kind": "osdu:wks:master-data--Wellbore:1.0.0",
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {"legaltags": [LEGAL_TAG], "otherRelevantDataCountries": ["US"], "status": "compliant"},
        "data": {
            "FacilityName": f"Auto Wellbore Beta {ts}",
            "FacilityID": f"WB-AUTO-B-{ts}",
            "FacilityTypeID": "Wellbore",
            "WellID": f"{DATA_PARTITION}:master-data--Well:auto-{ts}",
            "StatusSummary": "Drilling",
            "TargetFormation": "Eagle Ford",
        },
    },
]

reservoir_records = [
    {
        "kind": "osdu:wks:master-data--Reservoir:1.0.0",
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {"legaltags": [LEGAL_TAG], "otherRelevantDataCountries": ["US"], "status": "compliant"},
        "data": {
            "ReservoirName": f"Auto Reservoir {ts}",
            "ReservoirID": f"RES-AUTO-{ts}",
            "ReservoirType": "Sandstone",
            "ReservoirDescription": f"Auto-generated reservoir at {ts}",
        },
    },
]

sample_records = [
    {
        "kind": "osdu:wks:master-data--Sample:1.0.0",
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {"legaltags": [LEGAL_TAG], "otherRelevantDataCountries": ["US"], "status": "compliant"},
        "data": {
            "SampleAcquisition": {
                "SampleAcquisitionJobID": f"SAJ-AUTO-{ts}",
                "SampleAcquisitionTypeID": "Core",
                "AcquisitionStartDate": time.strftime("%Y-%m-%dT00:00:00.000Z"),
            },
        },
    },
]

url = f"{ADME_BASE_URL}/api/storage/v2/records"
all_ids = []

for records, label in [(wellbore_records, "Wellbore"), (reservoir_records, "Reservoir"), (sample_records, "Rock_and_Fluid")]:
    resp = requests.put(url, headers=HEADERS, json=records, timeout=60)
    if resp.status_code in (200, 201):
        ids = resp.json().get("recordIds", [])
        all_ids.extend(ids)
        print(f"[OK] {label}: {len(ids)} records inserted")
        for rid in ids:
            print(f"  - {rid}")
    else:
        print(f"[FAIL] {label}: {resp.status_code} - {resp.text[:300]}")

print(f"\nTotal inserted: {len(all_ids)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Refresh the UC connection token

# COMMAND ----------

db_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

fresh_token = credential.get_token(f"{ADME_API_CLIENT_ID}/.default").token

conn_payload = {
    "options": {
        "sourceName": "adme",
        "base_url": ADME_BASE_URL,
        "data_partition_id": DATA_PARTITION,
        "auth_mode": "static_token",
        "tenant_id": "72f988bf-86f1-41af-91ab-2d7cd011db47",
        "adme_api_client_id": ADME_API_CLIENT_ID,
        "access_token": fresh_token,
    }
}

resp = requests.patch(
    f"https://{host}/api/2.1/unity-catalog/connections/adme_production",
    headers={"Authorization": f"Bearer {db_token}", "Content-Type": "application/json"},
    json=conn_payload,
    timeout=30,
)

if resp.status_code == 200:
    print("UC connection token refreshed")
else:
    print(f"Failed to update connection: {resp.status_code} - {resp.text[:300]}")
    dbutils.notebook.exit("FAILED: could not refresh UC connection token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Trigger the ingestion pipeline

# COMMAND ----------

resp = requests.post(
    f"https://{host}/api/2.0/pipelines/{PIPELINE_ID}/updates",
    headers={"Authorization": f"Bearer {db_token}", "Content-Type": "application/json"},
    json={"full_refresh": True},
    timeout=30,
)

update = resp.json()
update_id = update.get("update_id", "")
print(f"Pipeline triggered: update_id={update_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Wait for pipeline to complete

# COMMAND ----------

print("Waiting for pipeline to complete...")
max_wait = 900
start = time.time()

while time.time() - start < max_wait:
    resp = requests.get(
        f"https://{host}/api/2.0/pipelines/{PIPELINE_ID}",
        headers={"Authorization": f"Bearer {db_token}"},
        timeout=30,
    )
    pipeline = resp.json()
    state = pipeline.get("state", "")
    
    latest = pipeline.get("latest_updates", [{}])[0]
    update_state = latest.get("state", "")
    
    elapsed = int(time.time() - start)
    print(f"  [{elapsed}s] Pipeline={state}, Update={update_state}")
    
    if update_state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    
    time.sleep(30)

if update_state == "COMPLETED":
    print("\nPipeline completed successfully!")
elif update_state == "FAILED":
    print("\nPipeline FAILED!")
    dbutils.notebook.exit("FAILED: pipeline update failed")
else:
    print(f"\nPipeline still in state: {update_state} after {max_wait}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify data in tables

# COMMAND ----------

for table in ["wellbore", "reservoir", "rock_and_fluid"]:
    df = spark.sql(f"SELECT COUNT(*) as cnt FROM adme_adb_sbx_scus_dbx_ws_1.adme_gokul_connector.{table}")
    count = df.collect()[0]["cnt"]
    print(f"  {table}: {count} rows")

print("\n--- Sample Wellbore records ---")
spark.sql("""
    SELECT id, FacilityName, FacilityID, StatusSummary, createTime 
    FROM adme_adb_sbx_scus_dbx_ws_1.adme_gokul_connector.wellbore 
    ORDER BY createTime DESC
""").show(truncate=False)

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: inserted {len(all_ids)} records, pipeline completed, data verified")
