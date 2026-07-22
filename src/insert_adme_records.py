# Databricks notebook source
# MAGIC %md
# MAGIC # Insert Test Records into ADME (OSDU)
# MAGIC
# MAGIC This notebook inserts sample Wellbore, Reservoir, and Rock_and_Fluid records
# MAGIC into the ADME instance so the ingestion pipeline can pull them.
# MAGIC
# MAGIC **Prerequisites:** The cluster must have managed identity access to the ADME instance.

# COMMAND ----------

import json
import time
import requests
from azure.identity import ManagedIdentityCredential

ADME_BASE_URL = "https://admesbxscusins1.energy.azure.com"
DATA_PARTITION = "opendes"
ADME_API_CLIENT_ID = "e37a6c70-7cbc-4593-80fc-01c1f20203f7"

credential = ManagedIdentityCredential()
token = credential.get_token(f"{ADME_API_CLIENT_ID}/.default").token

HEADERS = {
    "Authorization": f"Bearer {token}",
    "data-partition-id": DATA_PARTITION,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

print(f"Authenticated to {ADME_BASE_URL} (partition: {DATA_PARTITION})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check available legal tags and entitlement groups

# COMMAND ----------

legal_resp = requests.get(
    f"{ADME_BASE_URL}/api/legal/v1/legaltags",
    headers=HEADERS,
    timeout=30,
)
print(f"Legal tags ({legal_resp.status_code}):")
if legal_resp.status_code == 200:
    tags = legal_resp.json().get("legalTags", [])
    for t in tags[:10]:
        print(f"  - {t.get('name')}")
    LEGAL_TAG = tags[0]["name"] if tags else f"{DATA_PARTITION}-public-usa-dataset"
    print(f"\nUsing legal tag: {LEGAL_TAG}")
else:
    LEGAL_TAG = f"{DATA_PARTITION}-public-usa-dataset"
    print(f"Could not fetch legal tags, defaulting to: {LEGAL_TAG}")

# COMMAND ----------

entitlements_resp = requests.get(
    f"{ADME_BASE_URL}/api/entitlements/v2/groups",
    headers=HEADERS,
    timeout=30,
)
print(f"Entitlement groups ({entitlements_resp.status_code}):")
if entitlements_resp.status_code == 200:
    groups = entitlements_resp.json().get("groups", [])
    owner_groups = [g["email"] for g in groups if "owners" in g.get("email", "")]
    viewer_groups = [g["email"] for g in groups if "viewers" in g.get("email", "")]
    print(f"  Owner groups: {owner_groups[:5]}")
    print(f"  Viewer groups: {viewer_groups[:5]}")
    ACL_OWNERS = owner_groups[0] if owner_groups else f"data.default.owners@{DATA_PARTITION}.dataservices.energy"
    ACL_VIEWERS = viewer_groups[0] if viewer_groups else f"data.default.viewers@{DATA_PARTITION}.dataservices.energy"
else:
    ACL_OWNERS = f"data.default.owners@{DATA_PARTITION}.dataservices.energy"
    ACL_VIEWERS = f"data.default.viewers@{DATA_PARTITION}.dataservices.energy"

print(f"\nUsing ACL owners:  {ACL_OWNERS}")
print(f"Using ACL viewers: {ACL_VIEWERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define test records

# COMMAND ----------

timestamp = time.strftime("%Y%m%d-%H%M%S")

wellbore_records = [
    {
        "kind": "osdu:wks:master-data--Wellbore:1.0.0",
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {
            "legaltags": [LEGAL_TAG],
            "otherRelevantDataCountries": ["US"],
            "status": "compliant",
        },
        "data": {
            "FacilityName": f"Databricks Test Wellbore Alpha {timestamp}",
            "FacilityID": f"WB-DBX-ALPHA-{timestamp}",
            "FacilityTypeID": "Wellbore",
            "WellID": f"{DATA_PARTITION}:master-data--Well:dbx-test-well-{timestamp}",
            "StatusSummary": "Active",
            "TargetFormation": "Permian Basin",
        },
    },
    {
        "kind": "osdu:wks:master-data--Wellbore:1.0.0",
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {
            "legaltags": [LEGAL_TAG],
            "otherRelevantDataCountries": ["US"],
            "status": "compliant",
        },
        "data": {
            "FacilityName": f"Databricks Test Wellbore Beta {timestamp}",
            "FacilityID": f"WB-DBX-BETA-{timestamp}",
            "FacilityTypeID": "Wellbore",
            "WellID": f"{DATA_PARTITION}:master-data--Well:dbx-test-well-{timestamp}",
            "StatusSummary": "Plugged and Abandoned",
            "TargetFormation": "Eagle Ford",
        },
    },
]

reservoir_records = [
    {
        "kind": "osdu:wks:master-data--Reservoir:1.0.0",
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {
            "legaltags": [LEGAL_TAG],
            "otherRelevantDataCountries": ["US"],
            "status": "compliant",
        },
        "data": {
            "ReservoirName": f"Databricks Test Reservoir {timestamp}",
            "ReservoirID": f"RES-DBX-{timestamp}",
            "ReservoirType": "Sandstone",
            "ReservoirDescription": "Test reservoir inserted from Databricks notebook",
            "PorosityAverage": 0.18,
            "PermeabilityHorizontal": 125.5,
            "ReservoirTemperature": 95.0,
            "GrossThickness": 45.0,
            "NetPayThickness": 32.0,
        },
    },
]

rock_and_fluid_records = [
    {
        "kind": "osdu:wks:master-data--Sample:1.0.0",
        "acl": {"owners": [ACL_OWNERS], "viewers": [ACL_VIEWERS]},
        "legal": {
            "legaltags": [LEGAL_TAG],
            "otherRelevantDataCountries": ["US"],
            "status": "compliant",
        },
        "data": {
            "SampleAcquisition": {
                "SampleAcquisitionJobID": f"SAJ-DBX-{timestamp}",
                "SampleAcquisitionTypeID": "Core",
                "AcquisitionStartDate": "2026-06-01T00:00:00.000Z",
                "AcquisitionEndDate": "2026-06-01T12:00:00.000Z",
                "SampleAcquisitionDetail": {
                    "WellboreID": f"{DATA_PARTITION}:master-data--Wellbore:dbx-test-{timestamp}",
                    "ToolKind": "Rotary Sidewall Coring",
                    "RunNumber": "1",
                    "TopDepth": 3200.0,
                    "BaseDepth": 3250.0,
                    "FormationCondition": {
                        "Pressure": 4500.0,
                        "Temperature": 180.0,
                    },
                },
            },
        },
    },
]

print(f"Prepared {len(wellbore_records)} Wellbore records")
print(f"Prepared {len(reservoir_records)} Reservoir records")
print(f"Prepared {len(rock_and_fluid_records)} Rock_and_Fluid (Sample) records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert records into ADME

# COMMAND ----------

def insert_records(records, record_type):
    """Insert records into ADME via the Storage API."""
    url = f"{ADME_BASE_URL}/api/storage/v2/records"
    resp = requests.put(url, headers=HEADERS, json=records, timeout=60)
    
    if resp.status_code in (200, 201):
        result = resp.json()
        ids = result.get("recordIds", [])
        print(f"[OK] Inserted {len(ids)} {record_type} record(s):")
        for rid in ids:
            print(f"  - {rid}")
        return ids
    else:
        print(f"[FAIL] {record_type} insert failed: {resp.status_code}")
        print(f"  Response: {resp.text[:500]}")
        return []

# COMMAND ----------

print("=" * 60)
print("Inserting records into ADME...")
print("=" * 60)

wb_ids = insert_records(wellbore_records, "Wellbore")
print()
res_ids = insert_records(reservoir_records, "Reservoir")
print()
rf_ids = insert_records(rock_and_fluid_records, "Rock_and_Fluid")

print()
print("=" * 60)
total = len(wb_ids) + len(res_ids) + len(rf_ids)
print(f"Done! Inserted {total} total records into ADME.")
print("The pipeline will pick these up on the next run via modifyTime cursor.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — Search for the inserted records

# COMMAND ----------

import time
print("Waiting 10 seconds for ADME indexing...")
time.sleep(10)

for kind, label in [
    ("osdu:wks:master-data--Wellbore:*", "Wellbore"),
    ("osdu:wks:master-data--Reservoir:*", "Reservoir"),
    ("osdu:wks:master-data--Sample:*", "Rock_and_Fluid"),
]:
    search_body = {
        "kind": kind,
        "query": f'data.FacilityID:\"*DBX*\" OR data.ReservoirID:\"*DBX*\" OR data.SampleAcquisition.SampleAcquisitionJobID:\"*DBX*\"',
        "limit": 10,
    }
    resp = requests.post(
        f"{ADME_BASE_URL}/api/search/v2/query",
        headers=HEADERS,
        json=search_body,
        timeout=30,
    )
    if resp.status_code == 200:
        results = resp.json().get("results", [])
        print(f"\n{label}: Found {len(results)} Databricks-inserted records")
        for r in results[:5]:
            print(f"  - {r.get('id')} (modified: {r.get('modifyTime')})")
    else:
        print(f"\n{label}: Search returned {resp.status_code}")
