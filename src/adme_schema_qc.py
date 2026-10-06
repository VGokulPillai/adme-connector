# Databricks notebook source
# MAGIC %md
# MAGIC # ADME Schema QC / Rule-Checking Notebook
# MAGIC
# MAGIC Pulls the **OSDU JSON Schema** for each kind from the ADME **Schema Service**
# MAGIC (`GET /api/schema-service/v1/schema/{kind}`) and validates it against the data
# MAGIC that landed in the **Unity Catalog tables** produced by the connector.
# MAGIC
# MAGIC Use this to QC / rule-check data **before** loading to ADME (or to verify what
# MAGIC the connector pulled down matches the governing OSDU schema):
# MAGIC
# MAGIC 1. **Schema coverage** — which OSDU fields are surfaced as typed UC columns vs. only in `data_json`.
# MAGIC 2. **Required-field completeness** — OSDU `required` fields that are null in the UC data.
# MAGIC 3. **Primary-key uniqueness** — duplicate `id` detection (the issue that breaks `apply_changes_from_snapshot`).
# MAGIC 4. **Count reconciliation** — ADME Search `totalCount` vs. UC table row count.
# MAGIC
# MAGIC No connector import required — the notebook is self-contained.

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("base_url", "https://admesbxscusins1.energy.azure.com", "ADME base URL")
dbutils.widgets.text("data_partition_id", "opendes", "Data partition id")
dbutils.widgets.text("adme_api_client_id", "e37a6c70-7cbc-4593-80fc-01c1f20203f7", "ADME API client id (OAuth audience)")
dbutils.widgets.dropdown("auth_mode", "service_principal", ["service_principal", "static_token", "managed_identity"], "Auth mode")

# service_principal
dbutils.widgets.text("tenant_id", "", "SP: tenant id")
dbutils.widgets.text("client_id", "", "SP: client id")
dbutils.widgets.text("client_secret", "", "SP: client secret (or secret scope ref)")
# static_token
dbutils.widgets.text("access_token", "", "static_token: pre-issued bearer token")
# managed_identity
dbutils.widgets.text("managed_identity_client_id", "", "MI: user-assigned client id (optional)")

# UC target
dbutils.widgets.text("catalog", "adme_adb_sbx_scus_dbx_ws_1", "UC catalog")
dbutils.widgets.text("schema", "adme_gokul_connector", "UC schema")

BASE_URL = dbutils.widgets.get("base_url").rstrip("/")
DATA_PARTITION = dbutils.widgets.get("data_partition_id")
ADME_API_CLIENT_ID = dbutils.widgets.get("adme_api_client_id")
AUTH_MODE = dbutils.widgets.get("auth_mode")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# table_name -> (canonical OSDU kind, UC table name)
# Rock_and_Fluid maps to the OSDU Sample kind (see adme_schemas.py).
TABLES = {
    "Wellbore":       ("osdu:wks:master-data--Wellbore:1.0.0", "wellbore"),
    "Reservoir":      ("osdu:wks:master-data--Reservoir:1.2.0", "reservoir"),
    "Rock_and_Fluid": ("osdu:wks:master-data--Sample:2.1.0",    "rock_and_fluid"),
}

print(f"ADME: {BASE_URL} | partition: {DATA_PARTITION} | auth: {AUTH_MODE}")
print(f"UC:   {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md ## 2. Authenticate to ADME

# COMMAND ----------

import requests

def get_token() -> str:
    """Return a bearer token for the ADME API using the selected auth mode."""
    if AUTH_MODE == "static_token":
        tok = dbutils.widgets.get("access_token").strip()
        if not tok:
            raise ValueError("auth_mode=static_token requires 'access_token'")
        return tok

    if AUTH_MODE == "service_principal":
        tenant = dbutils.widgets.get("tenant_id").strip()
        client = dbutils.widgets.get("client_id").strip()
        secret = dbutils.widgets.get("client_secret").strip()
        if not (tenant and client and secret):
            raise ValueError("service_principal requires tenant_id, client_id, client_secret")
        resp = requests.post(
            f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client,
                "client_secret": secret,
                "scope": f"{ADME_API_CLIENT_ID}/.default",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    # managed_identity
    from azure.identity import ManagedIdentityCredential
    mi = dbutils.widgets.get("managed_identity_client_id").strip()
    cred = ManagedIdentityCredential(client_id=mi) if mi else ManagedIdentityCredential()
    return cred.get_token(f"{ADME_API_CLIENT_ID}/.default").token


TOKEN = get_token()
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "data-partition-id": DATA_PARTITION,
    "Content-Type": "application/json",
    "Accept": "application/json",
}
print("Authenticated to ADME.")

# COMMAND ----------

# MAGIC %md ## 3. Pull the OSDU schema from the ADME Schema Service

# COMMAND ----------

def _schema_url(kind: str) -> str:
    return f"{BASE_URL}/api/schema-service/v1/schema/{kind}"

def fetch_schema(kind: str) -> dict:
    """Fetch the JSON Schema for an exact kind; fall back to the latest version
    registered for the entity type if the canonical version isn't present."""
    r = requests.get(_schema_url(kind), headers=HEADERS, timeout=60)
    if r.status_code == 200:
        return r.json()
    # Fallback: enumerate versions for the entity type and take the latest.
    # kind format: authority:source:entityType:version
    try:
        authority, source, entity_type, _ = kind.split(":")
    except ValueError:
        r.raise_for_status()
    q = (
        f"{BASE_URL}/api/schema-service/v1/schema"
        f"?authority={authority}&source={source}&entityType={entity_type}&latestVersion=true"
    )
    lr = requests.get(q, headers=HEADERS, timeout=60)
    lr.raise_for_status()
    items = lr.json().get("schemaInfos", []) or lr.json().get("schema", [])
    if not items:
        raise RuntimeError(f"No schema found for {kind} (status {r.status_code})")
    latest_kind = items[0].get("schemaIdentity", {}).get("id") or items[0].get("id")
    rr = requests.get(_schema_url(latest_kind), headers=HEADERS, timeout=60)
    rr.raise_for_status()
    print(f"  (used latest registered version: {latest_kind})")
    return rr.json()


def extract_fields(schema: dict) -> dict:
    """Return {'envelope_required': [...], 'data_fields': {name: required_bool}}.

    OSDU master-data schemas keep entity fields under properties.data, which is
    usually an allOf of $ref(s) + an inline object with properties/required.
    """
    props = schema.get("properties", {}) or {}
    envelope_required = list(schema.get("required", []) or [])

    data_fields: dict[str, bool] = {}
    data_node = props.get("data", {}) or {}

    def _collect(obj: dict):
        if not isinstance(obj, dict):
            return
        required = set(obj.get("required", []) or [])
        for name in (obj.get("properties", {}) or {}).keys():
            data_fields[name] = data_fields.get(name, False) or (name in required)
        for sub in obj.get("allOf", []) or []:
            _collect(sub)

    _collect(data_node)
    # Some schemas inline data properties directly:
    if not data_fields and "properties" in data_node:
        _collect(data_node)
    return {"envelope_required": envelope_required, "data_fields": data_fields}


schemas = {}
for tname, (kind, _uc) in TABLES.items():
    print(f"Fetching schema for {tname} ({kind}) ...")
    try:
        raw = fetch_schema(kind)
        schemas[tname] = extract_fields(raw)
        n_data = len(schemas[tname]["data_fields"])
        n_req = sum(1 for v in schemas[tname]["data_fields"].values() if v)
        print(f"  OK: {n_data} data fields ({n_req} required), "
              f"envelope required: {schemas[tname]['envelope_required']}")
    except Exception as e:  # noqa: BLE001
        print(f"  FAILED: {e}")
        schemas[tname] = {"envelope_required": [], "data_fields": {}}

# COMMAND ----------

# MAGIC %md ## 4. ADME record counts (Search `totalCount`)

# COMMAND ----------

def adme_count(kind_query: str) -> int:
    """Exact-ish record count for a kind via the Search Service."""
    body = {"kind": kind_query, "query": "*", "limit": 1, "trackTotalCount": True}
    r = requests.post(
        f"{BASE_URL}/api/search/v2/query", headers=HEADERS, data=__import__("json").dumps(body), timeout=60
    )
    if r.status_code != 200:
        print(f"  count failed for {kind_query}: {r.status_code} {r.text[:200]}")
        return -1
    return int(r.json().get("totalCount", -1))

adme_counts = {}
for tname, (kind, _uc) in TABLES.items():
    # use wildcard version form for counting
    kind_q = ":".join(kind.split(":")[:3]) + ":*"
    adme_counts[tname] = adme_count(kind_q)
    print(f"{tname}: ADME totalCount = {adme_counts[tname]:,}" if adme_counts[tname] >= 0 else f"{tname}: n/a")

# COMMAND ----------

# MAGIC %md ## 5. Validate UC tables against the schema

# COMMAND ----------

from pyspark.sql import functions as F

report_rows = []

def add(table, check, field, status, detail):
    report_rows.append({
        "table": table, "check": check, "field": field,
        "status": status, "detail": detail,
    })

for tname, (kind, uc_table) in TABLES.items():
    fq = f"{CATALOG}.{SCHEMA}.{uc_table}"
    try:
        df = spark.table(fq)
    except Exception as e:  # noqa: BLE001
        add(tname, "table_exists", "-", "ERROR", f"cannot read {fq}: {e}")
        continue

    cols = set(df.columns)
    total = df.count()
    add(tname, "row_count", "-", "INFO", f"{total:,} rows in {fq}")

    # 5a. Count reconciliation vs ADME
    ac = adme_counts.get(tname, -1)
    if ac >= 0:
        delta = total - ac
        status = "OK" if delta == 0 else "WARN"
        add(tname, "count_reconciliation", "-", status,
            f"UC={total:,} ADME={ac:,} delta={delta:+,}")

    # 5b. Primary-key uniqueness (breaks apply_changes_from_snapshot)
    if "id" in cols:
        dupes = (df.groupBy("id").count().filter(F.col("count") > 1))
        n_dupe_keys = dupes.count()
        if n_dupe_keys == 0:
            add(tname, "pk_uniqueness", "id", "OK", "no duplicate ids")
        else:
            sample = [r["id"] for r in dupes.limit(5).collect()]
            add(tname, "pk_uniqueness", "id", "FAIL",
                f"{n_dupe_keys} duplicate id(s); e.g. {sample}")

    # 5c. Envelope required-field completeness
    for f in schemas[tname]["envelope_required"]:
        # map OSDU envelope names to the connector's flattened columns
        candidates = {
            "acl": ["acl_owners", "acl_viewers"],
            "legal": ["legal_legaltags", "legal_status"],
        }.get(f, [f])
        for c in candidates:
            if c in cols:
                nnull = df.filter(F.col(c).isNull()).count()
                status = "OK" if nnull == 0 else "FAIL"
                add(tname, "required_envelope", c, status,
                    f"{nnull:,} null of {total:,}")

    # 5d. Data required-field completeness (only for fields surfaced as columns)
    for field, required in schemas[tname]["data_fields"].items():
        if not required:
            continue
        if field in cols:
            nnull = df.filter(F.col(field).isNull()).count()
            status = "OK" if nnull == 0 else "WARN"
            add(tname, "required_data", field, status,
                f"{nnull:,} null of {total:,}")
        else:
            add(tname, "required_data", field, "INFO",
                "required by schema but not a typed column (check data_json)")

    # 5e. Schema coverage (data fields surfaced as typed columns)
    surfaced = [f for f in schemas[tname]["data_fields"] if f in cols]
    add(tname, "schema_coverage", "-", "INFO",
        f"{len(surfaced)}/{len(schemas[tname]['data_fields'])} schema data fields surfaced as typed columns")

report = spark.createDataFrame(report_rows) if report_rows else None

# COMMAND ----------

# MAGIC %md ## 6. QC report

# COMMAND ----------

if report is not None:
    # Order so failures surface first.
    order = F.when(F.col("status") == "FAIL", 0).when(F.col("status") == "ERROR", 1) \
             .when(F.col("status") == "WARN", 2).when(F.col("status") == "OK", 3).otherwise(4)
    display(report.withColumn("_o", order).orderBy("_o", "table", "check").drop("_o"))
else:
    print("No report rows produced.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to read the report
# MAGIC - **FAIL** — a hard rule violation: duplicate primary keys, or a schema-required
# MAGIC   envelope field (`id`/`kind`/`acl`/`legal`) is null. Fix before loading to ADME.
# MAGIC - **WARN** — soft issue: UC/ADME count mismatch, or a required data field is null.
# MAGIC - **INFO** — coverage / context (e.g. a required field lives only in `data_json`).
# MAGIC - **OK** — rule satisfied.
# MAGIC
# MAGIC The **pk_uniqueness** check is the one that directly explains the
# MAGIC `APPLY_CHANGES_FROM_SNAPSHOT_ERROR` ("Expected at most 1 row per key"):
# MAGIC duplicate ids in ADME must be resolved at source or deduped deterministically
# MAGIC (keep latest by `modifyTime`).
