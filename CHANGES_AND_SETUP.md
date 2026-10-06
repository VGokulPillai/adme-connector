# ADME Connector — Required Code Changes & Setup Guide

This document lists **every change required** to take the base Lakeflow Community
Connector and make the ADME (Azure Data Manager for Energy / OSDU) pipeline
actually run end-to-end in a customer workspace. Each section explains **what**
to change, **where**, **the exact code**, and **why** it is needed.

There are two categories of change:

1. **Code changes** — edits to `ingest.py` and the connector source. These are
   already applied in this repo; they are documented here so they can be
   reproduced in a fresh workspace.
2. **Configuration changes** — the Unity Catalog connection and pipeline compute
   settings. These are *not* code, but they are the difference between a pipeline
   that fails and one that succeeds (this is where the `managed_identity` failure
   comes from).

---

## Change 1 — `ingest.py`: make `databricks.labs.*` importable (namespace hack)

**File:** `src/ingest.py` (top of file)

**Why:** The pipeline compute ships with the `databricks-sdk` package, which
already owns the top-level `databricks` namespace. When the pipeline tries to
`import databricks.labs.community_connector...`, Python resolves `databricks` to
the SDK's package and never looks inside our workspace `src/` folder, producing:

```
ModuleNotFoundError: No module named 'databricks.labs.community_connector.pipeline'
```

The fix manually extends the `databricks` namespace path and registers a
`databricks.labs` namespace module that points at our source tree, so the
connector library becomes importable **alongside** the SDK.

**Code (must be the very first thing in `ingest.py`):**

```python
import sys, importlib, types

# Point this at the src/ folder of the connector as it lands in the workspace.
_src = "/Workspace/Users/<you>@<company>.com/testing/adme_pipeline/src"
if _src not in sys.path:
    sys.path.insert(0, _src)

import databricks
databricks.__path__.insert(0, _src + "/databricks")

_labs = types.ModuleType("databricks.labs")
_labs.__path__ = [_src + "/databricks/labs"]
_labs.__package__ = "databricks.labs"
sys.modules["databricks.labs"] = _labs
```

> **Customer action:** update the `_src` path to match where the `src/` folder is
> imported in *their* workspace (Repos path or Workspace user folder).

---

## Change 2 — `ingest.py`: enable Unity Catalog option injection

**File:** `src/ingest.py`

**Why:** The connector reads its credentials (base URL, partition, auth mode,
token, etc.) from a **Unity Catalog connection**, not from hard-coded values.
For those connection options to be injected into the Spark DataFrame reader at
runtime, this Spark conf must be turned on. Without it the connector receives an
empty options dict and fails validation ("requires connection option ...").

**Code:**

```python
spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
```

---

## Change 3 — `ingest.py`: declare which tables to ingest (per-table config)

**File:** `src/ingest.py`

**Why:** This is the pipeline specification. It names the UC connection to use
and lists the OSDU kinds (tables) to pull. **This is the file you tweak for each
table.** The `source_table` values map to the supported OSDU master-data kinds:

| `source_table` | OSDU kind |
|---|---|
| `Wellbore` | `osdu:wks:master-data--Wellbore` |
| `Reservoir` | `osdu:wks:master-data--Reservoir` |
| `Rock_and_Fluid` | `osdu:wks:master-data--Sample` |

**Code:**

```python
source_name = "adme"

pipeline_spec = {
    "connection_name": "adme_production",   # <-- name of the UC connection
    "objects": [
        {"table": {"source_table": "Wellbore"}},
        {"table": {"source_table": "Reservoir"}},
        {"table": {"source_table": "Rock_and_Fluid"}},
    ],
}

register(spark, source_name)   # dynamically import + register the source
ingest(spark, pipeline_spec)   # ingest the tables in the spec
```

> **Customer action:**
> - Set `connection_name` to their UC connection name.
> - Add/remove `{"table": {"source_table": "..."}}` entries per table they want.
> - To point a table at a *different* OSDU kind without code changes, they can set
>   a per-table option `kind_query_<table>` on the connection/table options
>   (the connector honours e.g. `kind_query_rock_and_fluid`).

---

## Change 4 — connector source: Lucene query fix for missing `modifyTime`

**File:** `src/databricks/labs/community_connector/sources/adme/adme.py`
(and the merged `_generated_adme_python_source.py`), function `_build_lucene_range`.

**Why:** The connector does incremental sync by filtering OSDU records on a
`modifyTime` range. **Some ADME instances do not populate `modifyTime` in the
search index** — records only carry `createTime`. With the original query
(`modifyTime:[* TO "..."]`) every record was filtered out and the pipeline
ingested **0 rows** even though data existed. The fix broadens the first-run
query to also match `createTime` and records that have no `modifyTime` at all.

**Before:**

```python
@staticmethod
def _build_lucene_range(since: str, until: str) -> str:
    if ADMELakeflowConnect._parse_iso(since) <= ADMELakeflowConnect._parse_iso(EPOCH_ISO):
        return f'modifyTime:[* TO "{until}"]'
    return f'modifyTime:{{"{since}" TO "{until}"]'
```

**After (current code):**

```python
@staticmethod
def _build_lucene_range(since: str, until: str) -> str:
    # First run (since <= epoch): match modifyTime OR createTime OR records
    # that have no modifyTime indexed at all.
    if ADMELakeflowConnect._parse_iso(since) <= ADMELakeflowConnect._parse_iso(EPOCH_ISO):
        return (
            f'(modifyTime:[* TO "{until}"]'
            f' OR createTime:[* TO "{until}"]'
            f' OR (*:* NOT _exists_:modifyTime))'
        )
    # Incremental runs: fall back to createTime when modifyTime is absent.
    return (
        f'(modifyTime:{{"{since}" TO "{until}"]'
        f' OR (NOT _exists_:modifyTime AND createTime:{{"{since}" TO "{until}"]))'
    )
```

---

## Change 5 — connector source: pickle-safety for Spark serialization

**File:** `adme.py` / `_generated_adme_python_source.py` — the auth providers and
the connector class.

**Why:** Spark's Python Data Source pickles the connector object and ships it from
the driver to the executors. Any field holding a `threading.Lock` or a
`requests.Session` is **not picklable**, producing:

```
Could not serialize object: TypeError: cannot pickle '_thread.lock' object
(PYTHON_DATA_SOURCE_ERROR)
```

The fix implements `__getstate__` / `__setstate__` on the classes that hold those
objects: strip the unpicklable field before pickling, and recreate it on the
executor after unpickling.

**Code (pattern applied to `_TokenCache`, `_AzureIdentityTokenProvider`, and
`ADMELakeflowConnect`):**

```python
def __getstate__(self):
    state = self.__dict__.copy()
    del state["_lock"]          # or "_session" on the connector class
    return state

def __setstate__(self, state):
    self.__dict__.update(state)
    self._lock = threading.Lock()   # or requests.Session() on the connector
```

> This is the same fix Genie applied in the customer's workspace — it is correct
> and should be kept. **However**, it does not by itself fix the `managed_identity`
> runtime failure below, because it still keeps the Azure credential object in
> state. The durable fix is the auth-mode change in Change 6.

---

## Change 6 — **Configuration:** auth mode (this is the customer's blocker)

**Not a code change — a Unity Catalog connection change.** This is the single most
important item, and the cause of the `_credential.get_token()` / `STREAM_FAILED`
error the customer is hitting.

### Why `managed_identity` fails on a pipeline

- A **Lakeflow / DLT pipeline always runs on its own managed pipeline compute**
  (a job cluster). You **cannot** attach it to an all-purpose cluster.
- The customer's Managed Identity (`dbmanagedidentity`) is attached to the
  **all-purpose cluster** — which is why the *smoke-test notebook* passes.
- At pipeline runtime, `read_partition` executes on the **job-cluster worker**,
  which does **not** have that identity, so
  `ManagedIdentityCredential().get_token(scope)` fails:

```
_search_paginated → _post_with_retry → _common_headers → get_token → _credential.get_token(scope)
PYTHON_EXCEPTION / STREAM_FAILED
```

There is no toggle that fixes this — the identity has to live on the compute that
runs the code, and for a pipeline you don't control that compute.

### Option A — `service_principal` (recommended for production)

Uses a plain OAuth2 client-credentials POST to `login.microsoftonline.com`. It has
**no `azure-identity` / IMDS dependency**, works on **any** compute (driver,
executor, job cluster, serverless), stores only strings (so the pickle error
disappears too), and is the standard production pattern.

1. Create/reuse an Azure AD **app registration (service principal)** with a secret.
2. In the ADME **Entitlements** service, grant it:
   `users.datalake.viewers@<data-partition-id>.dataservices.energy`
3. Set these fields on the `adme_production` UC connection:

| Connection field | Value |
|---|---|
| `auth_mode` | `service_principal` |
| `tenant_id` | Azure AD tenant / directory ID |
| `client_id` | SP app (client) ID |
| `client_secret` | SP secret |
| `adme_api_client_id` | ADME API app registration ID (OAuth audience) |
| `base_url` | `https://<instance>.energy.azure.com` |
| `data_partition_id` | e.g. `<inst>-opendes` |

Leave `managed_identity_client_id` empty. **No code change needed** — the
connector already supports this mode (`_TokenCache`).

### Option B — `static_token` (quick test / interim only)

1. On the all-purpose cluster (where MI works), mint a token for the ADME scope
   `api://<adme_api_client_id>/.default`.
2. Set `auth_mode = static_token` and paste it into the **Access token** field.

Caveat: tokens expire in ~1 hour — use for a single test run, not production.

---

## Change 7 — **Configuration:** pipeline compute (classic, not serverless)

**File:** pipeline settings / DAB YAML.

**Why:** In earlier testing, serverless DLT compute failed against certain UC
storage credential vending (e.g. `TempAzureSAS cannot be cast to TempAzureAAD`
with OneLake Beta), and Managed Identity is unavailable on serverless. Classic
DLT compute negotiates credentials differently and is the reliable choice.

```yaml
serverless: false
clusters:
  - label: default
    node_type_id: Standard_E4ads_v6   # pick an SKU available in the region
    autoscale:
      min_workers: 1
      max_workers: 5
```

> If using `service_principal` auth (Change 6), serverless can also work because
> auth no longer depends on the compute identity — but classic remains the safest
> default.

---

## Change 8 — **Configuration:** ingestion mode (snapshot vs CDC) + full refresh

**File:** `src/databricks/labs/community_connector/sources/adme/adme_schemas.py`
(`TABLE_METADATA[...]["ingestion_type"]`), plus a **Full Refresh** on the pipeline.

**Why:** The three tables default to `ingestion_type: "cdc"`, which makes
`ingest()` build the flow with `spark.readStream` + `apply_changes` (a streaming
flow). For a **bulk "pull everything" full mirror**, the streaming path is a poor
fit at scale: the whole load arrives as one large micro-batch, and after the data
lands the update still has to run the CDC `MERGE`, finalise the checkpoint, and do
table maintenance. At a few thousand rows this is seconds; at ~4.3M rows this
winddown/commit phase becomes very expensive and looks like a hang (observed:
data committed after ~1h15m, but the update sat for another ~1.5h before it was
cancelled).

**Snapshot mode terminates cleanly at any volume.** Setting
`ingestion_type: "snapshot"` makes `ingest()` use `_create_snapshot_table` →
`spark.read` (batch) + `apply_changes_from_snapshot`. A batch read has a natural
end, so there is no streaming offset/checkpoint lifecycle to grind through.

**Change:**

```python
# adme_schemas.py — TABLE_METADATA
"Wellbore": {
    "primary_keys": ["id"],
    "cursor_field": "modifyTime",
    "ingestion_type": "snapshot",   # was "cdc" — full-mirror, self-terminating
},
```

> Regenerate the merged `_generated_adme_python_source.py` from source — **do not
> hand-edit the generated file**, it is overwritten on the next build.

**Critical operational step — Full Refresh:** After switching mode you MUST run
**Run pipeline ▸ Run pipeline with full table refresh**, *not* a normal update.
The existing target table still carries a streaming checkpoint bound to the old
`apply_changes` (CDC) flow; switching to the batch `apply_changes_from_snapshot`
flow is incompatible with that checkpoint until a full refresh resets it.
**Manually deleting rows from the UC table does not clear the checkpoint** — only
a full refresh does. If it still conflicts on the flow-type change, drop the
target table once and full-refresh again.

### Which mode to run long-term

| | Snapshot (recommended for full mirror) | CDC / streaming |
|---|---|---|
| Termination | Natural (batch read) | Terminates, but winddown cost grows with volume |
| Cost per run | Full re-read each run | Incremental (only changed records) |
| Deletes | Reconciled (rows absent from snapshot are removed) | Not reflected (OSDU deletes aren't in the Search index) |
| Duplicate PK | Surfaced as an error (see Change 9) | Masked — `sequence_by=modifyTime` keeps latest |
| Best for | Initial load + periodic full mirror of master data | Low-latency incremental *once* `modifyTime` is reliably indexed |

Recommendation: **snapshot for the initial/full ADME mirror.** Revisit CDC only if
low-latency incremental is needed *and* `modifyTime` is dependably populated in
the ADME Search index (see Change 4 — some instances only populate `createTime`).

---

## Change 9 — Duplicate primary keys in snapshot mode

**Symptom:** `APPLY_CHANGES_FROM_SNAPSHOT_ERROR` —
`Found 2 rows for key '{"id": "..."}' ... Expected at most 1 row per key`.

**Why it appears only in snapshot mode:** `apply_changes_from_snapshot` treats the
input as a full point-in-time state and requires **≤1 row per key**, with no
sequence column to break ties. Streaming/CDC never hit this because
`apply_changes` uses `sequence_by = modifyTime` to keep the **latest** row per key
and silently drop older duplicates. So a duplicate `id` in ADME is *masked* by
streaming but *correctly surfaced* by snapshot as the data-quality issue it is
(OSDU's PK constraint should prevent it — raise with the ADME/Microsoft team).

**Fixes, in order of preference:**

1. **Fix at source** — resolve the duplicate in ADME. Cleanest; matches OSDU's own
   guarantee.
2. **Deterministic in-pipeline guard (interim, tracked patch):** if you must
   dedupe in the pipeline, do it deterministically to match streaming's
   "latest wins" — **not** a bare `dropDuplicates(id)`, which keeps an *arbitrary*
   (possibly stale) row:

```python
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

w = Window.partitionBy(*primary_keys).orderBy(col("modifyTime").desc_nulls_last())
df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
```

> Prefer applying this as a **transformation/view**, not as an edit to the shared
> framework file `pipeline/ingestion_pipeline.py` — framework edits are overwritten
> on rebuild and affect every table/connector, not just ADME.

---

## Governance & security (ACL / legal metadata)

**What comes across automatically (no extra config):** the connector projects the
**record-level** OSDU governance metadata attached to each record into typed
columns:

- `acl_owners`, `acl_viewers`
- `legal_legaltags`, `legal_status`, `legal_otherRelevantDataCountries`

**What the connector does NOT do (by design):**

- It does **not** sync the full OSDU **Entitlements groups** or **Legal-tag
  catalogue** as separate objects/tables.
- It does **not** translate OSDU entitlements into **Unity Catalog
  permissions/groups**.

If governance metadata is available from ADME but not surfacing correctly in the
landed tables, treat that as a **connector bug** and fix it in `sources/adme/`.
Mapping OSDU groups → UC permissions is a **customer-specific governance concern**
(see below), not connector scope. A good validation step is to compare what the
pipeline identity can actually see in ADME against what lands in Databricks.

---

## Why this connector is open source (and where the boundary is)

The connector is intentionally the **foundation/bridge** for getting ADME/OSDU
data into Unity Catalog — not a product that covers every customer-specific
governance requirement out of the box. Open source is what makes that work:

- **Transparency** — security/governance reviewers can inspect exactly how auth,
  ACL/legal projection, and ingestion behave, rather than trusting a black box.
- **Extensibility without lock-in** — customers adapt it to their needs on their
  own timeline, no vendor-roadmap dependency.
- **Clean separation of concerns** — the shared **framework** stays generic,
  **source-specific** logic lives in `sources/adme/`, and **customer-exclusive**
  logic (e.g. CVX group mappings) lives in the customer's own layer — never in the
  shared source.

**Recommended architecture:**

```
ADME → Connector → UC raw tables → Governance/mapping layer → Governed consumer tables/views
```

The connector owns the first hop reliably. A thin customer-owned governance layer
on top can:

- map ADME entitlements/groups to Databricks groups (customer-specific — keep out
  of the shared connector),
- store ACL and legal-tag mappings,
- apply UC row filters / ABAC policies from those mappings,
- add customer transformations/business rules before exposing data downstream.

The CDC/SCD handling in the connector exists to make the *ingestion* path robust
and production-ready; governance policy belongs in the layer above.

---

## Quick checklist for a fresh workspace

1. Import the `src/` tree into the workspace.
2. Edit `_src` path in `ingest.py` (Change 1).
3. Keep the UC option-injection conf (Change 2).
4. Set `connection_name` + table list in `ingest.py` (Change 3).
5. Ensure the connector source has the Lucene fix (Change 4) and pickle-safety
   (Change 5) — already in this repo.
6. Create the UC connection with `auth_mode = service_principal` (Change 6).
7. Configure the pipeline on classic compute (Change 7) and point it at
   `ingest.py`.
8. For the initial/full mirror, set `ingestion_type: "snapshot"` and start with a
   **full table refresh** (Change 8). Watch for duplicate-PK errors (Change 9).
9. Run the pipeline.

---

## Testing findings (reference)

- `service_principal` auth + Lucene fix validated: 8K basin-filtered load and a
  full ~4.3M Wellbore load both completed.
- Streaming (CDC) load of ~4.3M did not wind down cleanly at scale → switched to
  **snapshot + full refresh**, which loaded the full dataset and terminated
  cleanly (Change 8).
- Snapshot surfaced a **duplicate Wellbore `id`** in ADME that streaming had
  masked via `sequence_by=modifyTime` — a genuine source-side data-quality issue
  (Change 9).
