import sys, importlib, types

_src = "/Workspace/Users/gokul.pillai@databricks.com/testing/adme_pipeline/src"
if _src not in sys.path:
    sys.path.insert(0, _src)

import databricks
databricks.__path__.insert(0, _src + "/databricks")

_labs = types.ModuleType("databricks.labs")
_labs.__path__ = [_src + "/databricks/labs"]
_labs.__package__ = "databricks.labs"
sys.modules["databricks.labs"] = _labs

from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# Enable the injection of connection options from Unity Catalog connections into connectors
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

source_name = "adme"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
#
# Supported ADME (OSDU) tables:
#   - Wellbore       (osdu:wks:master-data--Wellbore)
#   - Reservoir      (osdu:wks:master-data--Reservoir)
#   - Rock_and_Fluid (osdu:wks:master-data--Sample)
#
# All tables use "id" as primary key and "modifyTime" as cursor field (CDC).
# Default catalog: adme_adb_sbx_scus_dbx_ws_1
# Default schema:  adme_gokul_connector
# =============================================================================

pipeline_spec = {
    "connection_name": "adme_production",
    "objects": [
        {"table": {"source_table": "Wellbore"}},
        {"table": {"source_table": "Reservoir"}},
        {"table": {"source_table": "Rock_and_Fluid"}},
    ],
}


# Dynamically import and register the LakeFlow source
register(spark, source_name)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
