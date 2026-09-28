# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC Creates the bronze, silver and config schemas and the ingestion state table.
# MAGIC Run once before the first pipeline run.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys


def project_root():
    candidates = []
    try:
        path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        parent = os.path.dirname(os.path.dirname(path))
        candidates += [parent, "/Workspace" + parent]
    except Exception:
        pass
    cwd = os.getcwd()
    candidates += [cwd, os.path.dirname(cwd), os.path.dirname(os.path.dirname(cwd))]

    for candidate in candidates:
        if os.path.isdir(os.path.join(candidate, "src")):
            return candidate
    raise RuntimeError("Project root not found. Checked: " + str(candidates))


ROOT = project_root()
if ROOT not in sys.path:
    sys.path.append(ROOT)

print("Project root:", ROOT)

# COMMAND ----------

from src.ingestion import load_config, state_table_name
from src.state import create_state_table

config = load_config(os.path.join(ROOT, "config", "api_config.json"))
catalog = config["catalog"]

# COMMAND ----------

for schema in [config["bronze_schema"], config["silver_schema"], config["config_schema"]]:
    spark.sql("CREATE SCHEMA IF NOT EXISTS " + catalog + "." + schema)

state_table = state_table_name(config)
create_state_table(spark, state_table)

print("Catalog:", catalog)
print("Schemas:", config["bronze_schema"], config["silver_schema"], config["config_schema"])
print("State table:", state_table)

# COMMAND ----------

display(spark.table(state_table))

# COMMAND ----------

registered = [(s["source_name"], s.get("load_type"), s.get("target_table"), s.get("enabled", True)) for s in config["sources"]]
display(spark.createDataFrame(registered, "source_name STRING, load_type STRING, target_table STRING, enabled BOOLEAN"))

# COMMAND ----------

dbutils.secrets.get(
    scope="api_integration",
    key="openweather_api_key"
)

# COMMAND ----------

dbutils.secrets.get(
    scope="api_integration",
    key="exchangerate_api_key"
)