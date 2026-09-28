# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 — API Ingestion
# MAGIC Calls every enabled source in `api_config.json` and lands the raw JSON in bronze.

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

from datetime import datetime, timezone

from src.ingestion import bronze_table_name, enabled_sources, ingest_source, load_config, state_table_name
from src.state import get_watermark, update_state

config = load_config(os.path.join(ROOT, "config", "api_config.json"))
state_table = state_table_name(config)
run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

print("Run id:", run_id)

# COMMAND ----------


def resolve_secret(source):
    auth = source.get("auth") or {}
    if auth.get("type", "none") in ("none", ""):
        return None
    return dbutils.secrets.get(scope=auth["secret_scope"], key=auth["secret_key"])


results = []

for source in enabled_sources(config):
    name = source["source_name"]
    bronze_table = bronze_table_name(config, source)
    watermark = get_watermark(spark, state_table, name)
    started = datetime.now(timezone.utc)

    try:
        secret = resolve_secret(source)
        count, new_watermark = ingest_source(spark, source, bronze_table, run_id, secret, watermark)
        update_state(spark, state_table, name, new_watermark, run_id, "SUCCESS", count)
        status, error = "SUCCESS", None
    except Exception as failure:
        count = 0
        error = str(failure)
        update_state(spark, state_table, name, watermark, run_id, "FAILED", 0, error)
        status = "FAILED"

    elapsed = round((datetime.now(timezone.utc) - started).total_seconds(), 2)
    results.append((name, source.get("load_type"), count, elapsed, status, error))
    print(name, status, count, "records", elapsed, "s")

# COMMAND ----------

if results:
    display(
        spark.createDataFrame(
            results,
            "source_name STRING, load_type STRING, records BIGINT, seconds DOUBLE, status STRING, error STRING",
        )
    )
else:
    print("No enabled sources in api_config.json.")

# COMMAND ----------

dbutils.notebook.exit(run_id)