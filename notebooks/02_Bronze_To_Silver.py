# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 — Bronze to Silver
# MAGIC Flattens, standardizes and deduplicates the latest bronze run per source
# MAGIC and merges it into the target silver table.

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

from pyspark.sql.functions import col, max as spark_max

from src.ingestion import bronze_table_name, enabled_sources, load_config, silver_table_name
from src.transformation import transform_source

config = load_config(os.path.join(ROOT, "config", "api_config.json"))

# COMMAND ----------

results = []

for source in enabled_sources(config):
    name = source["source_name"]
    bronze_table = bronze_table_name(config, source)
    silver_table = silver_table_name(config, source)

    if not spark.catalog.tableExists(bronze_table):
        results.append((name, silver_table, None, 0, "NO_BRONZE_TABLE", None))
        continue

    bronze = spark.table(bronze_table)
    target_run = bronze.select(spark_max("_run_id")).collect()[0][0]
    if target_run is None:
        results.append((name, silver_table, None, 0, "EMPTY_BRONZE", None))
        continue

    batch = bronze.filter(col("_run_id") == target_run)

    try:
        count = transform_source(spark, batch, source, silver_table)
        results.append((name, silver_table, target_run, count, "SUCCESS", None))
        status = "SUCCESS"
    except Exception as failure:
        results.append((name, silver_table, target_run, 0, "FAILED", str(failure)))
        print(name, "-> FAILED:", failure)
        continue

    print(name, "->", silver_table, status, count, "records")

# COMMAND ----------

if results:
    display(
        spark.createDataFrame(
            results,
            "source_name STRING, silver_table STRING, run_id STRING, records BIGINT, status STRING, error STRING",
        )
    )
else:
    print("No enabled sources in api_config.json.")