import json

from pyspark.sql.functions import current_timestamp, lit

from src.api_client import fetch_all_pages, get_auth_headers


def load_config(path):
    with open(path, "r") as handle:
        return json.load(handle)


def enabled_sources(config, source_name=None):
    sources = [s for s in config["sources"] if s.get("enabled", True)]
    if source_name:
        sources = [s for s in sources if s["source_name"] == source_name]
    return sources


def bronze_table_name(config, source):
    return config["catalog"] + "." + config["bronze_schema"] + "." + source["source_name"]


def silver_table_name(config, source):
    return config["catalog"] + "." + source["target_table"]


def state_table_name(config):
    return config["catalog"] + "." + config["config_schema"] + "." + config.get("state_table", "api_ingestion_state")


def get_nested(record, path):
    value = record
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def resolve_url(source, secret=None):
    url = source["api_url"]
    if secret and "{secret}" in url:
        url = url.replace("{secret}", secret)
    return url


def build_request_params(source, watermark=None):
    params = dict(source.get("params") or {})
    if source.get("load_type") == "incremental":
        value = watermark or source.get("initial_watermark")
        key = source.get("incremental_param") or source.get("incremental_column")
        if value and key:
            params[key] = value
    return params


def write_bronze(spark, records, table, run_id):
    rows = [(json.dumps(record),) for record in records]
    df = spark.createDataFrame(rows, "record STRING")
    df = df.withColumn("_run_id", lit(run_id)).withColumn("_ingested_at", current_timestamp())
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    return len(rows)


def next_watermark(records, incremental_column, current):
    if not incremental_column:
        return current
    values = [get_nested(r, incremental_column) for r in records]
    values = [str(v) for v in values if v is not None]
    return max(values) if values else current


def ingest_source(spark, source, bronze_table, run_id, secret=None, watermark=None):
    headers, auth_params = get_auth_headers(source.get("auth"), secret)
    headers.update(source.get("headers") or {})
    params = build_request_params(source, watermark)
    params.update(auth_params)

    resolved = dict(source, api_url=resolve_url(source, secret))
    records = fetch_all_pages(resolved, headers, params)
    if not records:
        return 0, watermark

    write_bronze(spark, records, bronze_table, run_id)
    return len(records), next_watermark(records, source.get("incremental_column"), watermark)
