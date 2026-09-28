from datetime import datetime, timezone

STATE_SCHEMA = (
    "source_name STRING, last_watermark STRING, last_run_id STRING, "
    "last_run_status STRING, last_record_count BIGINT, last_error STRING, updated_at TIMESTAMP"
)


def create_state_table(spark, table):
    spark.sql("CREATE TABLE IF NOT EXISTS " + table + " (" + STATE_SCHEMA + ") USING DELTA")


def get_watermark(spark, table, source_name):
    rows = spark.sql(
        "SELECT last_watermark FROM " + table + " WHERE source_name = '" + source_name + "'"
    ).collect()
    return rows[0][0] if rows else None


def update_state(spark, table, source_name, watermark, run_id, status, record_count, error=None):
    row = [(
        source_name,
        watermark,
        run_id,
        status,
        int(record_count),
        error[:1000] if error else None,
        datetime.now(timezone.utc),
    )]
    spark.createDataFrame(row, STATE_SCHEMA).createOrReplaceTempView("_state_update")
    spark.sql(
        "MERGE INTO " + table + " t USING _state_update s ON t.source_name = s.source_name "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def reset_state(spark, table, source_name=None):
    if source_name:
        spark.sql("DELETE FROM " + table + " WHERE source_name = '" + source_name + "'")
    else:
        spark.sql("TRUNCATE TABLE " + table)
