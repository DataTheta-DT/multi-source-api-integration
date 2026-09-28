from pyspark.sql import Window
from pyspark.sql.functions import col, from_json, lit, row_number, schema_of_json
from pyspark.sql.types import StructType

SCHEMA_SAMPLE_SIZE = 500


def infer_record_schema(spark, bronze_df, sample_size=SCHEMA_SAMPLE_SIZE):
    rows = bronze_df.select("record").limit(sample_size).collect()
    if not rows:
        return None
    payload = "[" + ",".join(r[0] for r in rows if r[0]) + "]"
    ddl = spark.range(1).select(schema_of_json(lit(payload))).collect()[0][0]
    if ddl.upper().startswith("ARRAY<"):
        ddl = ddl[6:-1]
    return ddl


def parse_bronze(spark, bronze_df):
    ddl = infer_record_schema(spark, bronze_df)
    if not ddl:
        return bronze_df.limit(0)
    return bronze_df.select(from_json(col("record"), ddl).alias("_r")).select("_r.*")


def flatten_json(df, max_depth=5):
    for _ in range(max_depth):
        nested = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        if not nested:
            break
        selected = []
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                for child in field.dataType.names:
                    selected.append(col(field.name + "." + child).alias(field.name + "_" + child))
            else:
                selected.append(col(field.name))
        df = df.select(selected)
    return df


def standardize_schema(df):
    for name in df.columns:
        clean = name.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "_")
        if clean != name:
            df = df.withColumnRenamed(name, clean)
    return df


def deduplicate(df, primary_key, order_column=None):
    if not primary_key:
        return df.dropDuplicates()

    keys = primary_key if isinstance(primary_key, list) else [primary_key]
    keys = [k for k in keys if k in df.columns]
    if not keys:
        return df.dropDuplicates()

    if order_column and order_column in df.columns:
        window = Window.partitionBy(*keys).orderBy(col(order_column).desc())
        return df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")

    return df.dropDuplicates(keys)


def merge_silver(spark, df, table, primary_key):
    keys = primary_key if isinstance(primary_key, list) else [primary_key]
    keys = [k for k in keys if k in df.columns]

    if not keys or not spark.catalog.tableExists(table):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        return df.count()

    view = "_stage_" + table.replace(".", "_")
    df.createOrReplaceTempView(view)
    condition = " AND ".join(["t." + k + " = s." + k for k in keys])
    body = (
        " INTO " + table + " t USING " + view + " s ON " + condition + " "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    try:
        spark.sql("MERGE WITH SCHEMA EVOLUTION" + body)
    except Exception:
        spark.sql("MERGE" + body)
    return df.count()


def flattened_name(column):
    return column.replace(".", "_").lower() if column else None


def transform_source(spark, bronze_df, source, silver_table):
    df = parse_bronze(spark, bronze_df)
    df = flatten_json(df)
    df = standardize_schema(df)
    df = deduplicate(df, source.get("primary_key"), flattened_name(source.get("incremental_column")))
    return merge_silver(spark, df, silver_table, source.get("primary_key"))
