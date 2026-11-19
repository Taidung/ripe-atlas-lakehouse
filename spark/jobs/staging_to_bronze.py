import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, input_file_name, lit, regexp_extract

STAGING = "s3a://staging/ripe_atlas"
BRONZE = "nessie.bronze"
UTC_PLUS_7 = timezone(timedelta(hours=7))


def prepare_metadata(rows):
    return rows.select(
        get_json_object("value", "$.id").cast("long").alias("id"),
        regexp_extract("source_file", r"/ingest_date=(\d{4}-\d{2}-\d{2})/", 1).alias("ingest_date"),
        col("value").alias("payload"),
        "source_file",
    )


def validate_metadata(rows, name: str) -> None:
    if not rows.head(1) or rows.filter(col("id").isNull() | (col("ingest_date") == "")).head(1):
        raise ValueError(f"Invalid or empty staging data for {name}")


def load_metadata(spark: SparkSession, name: str) -> None:
    source = f"{STAGING}/{name}/ingest_date=*/*.jsonl.gz"
    rows = prepare_metadata(spark.read.text(source).withColumn("source_file", input_file_name()))
    validate_metadata(rows, name)
    # shortcut: metadata reloads all staging snapshots; use incremental loading if volume grows.
    rows.writeTo(f"{BRONZE}.{name}").using("iceberg").createOrReplace()
    print(f"Loaded {BRONZE}.{name}")


def prepare_ping_results(rows, window_start: datetime):
    day = window_start.strftime("%Y-%m-%d")
    hour = window_start.strftime("%H")
    rows = rows.select(
        get_json_object("value", "$.msm_id").cast("long").alias("msm_id"),
        get_json_object("value", "$.prb_id").cast("long").alias("prb_id"),
        get_json_object("value", "$.timestamp").cast("long").alias("timestamp"),
        get_json_object("value", "$.type").alias("type"),
        col("value").alias("payload"),
        "source_file",
    ).withColumn("window_date", lit(day)).withColumn("window_hour", lit(hour))
    window_end = window_start + timedelta(hours=1)
    invalid = (
        col("msm_id").isNull()
        | col("prb_id").isNull()
        | col("timestamp").isNull()
        | col("type").isNull()
        | (col("type") != "ping")
        | (col("timestamp") < int(window_start.timestamp()))
        | (col("timestamp") >= int(window_end.timestamp()))
    )
    if not rows.head(1) or rows.filter(invalid).head(1):
        raise ValueError(f"Invalid or empty ping results for {window_start.isoformat()}")
    return rows


def load_ping_results(spark: SparkSession, window_start: datetime) -> None:
    if window_start.tzinfo is None:
        raise ValueError("window_start must include a UTC offset")
    window_start = window_start.astimezone(UTC_PLUS_7)
    window_end = window_start + timedelta(hours=1)
    if window_start.minute or window_start.second or window_start.microsecond or window_end > datetime.now(UTC_PLUS_7):
        raise ValueError("window_start must be the start of a completed UTC+7 hour")

    day = window_start.strftime("%Y-%m-%d")
    hour = window_start.strftime("%H")
    source = f"{STAGING}/ping_results/window_date={day}/window_hour={hour}/*.jsonl.gz"
    rows = prepare_ping_results(spark.read.text(source).withColumn("source_file", input_file_name()), window_start)

    table = f"{BRONZE}.ping_results"
    if spark.catalog.tableExists(table):
        rows.writeTo(table).overwritePartitions()
    else:
        rows.writeTo(table).using("iceberg").partitionedBy("window_date", "window_hour").create()
    print(f"Loaded {table} for {window_start.isoformat()}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-start", type=datetime.fromisoformat)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("staging_to_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BRONZE}")
        if args.window_start is None:
            for name in ("measurements", "probes"):
                load_metadata(spark, name)
        else:
            load_ping_results(spark, args.window_start)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
