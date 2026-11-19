"""LEGACY (SCD1): ghi vào silver.legacy_*; được thay bởi silver_build.py ở giai đoạn 3. Giữ để đối chiếu."""
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, concat_ws, get_json_object, row_number, sha2, timestamp_seconds, when
from staging_to_bronze import UTC_PLUS_7

SILVER = "nessie.silver"
BRONZE = "nessie.bronze"


def latest_metadata(rows):
    rows = rows.withColumn("source_load_id", get_json_object("payload", "$._dlt_load_id"))
    order = Window.partitionBy("id").orderBy(col("source_load_id").cast("double").desc(), col("source_file").desc())
    return rows.withColumn("rank", row_number().over(order)).filter(col("rank") == 1)


def measurement_dimension(rows):
    return latest_metadata(rows).select(
        col("id").alias("measurement_id"),
        get_json_object("payload", "$.target").alias("target"),
        get_json_object("payload", "$.target_ip").alias("target_ip"),
        get_json_object("payload", "$.af").cast("int").alias("address_family"),
        get_json_object("payload", "$.interval").cast("int").alias("interval_seconds"),
        get_json_object("payload", "$.packets").cast("int").alias("packets"),
        get_json_object("payload", "$.status.name").alias("status"),
        "ingest_date", "source_load_id",
    )


def probe_dimension(rows):
    return latest_metadata(rows).select(
        col("id").alias("probe_id"),
        get_json_object("payload", "$.country_code").alias("country_code"),
        get_json_object("payload", "$.asn_v4").cast("long").alias("asn_v4"),
        get_json_object("payload", "$.asn_v6").cast("long").alias("asn_v6"),
        get_json_object("payload", "$.status.name").alias("status"),
        get_json_object("payload", "$.geometry.coordinates[0]").cast("double").alias("longitude"),
        get_json_object("payload", "$.geometry.coordinates[1]").cast("double").alias("latitude"),
        "ingest_date", "source_load_id",
    )


def ping_fact(rows):
    # shortcut: one result per measurement, probe, and second; use a richer key if RIPE emits distinct results for the same second.
    rows = rows.withColumn("source_load_id", get_json_object("payload", "$._dlt_load_id"))
    order = Window.partitionBy("msm_id", "prb_id", "timestamp").orderBy(
        col("source_load_id").cast("double").desc(), col("payload").desc()
    )
    rows = rows.withColumn("rank", row_number().over(order)).filter(col("rank") == 1)
    rows = rows.select(
        sha2(concat_ws(":", "msm_id", "prb_id", "timestamp"), 256).alias("ping_id"),
        col("msm_id").alias("measurement_id"),
        col("prb_id").alias("probe_id"),
        col("timestamp").alias("event_epoch"),
        timestamp_seconds("timestamp").alias("event_time_utc"),
        get_json_object("payload", "$.avg").cast("double").alias("avg_rtt_ms"),
        get_json_object("payload", "$.min").cast("double").alias("min_rtt_ms"),
        get_json_object("payload", "$.max").cast("double").alias("max_rtt_ms"),
        get_json_object("payload", "$.sent").cast("int").alias("packets_sent"),
        get_json_object("payload", "$.rcvd").cast("int").alias("packets_received"),
        get_json_object("payload", "$.dup").cast("int").alias("duplicate_replies"),
        "window_date", "window_hour", "source_load_id",
    )
    return rows.withColumn(
        "packet_loss_pct",
        when(col("packets_sent") > 0, 100.0 * (col("packets_sent") - col("packets_received")) / col("packets_sent")),
    )


def load_dimensions(spark: SparkSession):
    for name, transform, key in (
        ("measurements", measurement_dimension, "measurement_id"),
        ("probes", probe_dimension, "probe_id"),
    ):
        rows = transform(spark.table(f"{BRONZE}.{name}"))
        if not rows.head(1) or rows.filter(col(key).isNull() | col("source_load_id").isNull()).head(1):
            raise ValueError(f"Invalid {name} metadata")
        rows.writeTo(f"{SILVER}.legacy_dim_{name[:-1]}").using("iceberg").createOrReplace()
        print(f"Loaded {SILVER}.legacy_dim_{name[:-1]}")


def load_fact(spark: SparkSession, window_start: datetime):
    if window_start.tzinfo is None:
        raise ValueError("window_start must include a UTC offset")
    window_start = window_start.astimezone(UTC_PLUS_7)
    if window_start.minute or window_start.second or window_start.microsecond or window_start + timedelta(hours=1) > datetime.now(UTC_PLUS_7):
        raise ValueError("window_start must be the start of a completed UTC+7 hour")
    day, hour = window_start.strftime("%Y-%m-%d"), window_start.strftime("%H")
    rows = ping_fact(spark.table(f"{BRONZE}.ping_results").filter((col("window_date") == day) & (col("window_hour") == hour)))
    if not rows.head(1) or rows.filter(col("event_epoch").isNull() | col("packets_sent").isNull() | col("packets_received").isNull()).head(1):
        raise ValueError(f"Invalid ping facts for {window_start.isoformat()}")
    for key, dim in (("measurement_id", "legacy_dim_measurement"), ("probe_id", "legacy_dim_probe")):
        if rows.select(key).distinct().join(spark.table(f"{SILVER}.{dim}").select(key), key, "left_anti").head(1):
            raise ValueError(f"Missing {dim} rows for {window_start.isoformat()}")
    table = f"{SILVER}.legacy_fact_ping"
    if spark.catalog.tableExists(table):
        rows.writeTo(table).overwritePartitions()
    else:
        rows.writeTo(table).using("iceberg").partitionedBy("window_date", "window_hour").create()
    print(f"Loaded {table} for {window_start.isoformat()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-start", type=datetime.fromisoformat)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER}")
        if args.window_start is None:
            load_dimensions(spark)
        else:
            load_fact(spark, args.window_start)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
