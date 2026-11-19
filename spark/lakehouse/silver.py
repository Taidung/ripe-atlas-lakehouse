"""Biến đổi Bronze -> Silver (hàm thuần, test được không cần S3). Đặc tả: docs/silver-model.md."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

REPORT_TZ = "Asia/Ho_Chi_Minh"
OPEN_END = "9999-12-31 00:00:00"
EPOCH = "1970-01-01 00:00:00"
UNKNOWN_SK = -1

MEASUREMENTS = {1001: (4, "k.root-servers.net"), 1004: (4, "f.root-servers.net"),
                2001: (6, "k.root-servers.net"), 2004: (6, "f.root-servers.net")}

REPLY_SCHEMA = StructType([
    StructField("rtt", DoubleType()), StructField("x", StringType()), StructField("error", StringType()),
    StructField("dup", LongType()), StructField("ttl", LongType()), StructField("src_addr", StringType()),
])

RESULT_SCHEMA = StructType([
    StructField("msm_id", LongType()), StructField("prb_id", LongType()), StructField("timestamp", LongType()),
    StructField("af", LongType()), StructField("type", StringType()), StructField("fw", LongType()),
    StructField("mver", StringType()), StructField("lts", LongType()), StructField("dst_addr", StringType()),
    StructField("proto", StringType()), StructField("ttl", LongType()), StructField("size", LongType()),
    StructField("sent", LongType()), StructField("rcvd", LongType()), StructField("dup", LongType()),
    StructField("min", DoubleType()), StructField("avg", DoubleType()), StructField("max", DoubleType()),
    StructField("stored_timestamp", LongType()), StructField("result", ArrayType(REPLY_SCHEMA)),
])

PROBE_SCHEMA = StructType([
    StructField("id", LongType()), StructField("country_code", StringType()),
    StructField("asn_v4", LongType()), StructField("asn_v6", LongType()),
    StructField("prefix_v4", StringType()), StructField("prefix_v6", StringType()),
    StructField("is_public", BooleanType()), StructField("is_anchor", BooleanType()),
    StructField("geometry", StructType([StructField("coordinates", ArrayType(DoubleType()))])),
])

MEASUREMENT_SCHEMA = StructType([
    StructField("id", LongType()), StructField("af", LongType()), StructField("target", StringType()),
    StructField("target_ip", StringType()), StructField("interval", LongType()), StructField("packets", LongType()),
    StructField("size", LongType()), StructField("is_public", BooleanType()), StructField("type", StringType()),
    StructField("status", StructType([StructField("name", StringType())])),
])

PROBE_TRACKED = ["country_code", "asn_v4", "asn_v6", "prefix_v4", "prefix_v6", "is_public", "is_anchor", "latitude", "longitude"]
MEASUREMENT_TRACKED = ["af", "target", "target_ip", "interval_s", "packets", "packet_size", "is_public", "status_name", "msm_type"]


def local_ts(col: str | F.Column) -> F.Column:
    return F.from_utc_timestamp(col, REPORT_TZ)


# ---------------------------------------------------------------------------- dimensions
def probe_snapshots(raw: DataFrame) -> DataFrame:
    p = F.from_json("raw_json", PROBE_SCHEMA)
    return (raw.filter(F.col("fetch_status") == "ok")
            .withColumn("p", p)
            .select(
                F.col("probe_id"), F.col("observed_at"),
                F.col("p.country_code").alias("country_code"), F.col("p.asn_v4").alias("asn_v4"),
                F.col("p.asn_v6").alias("asn_v6"), F.col("p.prefix_v4").alias("prefix_v4"),
                F.col("p.prefix_v6").alias("prefix_v6"), F.col("p.is_public").alias("is_public"),
                F.col("p.is_anchor").alias("is_anchor"),
                F.round(F.col("p.geometry.coordinates")[1], 4).alias("latitude"),
                F.round(F.col("p.geometry.coordinates")[0], 4).alias("longitude"),
            ))


def measurement_snapshots(raw: DataFrame) -> DataFrame:
    m = F.from_json("raw_json", MEASUREMENT_SCHEMA)
    return (raw.filter(F.col("fetch_status") == "ok").withColumn("m", m).select(
        F.col("measurement_id"), F.col("observed_at"),
        F.col("m.af").cast("int").alias("af"), F.col("m.target").alias("target"),
        F.col("m.target_ip").alias("target_ip"), F.col("m.interval").cast("int").alias("interval_s"),
        F.col("m.packets").cast("int").alias("packets"), F.col("m.size").cast("int").alias("packet_size"),
        F.col("m.is_public").alias("is_public"), F.col("m.status.name").alias("status_name"),
        F.col("m.type").alias("msm_type"),
    ))


def build_scd2(snapshots: DataFrame, key: str, tracked: list[str], entity: str, sk: str) -> DataFrame:
    """Gộp snapshot thành phiên bản SCD2. Snapshot trùng thuộc tính không sinh version mới."""
    snap = snapshots.withColumn("attr_hash", F.sha2(F.to_json(F.struct(*tracked)), 256))
    order = Window.partitionBy(key).orderBy("observed_at", "attr_hash")
    snap = (snap
            .withColumn("prev_hash", F.lag("attr_hash").over(order))
            .withColumn("prev_observed_at", F.lag("observed_at").over(order))
            .withColumn("is_change", F.col("prev_hash").isNull() | (F.col("prev_hash") != F.col("attr_hash")))
            .withColumn("version_no", F.sum(F.col("is_change").cast("int")).over(
                order.rowsBetween(Window.unboundedPreceding, Window.currentRow))))
    versions = snap.groupBy(key, "version_no").agg(
        F.min("observed_at").alias("valid_from"),
        F.max("observed_at").alias("last_observed_at"),
        F.count("*").alias("observation_count"),
        F.first("attr_hash").alias("attr_hash"),
        F.min(F.when(F.col("is_change"), F.col("prev_observed_at"))).alias("prev_version_last_observed_at"),
        *[F.first(c, ignorenulls=False).alias(c) for c in tracked],
    )
    nxt = Window.partitionBy(key).orderBy("version_no")
    open_end = F.to_timestamp(F.lit(OPEN_END))
    return (versions
            .withColumn("valid_to", F.coalesce(F.lead("valid_from").over(nxt), open_end))
            .withColumn("is_current", F.col("valid_to") == open_end)
            .withColumn("join_from", F.when(F.col("version_no") == 1, F.to_timestamp(F.lit(EPOCH))).otherwise(F.col("valid_from")))
            .withColumn("valid_from_basis", F.lit("observed_at"))
            .withColumn(sk, F.xxhash64(F.concat_ws("|", F.lit(entity), F.col(key).cast("string"),
                                                   F.date_format("valid_from", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))))
            .withColumn("is_unknown_member", F.lit(False)))


def with_unknown_member(spark: SparkSession, dim: DataFrame, key: str, sk: str) -> DataFrame:
    row = {f.name: None for f in dim.schema.fields}
    row.update({sk: UNKNOWN_SK, key: -1, "version_no": 0, "observation_count": 0, "is_current": True,
                "is_unknown_member": True, "attr_hash": "unknown", "valid_from_basis": "unknown_member"})
    unknown = spark.createDataFrame([tuple(row[f.name] for f in dim.schema.fields)], dim.schema)
    unknown = (unknown.withColumn("valid_from", F.to_timestamp(F.lit(EPOCH)))
               .withColumn("join_from", F.to_timestamp(F.lit(EPOCH)))
               .withColumn("valid_to", F.to_timestamp(F.lit(OPEN_END))))
    return dim.unionByName(unknown)


def dim_date(spark: SparkSession, start: str = "2010-01-01", end: str = "2035-12-31") -> DataFrame:
    d = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS full_date")
    dow = F.dayofweek("full_date")  # 1 = Chủ nhật trong Spark
    return d.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"), "full_date",
        F.year("full_date").alias("year"), F.quarter("full_date").alias("quarter"), F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"), F.dayofmonth("full_date").alias("day"),
        (((dow + 5) % 7) + 1).alias("day_of_week"), F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").alias("iso_week"), dow.isin(1, 7).alias("is_weekend"),
    )


def dim_time(spark: SparkSession) -> DataFrame:
    m = spark.range(0, 1440).select(F.col("id").cast("int").alias("minute_of_day"))
    hour, minute = (F.col("minute_of_day") / 60).cast("int"), F.col("minute_of_day") % 60
    return m.select(
        (hour * 100 + minute).alias("time_key"), hour.alias("hour"), minute.alias("minute"),
        F.format_string("%02d:00", hour).alias("hour_label"),
        F.when(hour < 6, "night").when(hour < 12, "morning").when(hour < 18, "afternoon").otherwise("evening").alias("part_of_day"),
    )


# ---------------------------------------------------------------------------- facts
def select_result_versions(bronze: DataFrame) -> DataFrame:
    """Một dòng mỗi (measurement, probe, event_ts): bản fetched_at mới nhất, rồi stored_ts, rồi raw_sha256."""
    key = Window.partitionBy("measurement_id", "probe_id", "event_ts")
    order = key.orderBy(F.col("fetched_at").desc(), F.col("stored_ts").desc_nulls_last(), F.col("raw_sha256").desc())
    return (bronze.withColumn("source_version_count", F.count("*").over(key))
            .withColumn("_rn", F.row_number().over(order)).filter("_rn = 1").drop("_rn"))


def _valid(e):
    return e["rtt"].isNotNull() & (F.coalesce(e["dup"], F.lit(0)) == 0) & (e["rtt"] >= 0)


def build_fact_result(selected: DataFrame, silver_run_id: str, loaded_at) -> DataFrame:
    r = F.col("r")
    arr = F.coalesce(r["result"], F.array().cast(ArrayType(REPLY_SCHEMA)))
    valid = F.filter(arr, _valid)
    sent, rcvd = r["sent"].cast("int"), r["rcvd"].cast("int")
    msm_af = F.create_map(*[x for k, (af, _) in MEASUREMENTS.items() for x in (F.lit(k), F.lit(af))])
    msm_target = F.create_map(*[x for k, (_, t) in MEASUREMENTS.items() for x in (F.lit(k), F.lit(t))])
    local = local_ts("event_time")
    df = selected.withColumn("r", F.from_json("raw_json", RESULT_SCHEMA))
    return df.select(
        F.sha2(F.concat_ws("|", F.lit("ping_result"), F.col("measurement_id").cast("string"),
                           F.col("probe_id").cast("string"), F.col("event_ts").cast("string")), 256).alias("result_id"),
        "measurement_id",
        msm_af[F.col("measurement_id")].alias("af"),
        msm_target[F.col("measurement_id")].alias("target"),
        "probe_id", "event_ts", "event_time",
        F.date_trunc("hour", "event_time").alias("event_hour_utc"),
        F.to_date(local).alias("ictrl_dt"),
        F.date_format(local, "yyyyMMdd").cast("int").alias("date_key"),
        (F.hour(local) * 100 + F.minute(local)).alias("time_key"),
        sent.alias("packets_sent"), rcvd.alias("packets_received"),
        F.when((sent > 0) & (rcvd <= sent), sent - rcvd).alias("packets_lost"),
        r["dup"].cast("int").alias("duplicate_count_src"),
        F.when(sent.isNull() | rcvd.isNull() | (rcvd > sent) | (rcvd < 0), "inconsistent")
         .when(sent == 0, "send_error").when(rcvd == 0, "all_lost")
         .when(rcvd < sent, "partial_loss").otherwise("all_received").alias("result_status"),
        F.coalesce(rcvd > 0, F.lit(False)).alias("has_response"),
        r["min"].alias("src_rtt_min"), r["avg"].alias("src_rtt_avg"), r["max"].alias("src_rtt_max"),
        F.when(r["min"] >= 0, r["min"]).alias("rtt_min_ms"),
        F.when(r["avg"] >= 0, r["avg"]).alias("rtt_avg_ms"),
        F.when(r["max"] >= 0, r["max"]).alias("rtt_max_ms"),
        F.size(arr).alias("reply_element_count"),
        F.size(valid).alias("valid_rtt_count"),
        F.when(F.size(valid) > 0, F.aggregate(valid, F.lit(0.0), lambda acc, e: acc + e["rtt"])).alias("rtt_sum_ms"),
        F.when(F.size(valid) > 0, F.aggregate(valid, F.lit(0.0), lambda acc, e: acc + e["rtt"] * e["rtt"])).alias("rtt_sumsq_ms2"),
        F.size(F.filter(arr, lambda e: e["rtt"].isNotNull() & (F.coalesce(e["dup"], F.lit(0)) > 0))).alias("duplicate_reply_count"),
        F.size(F.filter(arr, lambda e: e["x"].isNotNull())).alias("timeout_count"),
        F.size(F.filter(arr, lambda e: e["error"].isNotNull())).alias("error_count"),
        F.size(F.filter(arr, lambda e: e["rtt"].isNull() & e["x"].isNull() & e["error"].isNull())).alias("unknown_element_count"),
        (F.size(valid) == F.coalesce(rcvd, F.lit(-1))).alias("counts_consistent"),
        r["ttl"].cast("int").alias("ttl"), r["size"].cast("int").alias("packet_size"),
        r["proto"].alias("proto"), r["dst_addr"].alias("dst_addr"),
        r["fw"].cast("int").alias("fw"), r["mver"].alias("mver"), r["lts"].alias("lts"),
        F.col("stored_ts"), (F.col("stored_ts") - F.col("event_ts")).alias("ingest_delay_s"),
        # timestamp do đồng hồ probe ghi: nghi ngờ khi probe không biết đã sync (lts = -1) hoặc
        # RIPE lưu trước cả thời điểm đo (stored_ts < event_ts). Đo 2026-11-19: 712/44.340 result, 3 probe.
        F.coalesce((r["lts"] < 0) | (F.col("stored_ts") < F.col("event_ts")), F.lit(False)).alias("clock_suspect"),
        F.col("source_version_count").cast("int").alias("source_version_count"),
        (F.col("source_version_count") > 1).alias("is_source_corrected"),
        "record_uid", "raw_sha256", "ingest_run_id", "probe_set_version",
        F.lit(silver_run_id).alias("silver_run_id"),
        F.lit(loaded_at).cast("timestamp").alias("silver_loaded_at"),
        F.col("r.result").alias("_replies"),
    )


def asof_join(facts: DataFrame, dim_probe: DataFrame, dim_measurement: DataFrame) -> DataFrame:
    """Gắn phiên bản dimension hiệu lực tại event_time. Không mất fact, không nhân bản (khoảng không chồng lấn)."""
    p = F.broadcast(dim_probe.filter(~F.col("is_unknown_member")).select(
        F.col("probe_id").alias("_pid"), "probe_sk", "join_from", "valid_from", "valid_to",
        F.col("country_code").alias("_cc"), "asn_v4", "asn_v6", F.col("is_public").alias("_pub")))
    joined = facts.join(p, (facts.probe_id == p._pid) & (facts.event_time >= p.join_from) & (facts.event_time < p.valid_to), "left")
    joined = joined.select(
        facts["*"],
        F.coalesce("probe_sk", F.lit(UNKNOWN_SK)).alias("probe_sk"),
        F.col("_cc").alias("country_code"),
        F.when(facts.af == 4, F.col("asn_v4")).otherwise(F.col("asn_v6")).alias("asn"),
        F.when(F.col("_pub") == True, "public").when(F.col("_pub") == False, "not_public")  # noqa: E712
         .otherwise("unknown").alias("public_status"),
        F.when(F.col("probe_sk").isNull(), "no_metadata")
         .when(facts.event_time < F.col("valid_from"), "assumed_from_first_snapshot")
         .otherwise("asof_snapshot").alias("metadata_basis"),
    )
    m = F.broadcast(dim_measurement.filter(~F.col("is_unknown_member")).select(
        F.col("measurement_id").alias("_mid"), F.col("measurement_sk"), F.col("join_from").alias("_mjf"), F.col("valid_to").alias("_mvt")))
    joined = joined.join(m, (joined.measurement_id == m._mid) & (joined.event_time >= m._mjf) & (joined.event_time < m._mvt), "left")
    return joined.withColumn("measurement_sk", F.coalesce("measurement_sk", F.lit(UNKNOWN_SK))).drop("_mid", "_mjf", "_mvt")


def build_fact_reply(fact_with_dims: DataFrame) -> DataFrame:
    exploded = fact_with_dims.select(
        "result_id", "measurement_id", "af", "probe_id", "probe_sk", "event_time", "event_hour_utc", "ictrl_dt",
        "date_key", "country_code", "asn", "public_status", "silver_run_id", "silver_loaded_at",
        F.posexplode(F.coalesce("_replies", F.array().cast(ArrayType(REPLY_SCHEMA)))).alias("reply_seq", "e"))
    e = F.col("e")
    reply_type = (F.when(e["error"].isNotNull(), "error").when(e["x"].isNotNull(), "timeout")
                  .when(e["rtt"].isNotNull() & (F.coalesce(e["dup"], F.lit(0)) > 0), "duplicate")
                  .when(e["rtt"].isNotNull(), "reply").otherwise("unknown"))
    return exploded.select(
        F.sha2(F.concat_ws("|", "result_id", F.col("reply_seq").cast("string")), 256).alias("reply_id"),
        "result_id", F.col("reply_seq").cast("int").alias("reply_seq"), reply_type.alias("reply_type"),
        F.when(e["rtt"].isNotNull(), e["rtt"]).alias("rtt_ms"),
        ((reply_type == "reply") & (e["rtt"] >= 0)).alias("is_valid_rtt"),
        e["ttl"].cast("int").alias("reply_ttl"), e["error"].alias("error_message"),
        "measurement_id", "af", "probe_id", "probe_sk", "event_time", "event_hour_utc", "ictrl_dt", "date_key",
        "country_code", "asn", "public_status", "silver_run_id", "silver_loaded_at",
    ).withColumn("is_valid_rtt", F.coalesce("is_valid_rtt", F.lit(False)))
