import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dlt
from dlt.common.runtime.run_context import RunContext, switched_run_context
from dlt.sources.helpers.requests import Client, RequestException
from dlt.sources.helpers.rest_client import RESTClient

MEASUREMENT_IDS = [1001, 1004, 2001, 2004]
SELECTED_PROBE_IDS = [
    # Châu Á — 50 probe
    19967, 25828, 26671, 33849, 35547, 52594, 53609, 60014, 60031, 60433,
    60869, 61078, 61304, 61424, 62917, 64078, 64363, 64689, 64821, 64957,
    65847, 1000245, 1001118, 1001967, 1002729, 1003381, 1004047, 1005043, 1007644, 1009130,
    1010225, 1010441, 1010780, 1010910, 1011228, 1013524, 1013622, 1013750, 1013949, 1014215,
    1015936, 1016036, 1016804, 1017614, 1018110, 1018267, 1018393, 1018464, 1018530, 1018717,
    # Châu Âu — 40 probe
    12601, 12701, 16991, 21452, 24523, 27102, 27351, 28273, 29377, 50732,
    52991, 53797, 60983, 61006, 61388, 62980, 1000249, 1002205, 1002874, 1003452,
    1005433, 1006074, 1008086, 1009762, 1009887, 1010169, 1010752, 1010972, 1011852, 1012128,
    1013058, 1014077, 1014506, 1014701, 1015581, 1016407, 1018086, 1018600, 1018633, 1019004,
    # Bắc Mỹ — 30 probe
    12093, 13654, 18838, 20833, 21112, 27558, 35641, 51102, 52929, 54276,
    60829, 61062, 62083, 1000755, 1000978, 1004665, 1007773, 1008285, 1008836, 1009477,
    1010875, 1011683, 1015886, 1017123, 1017126, 1017131, 1017139, 1017735, 1018126, 1018546,
    # Mỹ Latinh và Caribbean — 30 probe
    12734, 16735, 18923, 30831, 35556, 53601, 60070, 60598, 64166, 64218,
    65007, 65473, 65754, 65842, 1001252, 1003171, 1007866, 1007941, 1009618, 1010704,
    1011154, 1011626, 1011901, 1012769, 1014262, 1015501, 1015668, 1015933, 1016685, 1017133,
    # Châu Phi — 30 probe
    10243, 13758, 19414, 19925, 32925, 33751, 51544, 52065, 54325, 60525,
    60583, 60928, 61129, 62475, 65165, 65290, 65793, 1002844, 1005929, 1008539,
    1010013, 1012981, 1013611, 1015164, 1015233, 1018231, 1018673, 1018703, 1018813, 1018936,
    # Châu Đại Dương — 20 probe
    10095, 23002, 30712, 31228, 33674, 50365, 53098, 60575, 61210, 61844,
    63050, 64783, 1010528, 1014480, 1014577, 1015134, 1015838, 1015851, 1016956, 1017877,
]
TIMEZONE = timezone(timedelta(hours=7))

ripe_atlas_client = RESTClient(
    base_url="https://atlas.ripe.net/api/v2/",
    session=Client(
        request_timeout=(5, 30),
        request_max_attempts=5,
        request_max_retry_delay=30,
        raise_for_status=False,
    ).session,
)


def fetch_json(path: str, params: dict) -> dict:
    try:
        response = ripe_atlas_client.get(path, params=params)
        response.raise_for_status()
        return response.json()
    except (RequestException, ValueError) as exc:
        raise RuntimeError(f"RIPE Atlas request failed: {path}, params={params}") from exc


@dlt.resource(name="measurements", write_disposition="append", max_table_nesting=0)
def get_ping_measurement_metadata():
    for measurement_id in MEASUREMENT_IDS:
        metadata = fetch_json(f"measurements/{measurement_id}/", {"format": "json"})
        if not isinstance(metadata, dict) or metadata.get("id") != measurement_id:
            raise ValueError(f"Invalid metadata for measurement {measurement_id}")
        yield metadata


@dlt.resource(name="probes", write_disposition="append", max_table_nesting=0)
def get_probe_metadata():
    for offset in range(0, len(SELECTED_PROBE_IDS), 50):
        selected = SELECTED_PROBE_IDS[offset:offset + 50]
        found = set()
        page = 1
        while True:
            data = fetch_json(
                "probes/",
                {
                    "id__in": ",".join(map(str, selected)),
                    "is_public": "true",
                    "page_size": 50,
                    "page": page,
                    "format": "json",
                },
            )
            if (
                not isinstance(data, dict)
                or not isinstance(data.get("results"), list)
                or not isinstance(data.get("next"), (str, type(None)))
            ):
                raise ValueError(f"Invalid probe response on page {page}")
            for probe in data["results"]:
                probe_id = probe.get("id") if isinstance(probe, dict) else None
                if probe_id not in selected:
                    raise ValueError(f"Unexpected probe ID on page {page}: {probe_id}")
                found.add(probe_id)
                yield probe
            if not data["next"]:
                break
            page += 1
        if missing := set(selected) - found:
            raise ValueError(f"Probe IDs not returned by RIPE Atlas: {sorted(missing)}")


@dlt.resource(name="ping_results", write_disposition="append", max_table_nesting=0)
def get_ping_results(window_start: datetime, window_end: datetime):
    start = int(window_start.timestamp())
    end = int(window_end.timestamp())
    selected = set(SELECTED_PROBE_IDS)
    for measurement_id in MEASUREMENT_IDS:
        path = f"measurements/{measurement_id}/results/"
        try:
            with ripe_atlas_client.get(
                path,
                params={
                    "start": start - 1,
                    "stop": end,
                    "probe_ids": ",".join(map(str, SELECTED_PROBE_IDS)),
                    "public_only": "true",
                    "format": "txt",
                },
                stream=True,
            ) as response:
                response.raise_for_status()
                # shortcut: interrupted streams restart the hour, add resume support if hourly responses grow too large.
                for line in response.iter_lines():
                    if not line:
                        continue
                    result = json.loads(line)
                    if (
                        not isinstance(result, dict)
                        or result.get("msm_id") != measurement_id
                        or result.get("prb_id") not in selected
                        or result.get("type") != "ping"
                        or type(result.get("timestamp")) is not int
                    ):
                        raise ValueError(f"Invalid ping result for measurement {measurement_id}")
                    if start <= result["timestamp"] < end:
                        yield result
        except (RequestException, ValueError) as exc:
            raise RuntimeError(f"RIPE Atlas results failed for measurement {measurement_id}, {window_start.isoformat()}") from exc


def run_ingestion() -> None:
    with switched_run_context(RunContext(str(Path(__file__).resolve().parent))):
        pipeline = dlt.pipeline(
            pipeline_name="ripe_atlas_pipeline",
            destination="filesystem",
            dataset_name="ripe_atlas",
        )

        try:
            load_info = pipeline.run(
                [get_ping_measurement_metadata(), get_probe_metadata()],
                loader_file_format="jsonl",
            )
            load_info.raise_on_failed_jobs()
        except Exception as exc:
            raise RuntimeError("RIPE Atlas ingestion failed") from exc
        print(load_info)


def run_ping_results(window_start: datetime | None = None) -> None:
    if window_start is None:
        window_start = datetime.now(TIMEZONE).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    elif window_start.tzinfo is None:
        raise ValueError("window_start must include a UTC offset")
    window_start = window_start.astimezone(TIMEZONE)
    if window_start.minute or window_start.second or window_start.microsecond:
        raise ValueError("window_start must be aligned to an hour in UTC+7")
    window_end = window_start + timedelta(hours=1)
    if window_end > datetime.now(TIMEZONE):
        raise ValueError("The hourly window must be complete")

    with switched_run_context(RunContext(str(Path(__file__).resolve().parent))):
        pipeline = dlt.pipeline(
            pipeline_name="ripe_atlas_ping_results",
            destination=dlt.destinations.filesystem(
                layout="{table_name}/window_date={YYYY}-{MM}-{DD}/window_hour={HH}/{load_id}.{file_id}.{ext}",
                current_datetime=window_start,
            ),
            dataset_name="ripe_atlas",
        )
        try:
            load_info = pipeline.run(get_ping_results(window_start, window_end), loader_file_format="jsonl")
            load_info.raise_on_failed_jobs()
        except Exception as exc:
            raise RuntimeError(f"RIPE Atlas ping results failed for {window_start.isoformat()}") from exc
        print(load_info)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ping-results", action="store_true")
    parser.add_argument("--window-start", type=datetime.fromisoformat)
    args = parser.parse_args()
    if args.ping_results:
        run_ping_results(args.window_start)
    elif args.window_start:
        parser.error("--window-start requires --ping-results")
    else:
        run_ingestion()
