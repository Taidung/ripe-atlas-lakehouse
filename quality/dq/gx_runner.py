"""Chạy một bộ expectation trên Spark DataFrame qua GX Checkpoint.

GX dùng file context ở artifacts/gx: validation result và Data Docs (HTML) được lưu lại để xem sau
(artifacts/gx/gx/uncommitted/data_docs/local_site/index.html).
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame

from dq.results import CRITICAL, CheckResult, make_check

GX_ROOT = Path("/opt/app/artifacts/gx")


def run_suite(df: DataFrame, suite_name: str, expectations, run_label: str, build_docs: bool = True) -> list[CheckResult]:
    import great_expectations as gx
    from great_expectations.checkpoint import UpdateDataDocsAction

    GX_ROOT.mkdir(parents=True, exist_ok=True)
    context = gx.get_context(mode="file", project_root_dir=str(GX_ROOT))

    # Data source / asset / batch definition: tạo nếu chưa có (chạy lại nhiều lần không lỗi).
    source = context.data_sources.add_or_update_spark(name="silver_spark")
    try:
        asset = source.get_asset(suite_name)
    except LookupError:
        asset = source.add_dataframe_asset(name=suite_name)
    try:
        batch_definition = asset.get_batch_definition("whole")
    except (KeyError, LookupError):
        batch_definition = asset.add_batch_definition_whole_dataframe("whole")

    suite = gx.ExpectationSuite(name=suite_name)
    for expectation, severity in expectations:
        expectation.meta = {"severity": severity}
        suite.add_expectation(expectation)
    suite = context.suites.add_or_update(suite)

    validation = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"{suite_name}_vd", data=batch_definition, suite=suite)
    )
    actions = [UpdateDataDocsAction(name="update_docs")] if build_docs else []
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=f"{suite_name}_checkpoint",
            validation_definitions=[validation],
            actions=actions,
            result_format={"result_format": "SUMMARY"},
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df}, run_id=gx.RunIdentifier(run_name=run_label))

    checks = []
    for validation_result in result.run_results.values():
        for r in validation_result.results:
            config, outcome = r.expectation_config, r.result or {}
            kwargs = {k: v for k, v in config.kwargs.items() if k != "batch_id"}
            checks.append(
                make_check(
                    name=f"{config.type}:{json.dumps(kwargs, sort_keys=True, default=str)[:180]}",
                    engine=f"gx-{gx.__version__}",
                    severity=(config.meta or {}).get("severity", CRITICAL),
                    success=r.success,
                    observed=outcome.get("observed_value"),
                    unexpected=outcome.get("unexpected_count"),
                    elements=outcome.get("element_count"),
                    details={
                        "partial_unexpected_list": outcome.get("partial_unexpected_list", [])[:10],
                        "exception": (r.exception_info or {}).get("exception_message"),
                    },
                )
            )
    return checks
