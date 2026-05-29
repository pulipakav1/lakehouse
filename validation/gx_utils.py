import sys
sys.path.insert(0, "/opt/airflow")

from great_expectations.dataset import SparkDFDataset
from utils.config import get_logger

logger = get_logger("gx_utils")


def run_suite(spark_df, suite_fn, table_name):
    gdf = SparkDFDataset(spark_df)
    results = suite_fn(gdf)
    failed = []
    for r in results:
        status = "PASS" if r.success else "FAIL"
        logger.info(
            f"  [{status}] {r.expectation_config.expectation_type}"
            f" | {r.expectation_config.kwargs}"
        )
        if not r.success:
            failed.append(r)
    if failed:
        summary = "\n".join(
            f"  {f.expectation_config.expectation_type} "
            f"kwargs={f.expectation_config.kwargs} "
            f"result={f.result}"
            for f in failed
        )
        raise ValueError(
            f"Quality gate FAILED for '{table_name}' — "
            f"{len(failed)}/{len(results)} expectations failed:\n{summary}"
        )
    logger.info(f"  Quality gate PASSED for '{table_name}' — {len(results)} checks")
