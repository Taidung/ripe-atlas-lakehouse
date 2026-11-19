from tests.spark_support import real_cases, spark, unique_results  # noqa: F401  (fixture dùng chung)


def pytest_configure(config):
    config.addinivalue_line("markers", "spark: chạy trong container Spark")
