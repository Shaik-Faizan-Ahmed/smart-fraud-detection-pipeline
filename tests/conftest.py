import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("fraud-pipeline-tests")
        .master("local[*]")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def test_config():
    return {
        "gold": {
            "high_value_percentile": 0.90,
            "risky_kyc_statuses": ["pending", "rejected", "expired"],
        }
    }
