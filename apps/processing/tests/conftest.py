"""Shared pytest fixtures for apps/processing tests."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("pytest-processing")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
