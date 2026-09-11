"""Shared pytest fixtures for apps/processing tests."""
import os

import pytest

# Must be set before the JVM starts (pyspark launches it as a subprocess that
# inherits this env). `spark.sql.session.timeZone` alone is not enough — it
# doesn't cover timestamp conversion on JDBC read/write, only SQL functions.
os.environ.setdefault("TZ", "UTC")

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("pytest-processing")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    yield session
    session.stop()
