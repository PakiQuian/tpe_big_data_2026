"""Shared pytest fixtures: a session-scoped local SparkSession for transform tests."""

import os

import pytest


@pytest.fixture(scope="session")
def spark():
    # Spark needs Java 17/21; system Java may be newer. Point at the project JDK
    # unless the caller already set JAVA_HOME.
    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-21-temurin-jdk")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("cpa-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
