# -*- coding: utf-8 -*-
"""
"""
import logging
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark session, persistent for the whole test session.
    """
    spark_session = (
        SparkSession.builder.appName("pytest-pyspark-local")
        .master("local")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.execution.arrow.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark_session.stop())
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)
    return spark_session


def test_spark(spark_session):
    assert isinstance(spark_session, SparkSession)
    assert "pytest-pyspark-local" in str(spark_session.sparkContext.getConf().getAll())  # app name should be correct
    assert "local" in str(spark_session.sparkContext.getConf().getAll())
