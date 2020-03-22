#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
"""
from pypchutils.generic import create_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T, functions as F

logger = create_logger(__name__, level="info")


def gen_test_data(spark: SparkSession = None) -> DataFrame:
    """
    """
    if spark is None:
        logger.info("Spark session is None, thus creating a new one...")
        spark = SparkSession.builder.master("local").appName("gen-test-data").getOrCreate()
    # Create a Spark data frame
    schema = T.StructType(
        [
            T.StructField("date", T.StringType(), True),
            T.StructField("business_id", T.IntegerType(), True),
            T.StructField("business_name", T.StringType(), True),
            T.StructField("total_receipts", T.IntegerType(), True),
            T.StructField("total_receipt_amount", T.FloatType(), True),
        ]
    )
    data = [
        ("2019-12-01", 1, "AA", 111, 111.11),
        ("2019-12-01", 2, "BB", 222, 222.22),
        ("2019-12-04", 1, "AA", 444, 444.44),
        ("2019-12-01", 3, "C'C", 333, 333.33),
    ]
    data = spark.createDataFrame(data, schema=schema)
    # assert data.isLocal(), "Data should have been created with a local Spark session"
    data = data.withColumn("updated_at", F.current_timestamp().cast("string"))  # Added updated_at column
    logger.info("Successfully created the test data in Spark")
    return data
