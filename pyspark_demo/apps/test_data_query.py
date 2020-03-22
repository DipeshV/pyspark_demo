#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
"""
from pypchutils.generic import create_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T, functions as F

from pyspark_demo.commons.rate_processor import RateProcessor

logger = create_logger(__name__, level="info")


def gen_test_data(spark: SparkSession, verbose: int = 1) -> DataFrame:
    """
    """
    # Create a Spark data frame
    schema = T.StructType(
        [
            T.StructField("date", T.StringType(), True),
            T.StructField("user_id", T.IntegerType(), True),
            T.StructField("user_name", T.StringType(), True),
            T.StructField("total_orders", T.IntegerType(), True),
            T.StructField("total_amount", T.FloatType(), True),
        ]
    )
    data = [
        ("2020-01-01", 1, "AA", 111, 111.11),
        ("2020-01-01", 2, "BB", 222, 222.22),
        ("2020-04-04", 1, "AA", 444, 444.44),
        ("2020-04-01", 3, "CC", 333, 333.33),
    ]
    data = spark.createDataFrame(data, schema=schema)

    proc = RateProcessor()
    proc_udf = F.udf(proc.run, T.FloatType())  # Convert a normal Python into a Spark UDF
    data = data.withColumn("rate", proc_udf("total_orders", "total_amount"))
    logger.info("Successfully added 'rate' column")

    data = data.withColumn("updated_at", F.current_timestamp().cast("string"))  # Added updated_at column
    logger.info("Successfully created the test data in Spark\n%s\n" % (data.toPandas().to_string(line_width=120)))
    return data
