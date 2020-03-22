#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ETL Control Plane

# Env Variables
# export PYTHONPATH=$(pwd)

##############
# Usage
##############
job=test  #
date_range=2020-01-01,2020-01-02

python pyspark_demo/etl_controller.py --job=$job --date_range=$date_range 2>&1 | tee logs/etl_controller_job_${job}.log

"""
import json
import os
import pandas as pd
from pypchutils.generic import create_logger
from pyspark.sql import SparkSession, DataFrame

from pyspark_demo.apps.test_data_query import gen_test_data
from pyspark_demo.commons import spark_utils as SU

pd.set_option("display.max_columns", 120)
pd.set_option("precision", 4)
pd.set_option("display.precision", 4)
pd.set_option("display.width", 120)

logger = create_logger(__name__, level="info")


def get_actions(job: str = "", verbose: int = 1, **kwargs,) -> dict:
    """A list of ETL jobs and the required job parameters"""
    actions_dict = {
        "test": {
            "gen_data_fn": gen_test_data,
            "gen_data_fn_args": {},
            "save_data_fn": lambda data: None,
            "save_data_fn_args": {},
        }
    }
    actions = actions_dict[job]
    if verbose >= 2:
        logger.info("actions:\n{}".format(json.dumps(str(actions), sort_keys=True, indent=4)))
    return actions


def main(
    job: str = "", spark_executor_memory: str = "2g", spark_driver_memory: str = "2g", **kwargs,
):
    """
    Args:
        job: Job name
        **kwargs: Other arguments of a job
    """
    spark = (
        SparkSession.builder.appName(f"job-{job}")
        .config("spark.executor.memory", spark_executor_memory)
        .config("spark.driver.memory", spark_driver_memory)
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.databricks.io.cache.enabled", "true")  # Enable delta cache
        .config("spark.sql.execution.arrow.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Find the list of actions for the given job
    actions = get_actions(job=job, verbose=1, **kwargs,)
    data = actions["gen_data_fn"](spark=spark, verbose=2, **actions["gen_data_fn_args"])
    assert isinstance(data, DataFrame)

    actions["save_data_fn"](data=data, **actions["save_data_fn_args"])
    logger.info(f"Successfully ran 'etl_controller' script with job: '{job}'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--job", type=str)
    parser.add_argument("--date_range", default=",")

    # Parse the cmd line args
    args = parser.parse_args()
    args = vars(args)
    logger.info("Cmd line args:\n{}".format(json.dumps(args, sort_keys=True, indent=4)))

    main(**args)
    logger.info("ALL DONE!\n")
