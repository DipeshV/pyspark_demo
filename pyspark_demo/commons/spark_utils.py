#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
"""
from pypchutils.generic import create_logger
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

logger = create_logger(__name__, level="info")


def write_into_hive(data: DataFrame):
    pass

