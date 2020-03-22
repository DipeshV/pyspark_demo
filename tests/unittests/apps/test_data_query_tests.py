# -*- coding: utf-8 -*-
"""
"""
import pyspark
from pyspark.sql import DataFrame
import pytest
from dashboard_gold_lake.apps import test_data_query as TAQ

pytest.mark.usefixtures("spark_session")


def test_gen_test_data(spark_session):
    assert "pytest-pyspark-local" in str(spark_session.sparkContext.getConf().getAll())  # app name
    data = TAQ.gen_test_data(spark=spark_session)
    assert isinstance(data, DataFrame)
    assert "business_id" in data.columns

    with pytest.raises(pyspark.sql.utils.AnalysisException):
        data.select("age")  # No such column

    data.select("business_id").dtypes[0] == ("business_id", "int")
