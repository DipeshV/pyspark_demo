# -*- coding: utf-8 -*-
"""
"""
import pyspark
from pyspark.sql import DataFrame
import pytest
from pyspark_demo.apps import test_data_query as TAQ

pytest.mark.usefixtures("spark_session")


def test_gen_test_data(spark_session):
    
    data = TAQ.gen_test_data(spark=spark_session)
    assert isinstance(data, DataFrame)
    assert "user_id" in data.columns

    with pytest.raises(pyspark.sql.utils.AnalysisException):
        data.select("age")  # No such column

    data.select("user_id").dtypes[0] == ("user_id", "int")
