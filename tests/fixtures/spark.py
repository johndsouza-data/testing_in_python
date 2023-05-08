"""
pyspark configurations
"""
import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_test_ctx(request):
    """
    Create Spark Context for unit tests
    :return: SparkSession
    :rtype: pyspark.sql.SparkSession.sparkContext
    """
    if 'nosharedctx' not in request.keywords:
        return SparkSession.builder.master('local[*]').appName('unit-testing').getOrCreate().sparkContext
    return None


@pytest.fixture(scope="session")
def spark_test_ses(request):
    """
    Create Spark Session for unit tests
    :return: SparkSession
    :rtype: pyspark.sql.SparkSession.sparkContext
    """
    if 'nosharedctx' not in request.keywords:
        return SparkSession.builder.master('local[*]').config("spark.default.parallelism", 1).\
            config("spark.sql.shuffle.partitions", 2).appName('unit-testing').getOrCreate()
    return None