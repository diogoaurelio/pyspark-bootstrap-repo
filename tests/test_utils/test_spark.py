"""
    Utilities common to all tests using spark
"""

import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import logging


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """
        fixture for creating a spark session
    Args:
        request: pytest.FixtureRequest object
    """
    conf = SparkConf() \
        .setMaster("local[2]") \
        .setAppName("pytest-pyspark-local-testing")
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    quiet_py4j()
    return sc


@pytest.fixture(scope="session")
def spark_session(request):
    """
        fixture for creating a spark session
    Args:
        request: pytest.FixtureRequest object
    """
    spark_conf = SparkConf() \
        .setMaster("local[2]") \
        .setAppName("pytest-pyspark2.+-local-testing")
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    quiet_py4j()
    return spark
