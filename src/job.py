"""
[scope]
    ETL Spark Job

"""
import sys
from argparse import ArgumentParser
from configparser import ConfigParser
from typing import Dict, Any
from datetime import datetime as dt

from pyspark.sql import DataFrame, Row, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from src.utils.spark_utils import get_spark_context, read_config_file, add_local_s3_conf


APP_NAME: str = 'etl_spark_job'


# create the general function
def _amount_spent(quantity: int, price: float) -> float:
    """
    Calculates the product between two variables
    :param quantity: (float/int)
    :param price: (float/int)
    :return:
            (float/int)
    """
    return quantity * price


def amount_spent_udf(data: DataFrame) -> DataFrame:
    # create the general UDF
    amount_spent_udf = F.udf(_amount_spent, DoubleType())
    # Note: DoubleType in Java/Scala is equal to Python float; thus you can alternatively specify FloatType()

    # Apply our UDF to the dataframe
    return data.withColumn('amount_spent', amount_spent_udf(F.col('quantity'), F.col('price')))


def main(conf: ConfigParser, spark: SparkSession) -> None:

    # mock data
    customers = spark.createDataFrame([
        Row(customer_name="Geoffrey", date="2016-04-22", category="A", product_name="apples", quantity=1, price=50.00),
        Row(customer_name="Geoffrey", date="2016-05-03", category="B", product_name="Lamp", quantity=2, price=38.00),
        Row(customer_name="Geoffrey", date="2016-05-03", category="D", product_name="Solar Pannel", quantity=1, price=29.00),
        Row(customer_name="Geoffrey", date="2016-05-03", category="A", product_name="apples", quantity=3, price=50.00),
        Row(customer_name="Geoffrey", date="2016-05-03", category="C", product_name="Rice", quantity=5, price=15.00),
        Row(customer_name="Geoffrey", date="2016-06-05", category="A", product_name="apples", quantity=5, price=50.00),
        Row(customer_name="Geoffrey", date="2016-06-05", category="A", product_name="bananas", quantity=5, price=55.00),
        Row(customer_name="Geoffrey", date="2016-06-15", category="Y", product_name="Motor skate", quantity=7, price=68.00),
        Row(customer_name="Geoffrey", date="2016-06-15", category="E", product_name="Book: The noose", quantity=1, price=125.00),
        Row(customer_name="Yann", date="2016-04-22", category="B", product_name="Lamp", quantity=1, price=38.00),
        Row(customer_name="Yann", date="2016-05-03", category="Y", product_name="Motor skate", quantity=1, price=68.00),
        Row(customer_name="Yann", date="2016-05-03", category="D", product_name="Recycle bin", quantity=5, price=27.00),
        Row(customer_name="Yann", date="2016-05-03", category="C", product_name="Rice", quantity=15, price=15.00),
        Row(customer_name="Yann", date="2016-04-02", category="A", product_name="bananas", quantity=3, price=55.00),
        Row(customer_name="Yann", date="2016-04-02", category="B", product_name="Lamp", quantity=2, price=38.00),
        Row(customer_name="Yann", date="2016-04-03", category="E", product_name="Book: Crime and Punishment", quantity=5, price=100.00),
        Row(customer_name="Yann", date="2016-04-13", category="E", product_name="Book: The noose", quantity=5, price=125.00),
        Row(customer_name="Yann", date="2016-04-27", category="D", product_name="Solar Pannel", quantity=5, price=29.00),
        Row(customer_name="Yann", date="2016-05-27", category="D", product_name="Recycle bin", quantity=5, price=27.00),
        Row(customer_name="Yann", date="2016-05-27",  category="A", product_name="bananas", quantity=3, price=55.00),
        Row(customer_name="Yann", date="2016-05-01", category="Y", product_name="Motor skate", quantity=1, price=68.00),
        Row(customer_name="Yann", date="2016-06-07", category="Z", product_name="space ship", quantity=1, price=227.00),
        Row(customer_name="Yoshua", date="2016-02-07", category="Z", product_name="space ship", quantity=2, price=227.00),
        Row(customer_name="Yoshua", date="2016-02-14", category="A", product_name="bananas", quantity=9, price=55.00),
        Row(customer_name="Yoshua", date="2016-02-14", category="B", product_name="Lamp", quantity=2, price=38.00),
        Row(customer_name="Yoshua", date="2016-02-14", category="A", product_name="apples", quantity=10, price=55.00),
        Row(customer_name="Yoshua", date="2016-03-07", category="Z", product_name="space ship", quantity=5, price=227.00),
        Row(customer_name="Yoshua", date="2016-04-07", category="Y", product_name="Motor skate", quantity=4, price=68.00),
        Row(customer_name="Yoshua", date="2016-04-07", category="D", product_name="Recycle bin", quantity=5, price=27.00),
        Row(customer_name="Yoshua", date="2016-04-07", category="C", product_name="Rice", quantity=5, price=15.00),
        Row(customer_name="Yoshua", date="2016-04-07",category= "A", product_name="bananas", quantity=9, price=55.00),
        Row(customer_name="Jurgen", date="2016-05-01", category="Z", product_name="space ship", quantity=1, price=227.00),
        Row(customer_name="Jurgen", date="2016-05-01", category="A", product_name="bananas", quantity=5, price=55.00),
        Row(customer_name="Jurgen", date="2016-05-08", category="A", product_name="bananas", quantity=5, price=55.00),
        Row(customer_name="Jurgen", date="2016-05-08", category="Y", product_name="Motor skate", quantity=1, price=68.00),
        Row(customer_name="Jurgen", date="2016-06-05", category="A", product_name="bananas", quantity=5, price=55.00),
        Row(customer_name="Jurgen", date="2016-06-05", category="C", product_name="Rice", quantity=5, price=15.00),
        Row(customer_name="Jurgen", date="2016-06-05", category="Y", product_name="Motor skate", quantity=2, price=68.00),
        Row(customer_name="Jurgen", date="2016-06-05", category="D", product_name="Recycle bin", quantity=5, price=27.00),
    ])
    result = amount_spent_udf(data=customers)
    result.show(10)


if __name__ == "__main__":
    parser = ArgumentParser(prog=APP_NAME,
                            usage=APP_NAME,
                            description='Reads masterdata dump, dumps key tables to S3 '
                                        '& optionally generates synthetic data',
                            epilog='',
                            add_help=True)

    parser.add_argument('-c', '--config',
                        metavar='filepath',
                        help='filepath of config-file')

    argv = sys.argv[1:]
    options, args = parser.parse_known_args(argv)

    # read config-file
    config_filepath: str = options.config
    config: ConfigParser = read_config_file(config_filepath)

    job_start: dt = dt.now()
    print("Starting Spark job {} at {}".format(APP_NAME, job_start))

    spark_app_name: str = "{}_{}".format(APP_NAME, job_start)
    spark: SparkSession = get_spark_context(config=config, app_name=spark_app_name)
    # Optionally add S3 Path Spark
    spark: SparkSession = add_local_s3_conf(spark=spark, config=config)

    main(conf=config, spark=spark)

    job_finish: dt = dt.now()
    print("Finished Spark job {} at {}".format(APP_NAME, job_finish))
