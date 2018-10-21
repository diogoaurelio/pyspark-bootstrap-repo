import configparser
from configparser import ConfigParser
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame


def read_config_file(config_filepath):
    config = configparser.ConfigParser()
    config.read(config_filepath)
    return config


def add_local_s3_conf(config: ConfigParser, spark: SparkSession) -> SparkSession:
    """ only useful for local dev with S3 docker """

    if all((config["SPARK"].get('fs.s3a.endpoint'),
            config["SPARK"].get('fs.s3a.access.key'),
            config["SPARK"].get('fs.s3a.secret.key'))):
        spark.sparkContext.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3.impl',
                                                          'org.apache.hadoop.fs.s3a.S3AFileSystem')
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.endpoint',
                                                          config["SPARK"].get('fs.s3a.endpoint'))
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.access.key',
                                                          config["SPARK"].get('fs.s3a.access.key'))
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key',
                                                          config["SPARK"].get('fs.s3a.secret.key'))
    return spark


def get_spark_context(config, app_name=None):
    """ get or create a spark session based on parameters given in <config>"""
    spark_master = config["SPARK"]["master"]
    spark_conf = (
        SparkConf()
            .setMaster(spark_master)
            .set('spark.executor.memory', config["SPARK"]["executor-memory"])
            .set('spark.driver.memory', config["SPARK"]["driver-memory"])
            .setAppName(app_name)
    )
    # get or create spark session
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    # Just report errors, no warnings.
    spark.sparkContext.setLogLevel('ERROR')
    return spark