import configparser
import os

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf=SparkConf()
    config=configparser.ConfigParser()

    # Construct the correct file path
    config_file_path = os.path.join(os.path.dirname(__file__), "spark.conf")

    # Read the configuration file
    config.read(config_file_path)

    config.read("spark_conf")

    if "SPARK_APP_CONFIGS" in config:
        for key, val in config.items("SPARK_APP_CONFIGS"):
            spark_conf.set(key, val)

    return spark_conf


def load_survey_df(spark ,data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema" , "true") \
        .csv(data_file)


def count_by_country(survey_df):
    return survey_df \
        .where("Age <40") \
        .select("Age", "Gender", "Country", "State") \
        .groupBy("Country") \
        .count()
