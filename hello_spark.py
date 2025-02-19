import sys

from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__=="__main__":

    conf=SparkConf()
    conf=get_spark_app_config()
    spark=SparkSession.builder \
        .config(conf=conf)  \
        .getOrCreate()

    logger=Log4J(spark)

    if len(sys.argv) !=2:
        print("usage HelloSpark <filename>")
        sys.exit(-1)

    logger.info("starting helloSpark")
    # conf_out=spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    survey_df=load_survey_df(spark , sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)
    count_df=count_by_country(survey_df)
    logger.info((count_df.collect()))

    count_df.show()
    input("Press Enter")

    logger.info("finished hellospark")


