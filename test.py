from pyspark.sql import *

spark=SparkSession.builder \
    .appName("Hello World") \
    .master("local[3]") \
    .getOrCreate()


data_list=[('nitin' , 29),
            ('rahul' , 29),
            ('rohan' , 37)]
df=spark.createDataFrame(data_list).toDF('Name' ,'Age')

df.show()