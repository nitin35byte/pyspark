# class Log4J:
#     def __init__(self,spark):
#         log4j=spark._jvm.org.apache.log4j
#         root_clas="guru.learningjournal.spark.examples"
#         conf=spark.sparkContext.getConf()
#         app_name=conf.get("spark.app.name")
#         self.logger=log4j.LogManager.getLogger(root_clas +"." +app_name)
#
#     def warn(self , message):
#         self.logger.warn(message)
#
#     def info(self , message):
#         self.logger.info(message)
#
#     def error(self , message):
#         self.logger.error(message)
#
#     def debug(self , message):
#         self.logger.debug(message)

class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j  # Access Log4J from Spark's JVM
        root_class = "guru.learningjournal.spark.examples"  # Fixed variable name
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
