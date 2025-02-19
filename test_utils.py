from unittest import TestCase

from pyspark.sql import SparkSession

from lib.utils import load_survey_df ,count_by_country



class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) ->None:
        cls.spark=SparkSession.builder \
                .master("local[3]") \
                .appName("HelloSparkTest") \
                .getOrCreate()


    def test_detail_loading(self):
        sample_df=load_survey_df(self.spark ,"data/sample.csv")
        result_count=sample_df.count()
        self.assertEqual(result_count , 9 , "Record count should be 9")

    def text_country_count(self):
        sample_df=load_survey_df(self.spark,"data/sample.csv")
        count_list=count_by_country(sample_df).collect()
        count_dict={}

        for row in count_list:
            count_dict[row["Country"]]=row["count"]
            self.assertEqual(count_dict["United States"] , 4 , "Count for the united states should be 4")
            self.assertEqual(count_dict["Canada"], 2, "Count for the united states should be 4")
            self.assertEqual(count_dict["United Kingdom"], 1, "Count for the united states should be 4")
