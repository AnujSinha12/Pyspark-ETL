from pyspark.sql import SparkSession
from spark_etl.utilities.utils import get_spark_app_config


class SparkSetup:
    @classmethod
    def get_config(cls):
        cls.conf = get_spark_app_config()

    @classmethod
    def start_spark(cls):
        print('_________start_spark_________')
        # spark_builder = SparkSession.builder.master(master).appName(app_name)
        #
        # # creating spark session
        # spark_session = spark_builder.getOrCreate()
        cls.get_config()
        spark_session = SparkSession.builder.config(conf=cls.conf).getOrCreate()
        return spark_session

    @classmethod
    def stop_spark(cls, spark):
        print('____________stop_spark__________')
        spark.stop()


