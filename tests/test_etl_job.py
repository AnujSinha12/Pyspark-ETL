import unittest
from spark_etl.dependencies.spark import SparkSetup
from pyspark.sql.types import *
from spark_etl.jobs.etl_job import transform


class SparkETLTestcase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_session = SparkSetup.start_spark()

    @classmethod
    def tearDownClass(cls) -> None:
        SparkSetup.stop_spark(cls.spark_session)

    def test_etl_transformation(self):
        print('_________test_etl_transformation________')
        input_schema = StructType([
            StructField('VendorID', IntegerType(), True),
            StructField('trip_distance', DoubleType(), True),
            StructField('fare_amount', DoubleType(), True)]
        )

        input_data = [(1, 3.8, 14.5),
                      (1, 2.1, 8.0),
                      (2, 0.97, 7.5),
                      (2, 1.09, 8.0),
                      (2, 4.3, 23.5)]

        self.spark_session.conf.set('spark.sql.repl.eagerEval.enabled', True)

        input_df = self.spark_session.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = StructType([
            StructField('VendorID', IntegerType(), True),
            StructField('total_trip_distance', DoubleType(), True),
            StructField('total_fare_amount', DoubleType(), True),
            StructField('avg_distance_fare', DoubleType(), True),
            StructField('Rank', IntegerType(), False)]
        )

        expected_data = [(2, 6.360000252723694, 39.0, 0.1630769295570178, 1),
                         (1, 5.8999998569488525, 22.5, 0.2622222158643934, 2)]

        expected_df = self.spark_session.createDataFrame(data=expected_data, schema=expected_schema)

        transformed_df = transform(self.spark_session, input_df)

        print('transformed_df = ', transformed_df)

        assert expected_df.count() == transformed_df.count()
        assert expected_df.schema == transformed_df.schema
        # assert expected_df == transformed_df
