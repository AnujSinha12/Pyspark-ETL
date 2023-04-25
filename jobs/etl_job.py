from spark_etl.dependencies.spark import SparkSetup
from pyspark.sql.functions import sum
from spark_etl.logging.log_provider import Log4j


def main():
    global logger
    spark = SparkSetup.start_spark()

    logger = Log4j(spark)

    extracted_df = extract(spark)
    transformed_data = transform(spark, extracted_df)

    load(transformed_data)

    # stop spark session
    SparkSetup.stop_spark(spark)


def extract(spark):
    logger.info('extract')
    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
    spark_df = spark.read.parquet(r'/home/anuj/anuj/spark-learning-journal/spark_etl/data/yellow_tripdata_2022-01.parquet')
    return spark_df


def transform(spark, extracted_df):
    logger.info('transform')

    # selecting only required columns
    extracted_df = extracted_df.select("VendorID", "trip_distance", "fare_amount")

    extracted_df_trip_info = extracted_df.select('*').groupby('VendorID').agg(sum('trip_distance').alias('total_trip_distance'))
    extracted_df_amount_info = extracted_df.select('*').groupby('VendorID').agg(sum('fare_amount').alias('total_fare_amount'))

    # To fix join issue of ambiguity column
    extracted_df_trip_info_alias = extracted_df_trip_info.alias('spark_df_trip_info')
    extracted_df_amount_info_alias = extracted_df_amount_info.alias('spark_df_amount_info')


    extracted_joined_df = extracted_df_trip_info_alias.join(extracted_df_amount_info_alias, "VendorID")

    # calculating average fare per distance
    extracted_joined_df = extracted_joined_df.select(extracted_df_trip_info.VendorID, extracted_df_trip_info.total_trip_distance,
                                 extracted_df_amount_info.total_fare_amount, (
                                             extracted_df_trip_info.total_trip_distance / extracted_df_amount_info.total_fare_amount).alias(
            'avg_distance_fare'))


    # Converting df to table as rank without partitionby issue in spark
    extracted_joined_df.createOrReplaceTempView("JoinedTable")

    # ranking based average distance fare value
    query = """SELECT *,   
    RANK () OVER (ORDER BY avg_distance_fare) AS Rank   
    FROM JoinedTable"""

    tranformed_df = spark.sql(query)

    return tranformed_df


def load(transformed_df):
    logger.info('load')
    transformed_df.coalesce(1).write.csv(r'/home/anuj/anuj/spark-learning-journal/spark_etl/output/rank_data', mode='overwrite', header=True)


if __name__ == '__main__':
    main()
