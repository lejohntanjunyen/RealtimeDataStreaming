import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, from_json


def setup_spark_connection(appname):
    logging.info("[INFO] Creating Spark Session")
    return SparkSession.builder \
            .appName(appname) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()
            
            
def spark_stream_message(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "customerInfo") \
        .load()
        

def create_schema():
    return StructType([
        StructField("firstName", StringType(), False),
        StructField("lastName", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("dob", TimestampType(), False),
        StructField("age", StringType(), False),
        StructField("address", StringType(), False),
        StructField("coordinates", StringType(), False),
        StructField("email", StringType(), False),
        StructField("contact", StringType(), False),
        StructField("contact2", StringType(), False),
        StructField("accountOpenedDate", TimestampType(), False),
        StructField("accountOpenedAge", StringType(), False),
        StructField("userId", StringType(), False),
        StructField("userName", StringType(), False),
        StructField("password", StringType(), False),
        StructField("profilePicture", StringType(), False)
    ])
    

def create_spark_df(sparkDF):
    schema = create_schema()
    transformedDF = sparkDF.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return transformedDF


