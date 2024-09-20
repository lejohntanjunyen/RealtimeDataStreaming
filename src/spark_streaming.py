import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_utils import setup_spark_connection, spark_stream_message, create_spark_df


def main():
    # Creating a Spark connection
    spark = setup_spark_connection("SparkReadFromKafka")
    # Connecting to Kafka using Spark
    # data = spark_stream_message(spark, "localhost:9092", "customerInfo")
    spark_df = spark_stream_message(spark)
    # Creating a DataFrame from the Kafka stream
    transformedDF = create_spark_df(spark_df)
    print(transformedDF.printSchema())
    # Establishing a Cassandra connection
    
    # Creating a keyspace and table in Cassandra
    # Setting up a streaming query to write data from Kafka to Cassandra
    

if __name__ == "__main__":
    main()