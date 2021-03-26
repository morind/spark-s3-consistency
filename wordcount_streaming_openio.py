import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":

    spark = SparkSession \
    .builder \
    .appName("PythonStructuredStreamingKafkaWordCount") \
    .config("spark.hadoop.fs.s3a.access.key", "99999999999999999999999999999999") \
    .config("spark.hadoop.fs.s3a.secret.key", "99999999999999999999999999999999") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.XXXXX.ovh.net") \
    .getOrCreate()

    # Subscribe to 1 topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.bootstrap.servers", "kafka.XXXXX:9093") \
        .option("subscribe", "test") \
        .option("startingOffsets", "earliest") \
        .option("groupidprefix", "test.s3") \
        .load()
        
    # Split the lines into words
    words = df.select(
        explode(
            split(df.value, " ")
        ).alias("word")
    )

    # Generate running word count
    wordCounts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
#    .option("startingOffsets", "earliest") \
    query = wordCounts \
        .writeStream \
        .queryName("wcsink") \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()
