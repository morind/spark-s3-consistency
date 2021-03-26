#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with

from __future__ import print_function

from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
import logging

if __name__ == "__main__":
    # create a SparkSession
    spark = SparkSession \
        .builder \
        .appName("tableCount") \
        .config("spark.hadoop.fs.s3a.access.key", "99999999999999999999999999999999") \
        .config("spark.hadoop.fs.s3a.secret.key", "99999999999999999999999999999999") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.XXXXX.ovh.net") \
        .config("spark.hadoop.parquet.block.size", "33554432") \
        .getOrCreate()

    importFile = spark.read.json("s3a://testconsistencyinput/*.json")

    importFile.printSchema()

    importFileCount = importFile.count()

    importFile \
        .write \
        .mode("OverWrite") \
        .option("compression", "gzip") \
        .parquet("s3a://testconsistencyoutput/output/homo_sapiens")

    imported = spark.read.parquet("s3a://testconsistencyoutput/output/homo_sapiens")
    importedCount = imported.count()

    logging.warning("Nombre de lignes lues : %d, nombre de lignes reellement ecrites : %d", importFileCount, importedCount)

    # very important: stop the current session
    spark.stop()
