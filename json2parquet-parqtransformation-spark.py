#!/usr/bin/env python

import pyspark
from pyspark.sql import SparkSession,SQLContext

import sys
from pyspark.sql import SQLContext
spark = SparkSession.builder.appName("Basics").getOrCreate()
sc=spark.sparkContext
sqlContext = SQLContext(sc)
if len(sys.argv) != 3:
    raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]
data = sqlContext.read.json(sys.argv[1])
data.write.parquet(sys.argv[2])