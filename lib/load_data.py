from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import pyreadr
import sys
import os

#os.environ["SPARK_HOME"] = "~/.local/"

print("import executed")
conf = SparkConf().set("spark.jars", "../databox-r/lib/trino-jdbc-393.jar") \
                  .set("spark.executor.memory", "1G") \
                  .set("spark.eventlog.enabled", "true") \
                  .set("spark.eventlog.dir", r"/app/") \
                  .set("spark.cores.max", 1) \
                  .set("spark.logConf", "true") \
                  .set("spark.ui.enabled", "false")

print(sys.argv[1])

sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
spark = SparkSession(sc).builder.appName('load_data').getOrCreate()
print("======= SPARK session successfully created =======")

source_df = spark.read.format("jdbc") \
.option("driver","io.trino.jdbc.TrinoDriver") \
.option("url", "jdbc:trino://192.168.7.221:8080") \
.option("query", sys.argv[1]) \
.option("user", 'unisoma') \
.option("password", '') \
.load()
print("======= TRINO connection established =======")

pyreadr.write_rds("~/dump.Rds", source_df.toPandas())
print("======= RDS generated =======")
