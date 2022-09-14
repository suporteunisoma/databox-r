from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import pyreadr

print("import executed")
conf = SparkConf().set("spark.jars", "trino-jdbc-393.jar") \
                  .set("spark.executor.memory", "1G") \
                  .set("spark.eventlog.enabled", "true") \
                  .set("spark.eventlog.dir", r"/app/") \
                  .set("spark.cores.max", 1) \
                  .set("spark.sql.crossJoin.enabled", "True") \
                  .set("spark.logConf", "true")

print(conf)

sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
spark = SparkSession(sc).builder.appName('load_data').getOrCreate()
print("======= SPARK session successfully created =======")

source_df = spark.read.format("jdbc") \
.option("driver","io.trino.jdbc.TrinoDriver") \
.option("url", "jdbc:trino://192.168.7.221:8080/postgresql/public_dataset") \
.option("query", "select * from bi_aloca_equipe") \
.option("user", 'unisoma').option("password", '').load()
print("======= TRINO connection established =======")

#pyreadr.write_rdata("~/test.RData", source_df.toPandas(), df_name="bi_aloca_equipe")
pyreadr.write_rds("~/test.Rds", source_df.toPandas())
print("======= RDS generated =======")
