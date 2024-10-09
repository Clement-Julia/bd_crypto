from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyhive import hive  # type: ignore

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.master", "local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

hive_host = 'hive-server'
hive_port = 10000
hive_database = 'test'

conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    database=hive_database
)

cursor = conn.cursor()

user_schema = StructType([
    StructField("id", StringType(), True),
    StructField("nom", StringType(), True),
    StructField("prenom", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic1") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")
df = df.select(from_json(col("value"), user_schema).alias("data")).select("data.*")

query = df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/kafka/checkpoint") \
    .option("path", "/tmp/kafka/output") \
    .start()

query.awaitTermination()
