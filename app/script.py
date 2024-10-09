from pyhive import hive # type: ignore
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, year, hour, avg, to_date, current_date, date_format, datediff, count, sum, abs

spark = SparkSession.builder \
    .appName("Analyse sismiques") \
    .getOrCreate()

hive_host = 'hive-server'
hive_port = 10000
hive_database = 'data'
conn = hive.Connection(
	host=hive_host,
	port=hive_port,
	database=hive_database
)
cursor = conn.cursor()


cursor.close()
conn.close()
spark.stop()