# -*- coding: utf-8 -*-

from pyhive import hive # type: ignore
from pyspark.sql import SparkSession

# Création de la session spark
spark = SparkSession.builder \
	.appName("Db data") \
	.getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/data/dataset_sismique.csv", header=True, inferSchema=True, encoding='utf-8')
df = df.withColumnRenamed("tension entre plaque", "tension")
data = df.collect()

# Connexion à la bdd
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