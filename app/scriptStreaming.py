from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, StringType
import json

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.master", "local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def convertir_data(valeur):
    if isinstance(valeur, int) or isinstance(valeur, float):
        return valeur 
    
    if isinstance(valeur, str) and valeur.isdigit():
        return int(valeur)
    
    try:
        return float(valeur)
    except ValueError:
        return valeur 
    
# itère sur le tableau 
def convertir_donnee_recu(ligne_json):
    try:
        liste_donnees = json.loads(ligne_json)
        return [[str(convertir_data(item)) for item in sous_liste] for sous_liste in liste_donnees]
    except json.JSONDecodeError:  
        return []

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic1") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

convertir_udf = udf(convertir_donnee_recu, ArrayType(ArrayType(StringType())))
df_converti = df.withColumn("valeurs_converties", convertir_udf(col("value")))
df_exploded = df_converti.select(explode(col("valeurs_converties")).alias("valeurs_individuelles"))

df_final = df_exploded.select(
    col("valeurs_individuelles")[0].alias("Timestamp"),
    col("valeurs_individuelles")[1].alias("Open"),
    col("valeurs_individuelles")[2].alias("High"),
    col("valeurs_individuelles")[3].alias("Low"),
    col("valeurs_individuelles")[4].alias("Close"),
    col("valeurs_individuelles")[5].alias("Volume")
)
query = df_final.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()