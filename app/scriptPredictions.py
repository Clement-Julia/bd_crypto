# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadCryptoDataFromHDFS") \
    .getOrCreate()

crypto = "BTC"
currency = "EUR"
hdfs_path = "hdfs://namenode:9000/data/"

years = range(2020, 2021)

def read_csv_from_hdfs(crypto, currency, year):

    file_path = "{}{}-{}_{}".format(hdfs_path, crypto, currency, year)
    print("----------------------------- 1 : Lecture des données depuis le répertoire : {} -----------------------------".format(file_path))
    
    # lecture des "part-*.csv" dans un DataFrame Spark
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        df.show(truncate=False)
        return df
    except Exception as e:
        print("Impossible de lire les fichiers CSV pour {}-{}_{} : {}".format(crypto, currency, year, str(e)))
        return None

def read_and_display_data(crypto, currency, start_year, end_year):

    all_dataframes = []
    for year in range(start_year, end_year + 1):
        df = read_csv_from_hdfs(crypto, currency, year)
        if df is not None:
            all_dataframes.append(df)

read_and_display_data(crypto, currency, 2020, 2021)

spark.stop()
