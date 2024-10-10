# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import requests
import json
import time

spark = SparkSession.builder \
    .appName("BinanceDataQuery") \
    .getOrCreate()

crypto = "BTC"
currency = "EUR"
base_url = "https://api.binance.com/api/v3/klines"
interval = "1d"
hdfs_path = "hdfs://namenode:9000/data/"

def date_to_millis(date_str):
    return int(time.mktime(time.strptime(date_str, "%Y-%m-%d")) * 1000)

def fetch_data(crypto, currency, start_time, end_time):
    symbol = crypto + currency
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Erreur lors de la récupération des données : {}".format(response.text))
        return []

import os

def save_hdfs(data, crypto, currency, year):
    try:
        if data:
            rdd = spark.sparkContext.parallelize(data)
            print("----------------------------- 2 : RDD créé avec {} enregistrements -----------------------------".format(rdd.count()))

            columns = [
                "Open Time", "Open", "High", "Low", "Close", "Volume",
                "Close Time", "Quote Asset Volume", "Number of Trades",
                "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"
            ]

            df = rdd.toDF(columns)
            df = df.withColumn("Open Time", (df["Open Time"] / 1000).cast("timestamp"))
            df = df.withColumn("Close Time", (df["Close Time"] / 1000).cast("timestamp"))

            # df.show(5, truncate=False)

            folder = "{}-{}_{}".format(crypto, currency, year)
            output_path = hdfs_path + folder

            print("----------------------------- 3: Chemin de sortie HDFS : {} -----------------------------".format(output_path))


            print("----------------------------- 4 : Sauvegarde du DataFrame Spark en CSV dans HDFS -----------------------------")
            df.write.csv(output_path, header=True, mode='overwrite')
            print("----------------------------- Données {}-{} pour l'année {} sauvegardées dans {} -----------------------------".format(crypto, currency, year, output_path))
        else:
            print("----------------------------- Aucune donnée disponible pour {}-{} pour l'année {} -----------------------------".format(crypto, currency, year))
    except Exception as e:
        print("----------------------------- ERREUR -----------------------------")
        print(e)


# récupérer les données par tranche d'un an
def get_save_data(crypto, currency, start_year, end_year):
    for year in range(start_year, end_year + 1):
        start_date = "{}-01-01".format(year)
        end_date = "{}-12-31".format(year)

        start_time = date_to_millis(start_date)
        end_time = date_to_millis(end_date)

        print("----------------------------- 1 : Récupération de {}/{} pour la période {} - {} -----------------------------".format(crypto, currency, start_date, end_date))

        data = fetch_data(crypto, currency, start_time, end_time)
        save_hdfs(data, crypto, currency, year)

get_save_data(crypto, currency, 2020, 2024) 

spark.stop()
