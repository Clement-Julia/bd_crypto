import requests
from flask import Flask, jsonify, render_template, request
from pyspark.sql import SparkSession
import json

app = Flask(__name__)

# Fonction pour récupérer les 50 premières cryptos de Binance
def get_top_50_cryptos_from_binance():
    api_url = 'https://api.binance.com/api/v3/ticker/24hr'
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()

        # Filtrer les paires qui se tradent contre USDT
        usdt_pairs = [item for item in data if item['symbol'].endswith('USDT')]

        # Trier par volume de trading (décroissant)
        usdt_pairs_sorted = sorted(usdt_pairs, key=lambda x: float(x['quoteVolume']), reverse=True)

        # Récupérer les 50 premières cryptomonnaies
        top_50_cryptos = [
            {
                "symbol": item['symbol'].replace('USDT', ''),  # Récupérer seulement le symbole sans USDT
                "name": item['symbol']
            } for item in usdt_pairs_sorted[:50]
        ]
        return top_50_cryptos
    else:
        return []

# Fonction pour lire les données d'une crypto depuis HDFS via PySpark
def read_csv_from_hdfs(crypto, currency, year):
    hdfs_path = f"hdfs://namenode:9000/data/{crypto}-{currency}_{year}.csv"
    print(f"Lecture des données depuis le chemin HDFS : {hdfs_path}")
    
    try:
        spark = SparkSession.builder \
            .appName("ReadCryptoDataFromHDFS") \
            .getOrCreate()
        
        df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
        
        # Vérifier si des lignes sont lues
        row_count = df.count()
        print(f"Nombre de lignes lues pour {crypto} en {year} : {row_count}")
        
        return df
    except Exception as e:
        print(f"Erreur lors de la lecture des fichiers CSV pour {crypto}-{currency}_{year} : {str(e)}")
        return None

def read_and_display_data(crypto, currency, start_year, end_year):
    all_dataframes = []
    for year in range(start_year, end_year + 1):
        df = read_csv_from_hdfs(crypto, currency, year)
        if df is not None and df.count() > 0:
            all_dataframes.append(df)
        else:
            print(f"Aucune donnée trouvée pour {crypto} en {year}")
    
    if all_dataframes:
        # Combiner les DataFrames
        full_df = all_dataframes[0].unionAll(*all_dataframes[1:])
        
        # Convertir les données en liste pour les renvoyer sous forme de JSON
        result = full_df.select("Timestamp", "Close").collect()
        
        # Formater les données en liste de dictionnaires pour le frontend
        data_json = [{"Timestamp": row["Timestamp"], "Close": row["Close"]} for row in result]
        return data_json

    print(f"Aucune donnée combinée pour {crypto} de {start_year} à {end_year}")
    return []

@app.route('/')
def index():
    cryptos = get_top_50_cryptos_from_binance()  # Récupérer la liste des cryptos
    return render_template('index.html', cryptos=cryptos)

@app.route('/data/<crypto>', methods=['POST'])
def receive_crypto_data(crypto):
    try:
        # Récupérer les données JSON envoyées dans la requête POST
        data = request.get_json()

        # Si des données sont reçues, les afficher
        if data:
            print(f"Réception des données pour {crypto} : {data}")
        else:
            print(f"Aucune donnée reçue pour {crypto}")

        # Retourner une réponse de succès
        return jsonify({"message": "Données reçues avec succès"}), 200

    except Exception as e:
        # En cas d'erreur, renvoyer un message d'erreur au frontend
        print(f"Erreur lors du traitement des données : {str(e)}")
        return jsonify({"error": str(e)}), 500
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
