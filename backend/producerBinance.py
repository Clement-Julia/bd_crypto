# -*- coding: utf-8 -*-
import requests
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER_URL = 'kafka:9092'
KAFKA_TOPIC = 'crypto_prices'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

symbol = 'BTCEUR'
base_url = f'https://api.binance.com/api/v3/ticker/price?symbol={symbol}'

def get_crypto_prices():
    try:
        response = requests.get(base_url)
        if response.status_code == 200:
            data = response.json()
            price = data.get('price')
            timestamp = time.time()
            return {
                'symbol': symbol,
                'price': float(price),
                'timestamp': timestamp
            }
        else:
            print(f"----------------------------- Erreur lors de la récupération des données : {response.text} -----------------------------")
            return None
    except Exception as e:
        print(f"Exception lors de la récupération des données : {str(e)}")
        return None

def produce_data():
    print(f"----------------------------- Démarrage de la récupération des données pour {symbol} -----------------------------")
    while True:
        data = get_crypto_prices()
        if data:
            print(f"Envoi des données à Kafka : {data}")
            producer.send(KAFKA_TOPIC, value=data)
        time.sleep(5) 

if __name__ == "__main__":
    produce_data()
