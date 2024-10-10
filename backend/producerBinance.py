# -*- coding: utf-8 -*-
import requests
import json
import time
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BROKER_URL = 'kafka:9092'
KAFKA_TOPIC = 'crypto_prices'
KAFKA_SELECTION_TOPIC = 'crypto_selection'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    KAFKA_SELECTION_TOPIC,
    bootstrap_servers=KAFKA_BROKER_URL,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Initialiser symbol à None pour ne pas démarrer les requêtes immédiatement
symbol = None

def get_crypto_prices():
    base_url = f'https://api.binance.com/api/v3/ticker/price?symbol={symbol}'
    try:
        response = requests.get(base_url)
        if response.status_code == 200:
            data = response.json()
            price = data.get('price')
            timestamp = time.time()
            return {'symbol': symbol, 'price': float(price), 'timestamp': timestamp}
        else:
            print(f"Erreur lors de la récupération des données : {response.text}")
            return None
    except Exception as e:
        print(f"Exception lors de la récupération des données : {str(e)}")
        return None

def listen_for_symbol_change():
    global symbol
    for msg in consumer:
        message = msg.value
        new_symbol = message.get('symbol')
        if new_symbol and new_symbol != symbol:
            print(f"Changement de crypto suivi : {new_symbol}")
            symbol = new_symbol

def produce_data():
    print("En attente de la sélection d'une cryptomonnaie...")
    while True:
        if symbol is not None:
            data = get_crypto_prices()
            if data:
                print(f"Envoi des données à Kafka : {data}")
                producer.send(KAFKA_TOPIC, value=data)
        else:
            print("Aucune cryptomonnaie sélectionnée. En attente de sélection...")
        time.sleep(5)

if __name__ == "__main__":
    # Lancer la consommation des changements de symbole dans un thread
    import threading
    threading.Thread(target=listen_for_symbol_change).start()
    produce_data()
