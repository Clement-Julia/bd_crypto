from flask import Flask, jsonify, render_template, request
from confluent_kafka import Consumer, Producer, KafkaException
import json

app = Flask(__name__)

# Configuration de Kafka
KAFKA_BROKER_URL = 'kafka:9092'
KAFKA_TOPIC = 'crypto_prices'
KAFKA_SELECTION_TOPIC = 'crypto_selection'
GROUP_ID = 'crypto_prices_group'

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER_URL,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
})
consumer.subscribe([KAFKA_TOPIC])

producer = Producer({'bootstrap.servers': KAFKA_BROKER_URL})

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/irt_chart/<symbol>')
def irt_chart(symbol):
    # Envoyer le symbole au producer pour qu'il change la crypto suivie
    producer.produce(KAFKA_SELECTION_TOPIC, json.dumps({'symbol': symbol + 'EUR'}))
    producer.flush()
    return render_template('irt_chart.html')

@app.route('/crypto_prices')
def get_latest_price():
    symbol = request.args.get('symbol', 'BTCEUR')
    try:
        # Récupérer un message de Kafka avec un timeout de 2 secondes.
        msg = consumer.poll(timeout=5.0)  # Augmenter le timeout pour permettre de récupérer les messages disponibles.

        # Si aucun message n'est reçu, retourner un code 204 (No Content).
        if msg is None:
            print(f"Aucun message pour {symbol} reçu dans le délai imparti.")
            return '', 204

        # Vérifier si le message contient une erreur.
        if msg.error():
            print(f"Erreur Kafka : {msg.error()}")
            return jsonify({'error': str(msg.error())}), 500

        # Charger le message JSON.
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Message reçu de Kafka : {data}")

        # Vérifier si le symbole correspond à celui demandé.
        if data.get('symbol') == symbol:
            return jsonify(data)

        # Retourner un code 204 si le symbole ne correspond pas.
        return '', 204

    except KafkaException as ke:
        print(f"Erreur KafkaException : {str(ke)}")
        return jsonify({'error': str(ke)}), 500

    except Exception as e:
        print(f"ERREUR : {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
