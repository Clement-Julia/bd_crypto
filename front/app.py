from flask import Flask, jsonify, render_template
from confluent_kafka import Consumer, KafkaException
import json

app = Flask(__name__)

consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'crypto_prices_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['crypto_prices'])

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/crypto_prices')
def get_latest_price():
    try:
        msg = consumer.poll(timeout=2.0)  # Augmenter le timeout pour permettre à Kafka de récupérer un message
        if msg is None:
            return '', 204  # Retourner un statut 204 sans corps si aucun message n'est reçu
        if msg.error():
            raise KafkaException(msg.error())
        data = json.loads(msg.value().decode('utf-8'))
        return jsonify(data)
    except Exception as e:
        print(f"ERREUR : {str(e)}")
        return jsonify({'error': str(e)}), 500



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

