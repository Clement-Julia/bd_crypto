from flask import Flask, jsonify, render_template, request
from confluent_kafka import Consumer, Producer, KafkaException
import json
import threading

app = Flask(__name__)

# Configuration de Kafka
KAFKA_BROKER_URL = 'kafka:9092'
KAFKA_TOPIC = 'crypto_prices'
KAFKA_SELECTION_TOPIC = 'crypto_selection'
GROUP_ID = 'crypto_prices_group'
BUFFER_SIZE = 10  # Nombre de messages à conserver dans le buffer

# Buffer pour stocker les derniers messages reçus de Kafka
buffer = []
buffer_lock = threading.Lock()

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER_URL,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
})
consumer.subscribe([KAFKA_TOPIC])

producer = Producer({'bootstrap.servers': KAFKA_BROKER_URL})

# Thread pour lire les messages de Kafka en continu et les stocker dans le buffer
def kafka_listener():
    global buffer
    while True:
        msg = consumer.poll(timeout=5.0)
        if msg and not msg.error():
            data = json.loads(msg.value().decode('utf-8'))
            with buffer_lock:
                buffer.append(data)
                if len(buffer) > BUFFER_SIZE:
                    buffer.pop(0)

threading.Thread(target=kafka_listener, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/irt_chart/<symbol>')
def irt_chart(symbol):
    producer.produce(KAFKA_SELECTION_TOPIC, json.dumps({'symbol': symbol + 'EUR'}))
    producer.flush()
    return render_template('irt_chart.html')

@app.route('/crypto_prices')
def get_latest_price():
    with buffer_lock:
        if buffer:
            # Retourne le dernier message disponible du buffer
            return jsonify(buffer[-1])
    return '', 204

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
