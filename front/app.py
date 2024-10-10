from flask import Flask, jsonify, render_template
from kafka import KafkaConsumer
import json

app = Flask(__name__)

consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/crypto_price')
def get_latest_price():
    for message in consumer:
        data = message.value
        return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
