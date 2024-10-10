from flask import Flask, request, jsonify # type: ignore

from kafka import KafkaProducer # type: ignore
from marshmallow import Schema, fields, ValidationError # type: ignore
import json
import requests
import time
import datetime
app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



@app.route('/predict', methods=['POST'])
def get_prediction():
    
    json_data = request.get_json()

    if not json_data:
        return jsonify({"message": "No input data provided"}), 400

    try:
        data = json_data
        current_time = datetime.datetime.now()
        start_time = current_time - datetime.timedelta(days= 1000)
        current_timestamp_unix = int(current_time.timestamp() * 1000)
        start_timestamp_unix = int(start_time.timestamp() * 1000) 

        params = {
            "symbol": "BTCEUR",
            "interval": "1d",
            "startTime": start_timestamp_unix,
            "endTime": current_timestamp_unix,
            "limit": 1500
        }
        response = requests.get("https://api.binance.com/api/v3/klines",params)
    except ValidationError as err:
        return jsonify(err.messages), 422

    result = response.json()
    producer.send('topic1', result)
    producer.flush()
    return jsonify({"message": "User data received and sent to Kafka","data-length": len(result),"data": result}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5550)
