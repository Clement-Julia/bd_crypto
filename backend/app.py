from flask import Flask, request, jsonify, Response # type: ignore

from kafka import KafkaProducer # type: ignore
from marshmallow import Schema, fields, ValidationError # type: ignore
import json,requests, time, datetime, base64, io
# import requests
# import time
# import datetime
# import base64
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
        data = json_data['crypto']
        current_time = datetime.datetime.now()
        start_time = current_time - datetime.timedelta(days= 1000)
        current_timestamp_unix = int(current_time.timestamp() * 1000)
        start_timestamp_unix = int(start_time.timestamp() * 1000) 

        params = {
            "symbol": data+"EUR",
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
    return jsonify({"message": "User data received and sent to Kafka","test": data,"data": result}), 200


@app.route('/get_csv', methods=['GET'])
def get_csv():
    symbol = request.args.get('symbol', default="BTC", type=str)
    year = request.args.get('year', default=datetime.datetime.now().year, type=str)
    
    hdfs_directory = "/data/{}-EUR_{}/".format(symbol, year)

    namenode_url = "http://127.0.0.1:9870/webhdfs/v1{}?op=LISTSTATUS".format(hdfs_directory)
    try:
        response = requests.get(namenode_url)
        if response.status_code != 200:
            return jsonify({"error": "Unable to list files in HDFS directory, status code: {}".format(response.status_code)}), 400
        
        file_list = response.json()['FileStatuses']['FileStatus']
        
        csv_buffer = io.StringIO()
        
        for file_info in file_list:
            file_name = file_info['pathSuffix']
            if file_name.endswith(".csv"): 
                file_url = "http://127.0.0.1:9870/webhdfs/v1{}{}?op=OPEN".format(hdfs_directory, file_name)
                file_response = requests.get(file_url)
                if file_response.status_code == 200:
                    csv_buffer.write(file_response.text) 
        
        return Response(csv_buffer.getvalue(), content_type='text/csv',
                        headers={"Content-Disposition": "attachment; filename={}_{}.csv".format(symbol, year)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5550)
