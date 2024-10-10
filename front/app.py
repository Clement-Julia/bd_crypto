from flask import Flask, render_template, jsonify
import csv

app = Flask(__name__)

@app.route('/')
def index():
    # Charger les données BTC et les passer à la page d'accueil sous forme de JSON
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
