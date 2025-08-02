from flask import Flask, jsonify, request
from prophet.serialize import model_from_json
import sqlite3
import pandas as pd
import os

app = Flask(__name__)

MODEL_DIR = 'models'

@app.route('/data')
def get_data():
    # Serve raw data from the trends database
    conn = sqlite3.connect('trends_stream.db')
    df = pd.read_sql_query("SELECT * FROM trends", conn)
    conn.close()
    return df.to_json(orient='records')

@app.route('/prophet/<keyword>')
def get_forecast(keyword):
    # Serve Prophet forecast for a given keyword

    model_path = os.path.join(MODEL_DIR, f'prophet_{keyword}.json')

    if not os.path.exists(model_path):
        return jsonify({"error": f"Model for '{keyword}' not found."}), 404

    # Load model
    with open(model_path, 'r') as f:
        model = model_from_json(f.read())

    # Get number of periods (default = 48 hours)
    periods = int(request.args.get('periods', 48))

    # Make forecast
    future = model.make_future_dataframe(periods=periods, freq='H')
    forecast = model.predict(future)

    # Only return forecasted future points
    forecast = forecast[forecast['ds'] > pd.Timestamp.now()]

    # Return key columns
    forecast_output = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

    return forecast_output.to_json(orient='records')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

