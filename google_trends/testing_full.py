import pandas as pd
import requests
import json
import time
import os
from prophet.serialize import model_from_json
from prophet import Prophet

with open('api_url.txt', 'r') as file:
    api_url = file.read().strip()

pbi_push_url = 'https://api.powerbi.com/beta/1d884f12-a0d7-42f0-8b15-3a91c853bcb5/datasets/8bbe0d00-29f9-495a-8fe5-3c5757c0649e/rows?experience=power-bi&key=aJBAmoEZBO049M11hHBCdPvuBlaIpE2PHIieS7mlVimjtO1OgYRcD9SvY6JW%2F%2BGuvZMsKT9J0Mpi0m2WHuOPJQ%3D%3D'

model_dir = 'models'

keywords = ['flu', 'cough', 'fever', 'cold', 'symptoms']

def push_to_powerbi(df):
    #Push DataFrame to Power BI in chunks of 5000 rows
    if df.empty:
        print("No data to push to Power BI.")
        return

    headers = {'Content-Type': 'application/json'}

    df['ds'] = df['ds'].astype(str)

    chunk_size = 5000
    num_chunks = (len(df) + chunk_size - 1) // chunk_size

    for i in range(num_chunks):
        chunk = df.iloc[i * chunk_size : (i + 1) * chunk_size]
        payload = chunk.to_dict(orient='records')

        try:
            response = requests.post(pbi_push_url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                print(f"Successfully pushed chunk {i+1}/{num_chunks} ({len(payload)} records)")
            else:
                print(f"Chunk {i+1} failed. Status code: {response.status_code}. Response: {response.text}")
        except Exception as e:
            print(f"Error while pushing chunk {i+1}: {e}")

while True:
    try:
        # Fetch latest raw data
        response = requests.get(api_url)
        data = response.json()
        df = pd.DataFrame(data)

        if df.empty:
            print(f"No raw data pulled at {pd.Timestamp.now()}")
            time.sleep(60)
            continue

        df['pull_time'] = pd.to_datetime(df['pull_time'], format='ISO8601')
        df['trending_time'] = pd.to_datetime(df['trending_time'], format='ISO8601')


        all_forecasts = []

        for kw in keywords:
            print(f"Forecasting for keyword: {kw}")

            try:
                model_path = os.path.join(model_dir, f'prophet_{kw}.json')
                if not os.path.exists(model_path):
                    print(f"Model for '{kw}' not found, skipping.")
                    continue

                # Load model
                with open(model_path, 'r') as f:
                    model = model_from_json(f.read())

                # Filter recent data for this keyword
                kw_df = df[df['keyword'] == kw][['trending_time', 'hits']].rename(columns={'trending_time': 'ds', 'hits': 'y'})

                if kw_df.empty:
                    print(f"No data for {kw}, skipping forecast.")
                    continue

                model.history = kw_df  

                # Predict into future
                future = model.make_future_dataframe(periods=48, freq='H')  # Predict 48h ahead
                forecast = model.predict(future)

                forecast['keyword'] = kw
                forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'keyword']]

                all_forecasts.append(forecast)

            except Exception as e:
                print(f"Error forecasting for {kw}: {e}")

        # Merge all forecasts
        if all_forecasts:
            final_forecast_df = pd.concat(all_forecasts, ignore_index=True)
            print(f"Prepared {len(final_forecast_df)} total forecast records. Pushing to Power BI...")

            push_to_powerbi(final_forecast_df)

        else:
            print(f"No forecasts generated at {pd.Timestamp.now()}")

    except Exception as e:
        print(f"Error fetching or processing data: {e}")

    time.sleep(60)
