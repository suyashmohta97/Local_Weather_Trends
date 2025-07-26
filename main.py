print("Running Weather ETL...")


import requests
import json
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherETL:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.dataset_id = os.getenv('BIGQUERY_DATASET')
        self.table_id = os.getenv('BIGQUERY_TABLE')
        
        # Initialize BigQuery client
        self.client = bigquery.Client(project=self.project_id)
        
        # Cities to track
        self.cities = [
            {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
            {'name': 'London', 'lat': 51.5074, 'lon': -0.1278},
            {'name': 'Tokyo', 'lat': 35.6762, 'lon': 139.6503},
            {'name': 'Sydney', 'lat': -33.8688, 'lon': 151.2093}
        ]
    
        
    def extract_weather_data(self, city):
        base_url = "https://api.openweathermap.org/data/2.5/forecast"
        params = {
            'lat': city['lat'],
            'lon': city['lon'],
            'appid': self.api_key,
            'units': 'metric'
        }
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            forecast_list = data.get('list', [])
            city_info = data.get('city', {})
            return forecast_list, city_info
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {city['name']}: {e}")
            return [], {}



    
    
        

    def transform_weather_data(self, forecast_data, city_name, city, city_info):
        try:
            transformed_data = {
                'city_name': city_name,
                'country': city_info.get('country', ''),
                'latitude': city['lat'],
                'longitude': city['lon'],
                'timestamp': forecast_data['dt_txt'],  # forecast datetime string
                'weather_main': forecast_data['weather'][0]['main'],
                'weather_description': forecast_data['weather'][0]['description'],
                'temperature': forecast_data['main']['temp'],
                'feels_like': forecast_data['main']['feels_like'],
                'temp_min': forecast_data['main']['temp_min'],
                'temp_max': forecast_data['main']['temp_max'],
                'pressure': float(forecast_data['main']['pressure']),
                'humidity': float(forecast_data['main']['humidity']),
                'visibility': float(forecast_data.get('visibility', None)),
                'wind_speed': float(forecast_data.get('wind', {}).get('speed', 0)),
                'wind_direction': float(forecast_data.get('wind', {}).get('deg', 0)),
                'cloudiness': float(forecast_data.get('clouds', {}).get('all', 0)),
                'sunrise': datetime.fromtimestamp(city_info.get('sunrise', 0), timezone.utc).isoformat() if city_info.get('sunrise') else None,
                'sunset': datetime.fromtimestamp(city_info.get('sunset', 0), timezone.utc).isoformat() if city_info.get('sunset') else None,
            }
            return transformed_data
        except KeyError as e:
            logger.error(f"Error transforming data for {city_name}: Missing key {e}")
            return None


    
    def create_table_if_not_exists(self):
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {self.dataset_id}.{self.table_id} already exists")
        except Exception:
            schema = [
                bigquery.SchemaField("city_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("latitude", "FLOAT"),
                bigquery.SchemaField("longitude", "FLOAT"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("weather_main", "STRING"),
                bigquery.SchemaField("weather_description", "STRING"),
                bigquery.SchemaField("temperature", "FLOAT"),
                bigquery.SchemaField("feels_like", "FLOAT"),
                bigquery.SchemaField("temp_min", "FLOAT"),
                bigquery.SchemaField("temp_max", "FLOAT"),
                bigquery.SchemaField("pressure", "FLOAT"),
                bigquery.SchemaField("humidity", "FLOAT"),
                bigquery.SchemaField("visibility", "FLOAT"),
                bigquery.SchemaField("wind_speed", "FLOAT"),
                bigquery.SchemaField("wind_direction", "FLOAT"),
                bigquery.SchemaField("cloudiness", "FLOAT"),
                bigquery.SchemaField("sunrise", "TIMESTAMP"),
                bigquery.SchemaField("sunset", "TIMESTAMP"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            logger.info(f"Created table {self.dataset_id}.{self.table_id}")
    
    def load_to_bigquery(self, data_list):
        if not data_list:
            logger.warning("No data to load")
            return
        df = pd.DataFrame(data_list)

        timestamp_cols = ['timestamp','sunrise','sunset']
        for i in timestamp_cols:
            df[i] = pd.to_datetime(df[i])

        # print(df.dtypes)
        # print(df.head(5))

        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=False
        )
        try:
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            logger.info(f"Loaded {len(data_list)} rows to {self.dataset_id}.{self.table_id}")
        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {e}")
    
    

    def run_etl_pipeline(self):
        logger.info("Starting Weather ETL Pipeline")
        self.create_table_if_not_exists()
        transformed_data = []
        for city in self.cities:
            logger.info(f"Processing weather data for {city['name']}")
            forecast_list, city_info = self.extract_weather_data(city)
            for forecast_data in forecast_list:
                transformed = self.transform_weather_data(forecast_data, city['name'], city, city_info)
                if transformed:
                    transformed_data.append(transformed)
        self.load_to_bigquery(transformed_data)
        logger.info("ETL Pipeline completed successfully")


def main():
    etl = WeatherETL()
    etl.run_etl_pipeline()

if __name__ == "__main__":
    main()
