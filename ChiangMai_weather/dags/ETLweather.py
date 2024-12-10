from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

## lattitude and longtitude of Chaing Mai, Thailand
LAT = '18.7904'
LONG = '98.9847'

POSTGRES_CONN_ID = 'postgres_default'

## API that provides your data
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner' : 'airflow',
    'start_date' : days_ago(1)
}

## Create Directed Acyclic Graph(DAG)
with DAG(
    dag_id = 'ETL_weather_proj',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
    ) as dags:

    @task()
    def EXTRACTdata():
        """extract data from opensource API"""

        #use Httphook here
        http_hook = HttpHook(http_conn_id = API_CONN_ID, method='GET')

        #build API endpoint
        """https://api.open-meteo.com/v1/forecast?latitude=18.7904&longitude=98.9847&
        current=temperature_2m,relative_humidity_2m,rain,showers,weather_code,cloud_cover,
        pressure_msl,wind_speed_10m,wind_direction_10m&timezone=Asia%2FBangkok"""
        
        endpnt = f'/v1/forecast?latitude={LAT}&longitude={LONG}&current=temperature_2m,relative_humidity_2m,rain,showers,weather_code,cloud_cover,pressure_msl,wind_speed_10m,wind_direction_10m&timezone=Asia%2FBangkok'

        #make the request via the HTTP HOOK
        response = http_hook.run(endpnt)

        if response.status_code == 200: ## which mean the http run successfully
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        

    @task()
    def TRANSFORMdata(weather_data):
        current_weather = weather_data['current']
        transformedData = {
            "latitude" : LAT,
            "longtitude" : LONG,
            "temperature" : current_weather["temperature_2m"],
            "humidity" : current_weather["relative_humidity_2m"],
            "rain": current_weather["rain"],
            "weather_code": current_weather["weather_code"],
            "cloud_cover": current_weather["cloud_cover"],
            "pressure_msl": current_weather["pressure_msl"],
            "windspeed": current_weather["wind_speed_10m"],
            "winddirection": current_weather["wind_direction_10m"]
        }
        return transformedData
    
    @task()
    def LOADdata(transformedData):
        """Push the data to Postgres db"""
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesnt exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longtitude FLOAT,
            temperature FLOAT,
            humidity FLOAT,
            rain FLOAT,
            weather_code INT,
            cloud_cover FLOAT,
            pressure_msl FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data to db
        cursor.execute("""
        INSERT INTO weather_data(latitude, longtitude, temperature, humidity, rain, weather_code,
            cloud_cover, pressure_msl, windspeed, winddirection)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,(
            transformedData['latitude'],
            transformedData['longtitude'],
            transformedData['temperature'],
            transformedData['humidity'],
            transformedData['rain'],
            transformedData['weather_code'],
            transformedData['cloud_cover'],
            transformedData['pressure_msl'],
            transformedData['windspeed'],
            transformedData['winddirection']

        ))

        conn.commit()
        cursor.close()
    
    ## DAG workflow
    weather_data = EXTRACTdata()
    transformedData = TRANSFORMdata(weather_data)
    LOADdata(transformedData)