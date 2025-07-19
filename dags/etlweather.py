from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta

# Location Bojonegoro East Java
LATITUDE = '-7.1500'
LONGITUDE = '111.8816'

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

with DAG(
    dag_id='indo_weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['weather', 'ETL'],
) as dag:

    @task()
    def extract_weather_data():
        """Extract current weather from Open-Meteo API."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            print("Weather API response:", response.json())
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        """Transform API response to flat structure."""
        current = weather_data["current_weather"]
        result = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current["temperature"],
            "windspeed": current["windspeed"],
            "winddirection": current["winddirection"],
            "weathercode": current["weathercode"]
        }
        print("Transformed data:", result)
        return result

    @task()
    def load_weather_data(data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            data["latitude"],
            data["longitude"],
            data["temperature"],
            data["windspeed"],
            data["winddirection"],
            data["weathercode"]
        ))

        conn.commit()
        cursor.close()
        print("Data inserted successfully.")

    # Pipeline
    raw_data = extract_weather_data()
    transformed_data = transform_weather_data(raw_data)
    load_weather_data(transformed_data)
