import pandas as pd
import requests
from sqlalchemy import create_engine
import clickhouse_connect

def extract_data():
    # Extract from MySQL
    # TODO: Replace with your MySQL connection details
    mysql_engine = create_engine('mysql+pymysql://root:root@localhost:3306/coal_mining')
    production_logs_df = pd.read_sql('SELECT * FROM production_logs', mysql_engine)

    # Extract from CSV
    equipment_sensors_df = pd.read_csv('/app/data/equipment_sensors.csv')

    # Extract from API
    # For simplicity, fetching for a fixed date. This should be made dynamic.
    response = requests.get('https://api.open-meteo.com/v1/forecast?latitude=2.0167&longitude=117.3000&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta&start_date=2024-01-01&end_date=2024-01-01')
    weather_data = response.json()
    weather_df = pd.DataFrame(weather_data['daily'])

    # Load to ClickHouse (Bronze Layer)
    # TODO: Replace with your Clickhouse connection details
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin')

    # Create databases and tables if they don't exist
    client.command('CREATE DATABASE IF NOT EXISTS bronze')

    # For simplicity, we'll just load the dataframes into tables.
    # In a real-world scenario, you might want to store the raw data as JSON or another semi-structured format.
    client.insert_df('bronze.production_logs', production_logs_df)
    client.insert_df('bronze.equipment_sensors', equipment_sensors_df)
    client.insert_df('bronze.weather', weather_df)

    print("Data loaded to bronze layer")

if __name__ == '__main__':
    extract_data()