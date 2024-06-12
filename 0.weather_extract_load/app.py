# import required libraries from the installed packages
import requests
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO



# Load environment variables
load_dotenv()

API_key = os.getenv('weather_api')
# Function to get the weather data through openweather API
def get_weather_data(lat, lon, API_key):
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}'
    response = requests.get(url)
    return response.json()

# Function to process relevant weather data for all locations
def process_weather_data(data):
    city = data['name']
    weather_main = data['weather'][0]['main']
    description = data['weather'][0]['description']
    temp = data['main']['temp']
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    dt = data['dt']
    tz = data['timezone']
    
    # making a dictionary from collected data
    data_dict = {
        "city": [city],
        "Weather": [weather_main],
        "Description": [description],
        "Temperature": [temp],
        "Pressure": [pressure],
        "Humidity": [humidity],
        "Wind Speed": [wind_speed],
        "Datetime": [dt],
        "Timezone": [tz]
    }
    return data_dict

# Function to load raw weather data to staging database 
def load_to_snowflake(df):
    engine = create_engine(URL(
        account=os.getenv('account_identifier'),
        user=os.getenv('sn_user'),
        password=os.getenv('sn_password'),
        database=os.getenv('sn_database'),
        schema=os.getenv('sn_schema'),
        warehouse=os.getenv('sn_warehouse'),
        role=os.getenv('sn_role')
    ))

    df.to_sql('stg_weather_data', con=engine, if_exists='append', index=False)


def save_daily_raw_data_to_azure_blob(df):
        
     # Get connection details from environment variables
    connection_string = os.getenv('azure_storage_connection_string')
    container_name = os.getenv('azure_storage_container_name')
        
    # generate file name with dates
    file_prefix = 'daily_weather_data'
    current_date = datetime.now().strftime('%Y%m%d')
    filename = f"{file_prefix}_{current_date}.csv"
        
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)
        
    # Convert the DataFrame to a CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
        
   # Create a BlobClient
    blob_client = container_client.get_blob_client(filename)
        
    # Upload the CSV string as a blob
    blob_client.upload_blob(csv_data, blob_type="BlockBlob")
        
    print(f"File {filename} uploaded to Azure Blob Storage successfully.")



# Fetch weather data for multiple locations
def main():
    with open('uk_cities_coordinates.json', 'r') as json_file:
        all_uk_cities = pd.DataFrame(json.load(json_file)['cities'])

    all_uk_cities = all_uk_cities.rename(columns={'name': 'city'})

    # Empty list to hold data points
    records = []
    for city, row in all_uk_cities.iterrows():
        lat = row['latitude']
        lon = row['longitude']
        data = get_weather_data(lat, lon, API_key)
        processed_data = process_weather_data(data)
        records.append(processed_data)
    
    df = pd.DataFrame(records)
    load_to_snowflake(df)
    save_daily_raw_data_to_azure_blob(df)

if __name__ == "__main__":
    main()
