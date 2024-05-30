import os 
import requests
import pathlib
import logging


# Set the API URL and local directory
api_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
local_dir = pathlib.Path("data/raw")

# Create the local directory if it doesn't exist
if not local_dir.exists():
    local_dir.mkdir(parents=True)

# Set up logging
logging.basicConfig(filename='download.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the file names and API URLs
file_names = ["green_tripdata_2023-01.parquet", "green_tripdata_2023-02.parquet", "green_tripdata_2023-03.parquet"]
api_urls = [f"{api_url}{file_name}" for file_name in file_names]

# Download each file
for api_url in api_urls:
    file_name = os.path.basename(api_url)
    local_file_path = local_dir / file_name
    try:
        response = requests.get(api_url, stream=True)
        if response.status_code == 200:
            with open(local_file_path, "wb") as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            logging.info(f"Downloaded {file_name} to {local_dir}")
        else:
            logging.error(f"Failed to download {file_name}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {file_name}: {str(e)}")

