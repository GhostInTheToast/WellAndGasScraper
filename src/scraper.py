import time
import random
import requests
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to scrape data for a single API
def scrape_well_data(api_number):
    # Building the url based on the api num passed in
    url = f"https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/WellDetails.aspx?api={api_number}"

    try:
        response = fetch_with_retries(url)
        if not response:
            return None  # Skip this API number if it keeps failing
        soup = BeautifulSoup(response.text, 'lxml')

        # the data field names are mapped to the scraped html id here
        fields = {
            "operator": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblOperator",
            "status": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblStatus",
            "well_type": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblWellType",
            "work_type": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblWorkType",
            "directional_status": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblDirectionalStatus",
            "multi_lateral": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblMultiLateral",
            "mineral_owner": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblMineralOwner",
            "surface_owner": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblSurfaceOwner",
            "surface_location": "ctl00_ctl00__main_main_ucGeneralWellInformation_Location_lblLocation",
            "gl_elevation": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblGLElevation",
            "kb_elevation": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblKBElevation",
            "df_elevation": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblDFElevation",
            "single_or_multiple_completion": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblCompletions",
            "potash_waiver": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblPotashWaiver",
            "spud_date": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblSpudDate",
            "last_inspection": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblLastInspectionDate",
            "tvd": "ctl00_ctl00__main_main_ucGeneralWellInformation_lblTrueVerticalDepth"
        }

        # api #
        api = api_number

        # coordinate_data for latitude, longitude, crs #
        coordinate_data_tag = soup.find('span',
                                        id='ctl00_ctl00__main_main_ucGeneralWellInformation_Location_lblCoordinates')
        if coordinate_data_tag:
            coordinate_data = coordinate_data_tag.text.strip()
            if coordinate_data:
                parts = coordinate_data.rsplit(' ', 1)  # Attempt to split from the right
                if len(parts) == 2:
                    coords, crs = parts  # Successfully split into coordinates and CRS
                else:
                    coords, crs = parts[0], None  # No CRS found, assign None

                # Further split coordinates into latitude and longitude
                latitude, longitude = coords.split(',') if ',' in coords else (None, None)
        else:
            crs = latitude = longitude = None

        # storing extracted values
        data = {"API": api, "Latitude": latitude, "Longitude": longitude, "CRS": crs}

        for key, field_id in fields.items():
            tag = soup.find('span', id=field_id)
            data[key] = tag.text.strip() if tag else None

        # operator data needs the id number of the company before the name removed. we are doing so below.
        last_bracket_index = data['operator'].rfind("]")   # finding the last closing bracket

        # extracting everything after the last bracket and finally storing it
        cleaned_operator = data['operator'][last_bracket_index + 1:].strip() if last_bracket_index != -1 else data['operator']
        data['operator'] = cleaned_operator

        # Replace empty strings with None, ensuring None values are not processed
        data = {key: (value.strip() if isinstance(value, str) and value.strip() else None) for key, value in
                data.items()}

        return data

    except requests.RequestException as e:
        print(f"Error scraping API {api_number}: {e}")
        return None


# Function to read the API numbers from the CSV file
def read_api_numbers(file_path):
    with open(file_path, 'r', encoding='utf-8-sig') as f:  # Handles BOM
        reader = csv.DictReader(f)
        print(reader.fieldnames)  # Debugging: Check actual field names
        return [row['api'] for row in reader]  # Extract API numbers


# Multithreaded scraping function [not being used but its something to do eventually maybe]
def scrape_all_apis(api_numbers, max_threads=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_api = {executor.submit(scrape_well_data, api): api for api in api_numbers}

        for future in as_completed(future_to_api):
            api_number = future_to_api[future]
            try:
                data = future.result()
                if data:
                    results.append(data)
                    print(f"Scraped data for API {api_number}")
                else:
                    print(f"Failed to scrape API {api_number}")
            except Exception as e:
                print(f"Exception occurred while scraping API {api_number}: {e}")

    return results


# Function to fetch but retry also!
def fetch_with_retries(url, max_retries=5):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)  # 10 sec timeout
            response.raise_for_status()  # Raise an error for 4xx/5xx
            return response
        except requests.exceptions.Timeout:
            print(f"Timeout, retrying {attempt + 1}/{max_retries}...")
            time.sleep(random.uniform(2, 5))  # Random delay before retry
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
    print("Max retries reached. Skipping.")
    return None

