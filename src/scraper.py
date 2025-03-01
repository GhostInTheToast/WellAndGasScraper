import time
import random
import requests
from bs4 import BeautifulSoup
import csv
import logging
from shapely.geometry import Point

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Function to scrape data for a single API
def scrape_well_data(api_number):
    """
    Scraping data from the well details page for a given API number.

    Arguments:
    api_number (str): The API number of the well to scrape.

    Returns:
    dict: A dictionary containing the scraped well data, or None if an error occurs.
    """
    # Building the url based on the api num passed in
    url = f"https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/WellDetails.aspx?api={api_number}"

    try:
        # Fetching the response with retries
        response = fetch_with_retries(url)
        if not response:
            return None  # Skipping this API number if it keeps failing
        soup = BeautifulSoup(response.text, 'lxml')

        # Defining the fields and their corresponding HTML ids
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

        # Extracting coordinate data for latitude, longitude, and CRS
        coordinate_data_tag = soup.find('span', id='ctl00_ctl00__main_main_ucGeneralWellInformation_Location_lblCoordinates')
        if coordinate_data_tag:
            coordinate_data = coordinate_data_tag.text.strip()
            if coordinate_data:
                parts = coordinate_data.rsplit(' ', 1)  # Attempting to split from the right
                if len(parts) == 2:
                    coords, crs = parts  # Successfully splitting into coordinates and CRS
                else:
                    coords, crs = parts[0], None  # No CRS found, assigning None
                latitude, longitude = coords.split(',') if ',' in coords else (None, None)
        else:
            crs = latitude = longitude = None

        # Storing extracted values
        data = {"API": api_number, "Latitude": latitude, "Longitude": longitude, "CRS": crs}

        # Extracting well-specific data fields
        for key, field_id in fields.items():
            tag = soup.find('span', id=field_id)
            data[key] = tag.text.strip() if tag else None

        # Cleaning operator data by removing the company id number
        last_bracket_index = data['operator'].rfind("]")
        cleaned_operator = data['operator'][last_bracket_index + 1:].strip() if last_bracket_index != -1 else data['operator']
        data['operator'] = cleaned_operator

        # Replacing empty strings with None, ensuring None values are not processed
        data = {key: (value.strip() if isinstance(value, str) and value.strip() else None) for key, value in data.items()}

        logger.info(f"Scraped data for API {api_number}")
        return data

    except requests.RequestException as e:
        logger.error(f"Error scraping API {api_number}: {e}")
        return None


# Function to read the API numbers from the CSV file
def read_api_numbers(file_path):
    """
    Reading API numbers from a CSV file.

    Arguments:
    file_path (str): The path to the CSV file containing API numbers.

    Returns:
    list: A list of API numbers extracted from the CSV.
    """
    with open(file_path, 'r', encoding='utf-8-sig') as f:  # Handles BOM
        reader = csv.DictReader(f)
        logger.debug(f"CSV fieldnames: {reader.fieldnames}")  # Debugging: Checking actual field names
        return [row['api'] for row in reader]  # Extracting API numbers


# Function to check if the well is within the given geospatial polygon
# Didn't actually use this btw, this experimental unused code
def is_within_polygon(latitude, longitude, polygon):
    """
    Checking if a well's coordinates are within a specified geospatial polygon.

    Arguments:
    latitude (str): The latitude of the well.
    longitude (str): The longitude of the well.
    polygon (Polygon): A Shapely Polygon object representing the area to check against.

    Returns:
    bool: True if the well's coordinates are within the polygon, False otherwise.
    """
    if latitude and longitude:
        point = Point(float(longitude), float(latitude))  # Shapely uses (longitude, latitude)
        return polygon.contains(point)
    return False


# Function to fetch data with retries
def fetch_with_retries(url, max_retries=5):
    """
    Fetching data from a URL with retries in case of failure.

    Arguments:
    url (str): The URL to fetch data from.
    max_retries (int): The maximum number of retries to attempt in case of failure.

    Returns:
    response: The HTTP response object if successful, None otherwise.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)  # 10 sec timeout
            response.raise_for_status()  # Raise an error for 4xx/5xx
            return response
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout, retrying {attempt + 1}/{max_retries}...")
            time.sleep(random.uniform(2, 5))  # Random delay before retry
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
    logger.error("Max retries reached. Skipping.")
    return None
