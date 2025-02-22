import sqlite3
from typing import List, Dict, Tuple

DB_PATH = "../data/wells.db"  # DB file location


# Initializing the database and creating the table if it does not exist
def initialize_db():
    """
    This function is initializing the SQLite database by creating the `api_well_data` table.
    If the table already exists, it is skipping the creation step.
    """
    # Establishing the connection to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Creating the table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_well_data (
            "Operator" TEXT,
            "Status" TEXT,
            "Well Type" TEXT,
            "Work Type" TEXT,
            "Directional Status" TEXT,
            "Multi-Lateral" TEXT,
            "Mineral Owner" TEXT,
            "Surface Owner" TEXT,
            "Surface Location" TEXT,
            "GL Elevation" INTEGER, -- Might need to be REAL type, but seems int for all the apis i skimmed
            "KB Elevation" REAL,
            "DF Elevation" REAL,
            "Single/Multiple Completion" TEXT,
            "Potash Waiver" TEXT,
            "Spud Date" TEXT,
            "Last Inspection" TEXT,
            "TVD" INTEGER, -- Might need to be REAL type but seems integer
            "API" TEXT PRIMARY KEY,
            "Latitude" REAL,
            "Longitude" REAL,
            "CRS" TEXT
        )
    """)

    # Committing the changes and closing the connection
    conn.commit()
    conn.close()


# Inserting a single well record into the database
def insert_well_data(data: Dict):
    """
    This function is inserting a single well record into the database.
    If the record already exists (based on the API number), it is ignoring the insertion.

    :param data: A dictionary containing the well data.
    """
    # Establishing the connection to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Inserting the data while ignoring duplicates based on the API number (primary key)
    cursor.execute("""
        INSERT OR IGNORE INTO api_well_data (
            "Operator", "Status", "Well Type", "Work Type", "Directional Status", 
            "Multi-Lateral", "Mineral Owner", "Surface Owner", "Surface Location", 
            "GL Elevation", "KB Elevation", "DF Elevation", "Single/Multiple Completion", 
            "Potash Waiver", "Spud Date", "Last Inspection", "TVD", "API", "Latitude", "Longitude", "CRS"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["operator"], data['status'], data["well_type"], data['work_type'],
        data['directional_status'], data['multi_lateral'], data['mineral_owner'],
        data['surface_owner'], data['surface_location'], data['gl_elevation'],
        data['kb_elevation'], data['df_elevation'], data['single_or_multiple_completion'],
        data['potash_waiver'], data['spud_date'], data['last_inspection'],
        data['tvd'], data['API'], data['Latitude'], data['Longitude'], data['CRS']
    ))

    # Committing the changes and closing the connection
    conn.commit()
    conn.close()


# Retrieving well data by API number
def get_well_by_api(api_number: str) -> Dict:
    """
    This function is retrieving well data from the database based on the provided API number.

    :param api_number: The API number of the well.
    :return: A dictionary containing the well data if found, or an empty dictionary if not.
    """
    # Establishing the connection to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Retrieving the well data for the given API number
    cursor.execute('SELECT * FROM api_well_data WHERE "API" = ?', (api_number,))
    row = cursor.fetchone()

    # Getting the column names from the cursor description
    columns = [desc[0] for desc in cursor.description]

    # Closing the connection
    conn.close()

    # Returning the result as a dictionary if a row is found
    return dict(zip(columns, row)) if row else {}


# Retrieving the API numbers of all wells within a given geospatial polygon
def get_wells_in_polygon(polygon: List[Tuple[float, float]]) -> List[str]:
    """
    This function is retrieving the API numbers of all wells inside the given geospatial polygon.

    :param polygon: A list of latitude, longitude tuples representing the polygon.
    :return: A list of API numbers for wells within the polygon.
    """
    # Establishing the connection to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Extracting the min and max lat/lon to create a bounding box for filtering
    min_lat = min(lat for lat, lon in polygon)
    max_lat = max(lat for lat, lon in polygon)
    min_lon = min(lon for lat, lon in polygon)
    max_lon = max(lon for lat, lon in polygon)

    # Filtering the data based on the bounding box
    cursor.execute("""
        SELECT "API", "Latitude", "Longitude" FROM api_well_data 
        WHERE "Latitude" BETWEEN ? AND ? AND "Longitude" BETWEEN ? AND ?
    """, (min_lat, max_lat, min_lon, max_lon))

    # Checking if the points are inside the polygon
    results = []
    for api, lat, lon in cursor.fetchall():
        if is_point_in_polygon(lat, lon, polygon):
            results.append(api)

    # Closing the connection
    conn.close()

    # Returning the list of API numbers inside the polygon
    return results


# Checking if a point (lat, lon) is inside a polygon
def is_point_in_polygon(lat: float, lon: float, polygon: List[Tuple[float, float]]) -> bool:
    """
    This function is checking if a point is inside a given geospatial polygon using the ray-casting algorithm.

    :param lat: Latitude of the point to check.
    :param lon: Longitude of the point to check.
    :param polygon: List of tuples representing the polygon's vertices.
    :return: True if the point is inside the polygon, False otherwise.
    """
    # Implementing the ray-casting algorithm for checking point-in-polygon
    num = len(polygon)
    j = num - 1
    inside = False

    for i in range(num):
        lat1, lon1 = polygon[i]
        lat2, lon2 = polygon[j]
        if ((lon1 > lon) != (lon2 > lon)) and (lat < (lat2 - lat1) * (lon - lon1) / (lon2 - lon1) + lat1):
            inside = not inside  # Flipping the inside status when crossing the edge
        j = i

    # Returning whether the point is inside the polygon
    return inside


# Initializing the database when the script is executed
if __name__ == "__main__":
    initialize_db()
    print("Database initialized.")
