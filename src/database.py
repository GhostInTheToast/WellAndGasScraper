import sqlite3
from typing import List, Dict, Tuple

DB_PATH = "data/wells.db"  # DB file location


# Function for initializing the database
def initialize_db():

    # Creating sqlite db and api_well_data table if they do not exist already
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

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

    conn.commit()
    conn.close()



# Function for inserting 1 single well record at a time into the db
def insert_well_data(data: Dict):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO api_well_data (
            "Operator", "Status", "Well Type", "Work Type", "Directional Status", 
            "Multi-Lateral", "Mineral Owner", "Surface Owner", "Surface Location", 
            "GL Elevation", "KB Elevation", "DF Elevation", "Single/Multiple Completion", 
            "Potash Waiver", "Spud Date", "Last Inspection", "TVD", "API", "Latitude", "Longitude", "CRS"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("Operator"), data.get("Status"), data.get("Well Type"), data.get("Work Type"),
        data.get("Directional Status"), data.get("Multi-Lateral"), data.get("Mineral Owner"),
        data.get("Surface Owner"), data.get("Surface Location"), data.get("GL Elevation"),
        data.get("KB Elevation"), data.get("DF Elevation"), data.get("Single/Multiple Completion"),
        data.get("Potash Waiver"), data.get("Spud Date"), data.get("Last Inspection"),
        data.get("TVD"), data.get("API"), data.get("Latitude"), data.get("Longitude"), data.get("CRS")
    ))

    conn.commit()
    conn.close()



# Function for getting a well by its api number [Note: it is actually stored as string, not a number, in the db]
def get_well_by_api(api_number: str) -> Dict:

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM api_well_data WHERE "API" = ?', (api_number,))
    row = cursor.fetchone()
    columns = [desc[0] for desc in cursor.description]
    conn.close()

    return dict(zip(columns, row)) if row else {}



# Function for getting all wells inside a given geospatial polygon
def get_wells_in_polygon(polygon: List[Tuple[float, float]]) -> List[str]:

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Extract min/max lat/lon for a bounding box filter
    min_lat = min(lat for lat, lon in polygon)
    max_lat = max(lat for lat, lon in polygon)
    min_lon = min(lon for lat, lon in polygon)
    max_lon = max(lon for lat, lon in polygon)

    # Initial filter using bounding box (simpler than full polygon check)
    cursor.execute("""
        SELECT "API", "Latitude", "Longitude" FROM api_well_data 
        WHERE "Latitude" BETWEEN ? AND ? AND "Longitude" BETWEEN ? AND ?
    """, (min_lat, max_lat, min_lon, max_lon))

    results = []
    for api, lat, lon in cursor.fetchall():
        if is_point_in_polygon(lat, lon, polygon):
            results.append(api)

    conn.close()
    return results



# Function for checking if a point is in a geospatial polygon or not (true or false boolean check)
def is_point_in_polygon(lat: float, lon: float, polygon: List[Tuple[float, float]]) -> bool:

    # This function is using this ray casting alg which i have linked below for an example on
    # https://rosettacode.org/wiki/Ray-casting_algorithm
    num = len(polygon)
    j = num - 1
    inside = False

    for i in range(num):
        lat1, lon1 = polygon[i]
        lat2, lon2 = polygon[j]
        if ((lon1 > lon) != (lon2 > lon)) and (lat < (lat2 - lat1) * (lon - lon1) / (lon2 - lon1) + lat1):
            inside = not inside
        j = i

    return inside



if __name__ == "__main__":
    initialize_db()
    print("Database initialized.")
