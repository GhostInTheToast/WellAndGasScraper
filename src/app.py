from flask import Flask, request, jsonify
import sqlite3
from shapely.geometry import Point, Polygon

app = Flask(__name__)

# Getting db connection
def get_db_connection():
    conn = sqlite3.connect("../data/wells.db")
    conn.row_factory = sqlite3.Row  # Allows column access by name
    return conn

# Endpoint 1: Retrieve well data for a given API number
@app.route("/well/<api_number>", methods=["GET"])
def get_well_data(api_number):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM api_well_data WHERE API = ?", (api_number,))
    well = cursor.fetchone()
    conn.close()

    if well:
        return jsonify(dict(well))
    else:
        return jsonify({"error": "Well not found"}), 404

# Endpoint 2: Get API numbers of wells within a given polygon
@app.route("/wells-in-polygon", methods=["POST"])
def get_wells_in_polygon():
    data = request.json
    polygon = data.get("polygon")  # Expecting a list of (lat, lon) tuples

    if not polygon:
        return jsonify({"error": "Polygon data required"}), 400

    query = """
    SELECT API, Latitude, Longitude FROM api_well_data
    WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    wells = cursor.fetchall()
    conn.close()

    poly = Polygon(polygon)

    filtered_wells = [
        dict(well) for well in wells
        if poly.contains(Point(float(well["Latitude"]), float(well["Longitude"])))
    ]

    return jsonify(filtered_wells)

if __name__ == "__main__":
    app.run(debug=True)
