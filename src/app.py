from flask import Flask, request, jsonify
import sqlite3
from shapely.geometry import Point, Polygon
from database import get_well_by_api, get_wells_in_polygon


app = Flask(__name__)

# Getting db connection
def get_db_connection():
    conn = sqlite3.connect("../data/wells.db")
    conn.row_factory = sqlite3.Row
    return conn



# Endpoint 1 for retrieving well data for a given API number (get request)
@app.route('/well/<api_number>', methods=['GET'])
def get_well(api_number):
    well_data = get_well_by_api(api_number)
    if not well_data:
        return jsonify({"error": "Well not found"}), 404
    return jsonify(well_data)


# Endpoint 2 for getting the API numbers of ALL wells within a given polygon
@app.route('/wells-in-polygon', methods=['POST'])
def get_wells():
    polygon = request.json.get("polygon", [])
    if not polygon:
        return jsonify({"error": "Polygon data required"}), 400

    api_numbers = get_wells_in_polygon(polygon)
    return jsonify({"api_numbers": api_numbers})





if __name__ == "__main__":
    app.run(debug=True)
