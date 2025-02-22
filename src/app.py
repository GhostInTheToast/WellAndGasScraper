import sqlite3
from flask import Flask, request, jsonify
from database import get_well_by_api, get_wells_in_polygon

# Initializing the Flask app
app = Flask(__name__)


# Endpoint 1: Retrieving well data by API number (GET request)
@app.route('/well/<api_number>', methods=['GET'])
def get_well(api_number):
    """
    This endpoint is retrieving well data for a given API number.
    It is returning the well information in JSON format if found,
    or an error message if the well is not found or if an error occurs.

    :param api_number: The unique API number of the well.
    :return: JSON response with well data or error message.
    """
    try:
        # Attempting to retrieve the well data using the provided API number
        well_data = get_well_by_api(api_number)

        # If the well data is not found, returning a 404 Not Found error
        if not well_data:
            return jsonify({"error": "Well not found"}), 404

        # Returning the well data as a JSON response
        return jsonify(well_data)

    except sqlite3.Error as e:
        # Handling database-related errors (e.g., connection issues, query errors)
        return jsonify({"error": f"Database error: {str(e)}"}), 500

    except Exception as e:
        # Handling any other unexpected errors
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


# Endpoint 2: Retrieving API numbers of wells inside a given geospatial polygon (POST request)
@app.route('/wells-in-polygon', methods=['POST'])
def get_wells():
    """
    This endpoint is receiving a polygon (list of latitude, longitude tuples) and
    is returning the API numbers of all wells within the polygon.

    :return: JSON response containing a list of API numbers within the given polygon.
    """
    # Extracting the polygon data from the request body (expects a list of tuples)
    polygon = request.json.get("polygon", [])

    # Validating the polygon data: Ensuring it's a non-empty list
    if not isinstance(polygon, list) or not polygon:
        return jsonify({"error": "Valid polygon data required"}), 400

    # Fetching the list of API numbers that are within the given polygon
    api_numbers = get_wells_in_polygon(polygon)

    # Returning the list of API numbers as a JSON response
    return jsonify({"api_numbers": api_numbers})


if __name__ == "__main__":
    # Starting the Flask development server with debugging enabled
    app.run(debug=True)
