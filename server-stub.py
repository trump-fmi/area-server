#!/usr/bin/env python3
import json
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from database import DatabaseConnection
from jsonschema import validate

# Port to use
PORT_NUMBER = 8080

# File paths for area types JSON schema and document files
AREA_TYPES_DOCUMENT_FILE = "schema/area_types.json"
AREA_TYPES_SCHEMA_FILE = "schema/area_types_schema.json"

# Database settings
DATABASE_HOST = "localhost"
DATABASE_NAME = "gis"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = None

# Resource paths to register the http handlers on
RESOURCE_AREA_TYPES = '/types'
RESOURCE_PATH_GET = '/get/'

# JSON key names of the area types definition
JSON_KEY_TYPES_LIST = "types"
JSON_KEY_TYPE_NAME = "name"
JSON_KEY_TYPE_RESOURCE = "resource"
JSON_KEY_TYPE_TABLE_NAME = "table_name"
JSON_KEY_TYPE_GEOMETRY_LIST = "geometries"
JSON_KEY_TYPE_CONDITIONS = "filter_condition"
JSON_KEY_TYPE_LABELS = "labels"
JSON_KEY_TYPE_SIMPLIFICATION = "simplification"
JSON_KEY_TYPE_ZOOM_MIN = "zoom_min"
JSON_KEY_TYPE_ZOOM_MAX = "zoom_max"

# Holds the area types definition (from JSON)
area_types_definition = {}

# Maps resource names of area types to the area types object
area_types_mapping = {}

# Reference to the database connection to use
database = None


# Class that handles incoming HTTP requests
class HTTPHandler(BaseHTTPRequestHandler):
    # Handler for GET requests
    def do_GET(self):
        # Handle request depending on the request path
        if self.path == RESOURCE_AREA_TYPES:
            self.handle_area_types()
        elif self.path.startswith(RESOURCE_PATH_GET):
            self.handle_areas()
        else:
            self.error_headers()
        return

    # Handles requests for area types
    def handle_area_types(self):
        json_string = json.dumps(area_types_definition)
        self.success_headers()
        self.wfile.write(bytes(json_string, "UTF-8"))

    # Handles requests for areas
    def handle_areas(self):
        global database

        # Extract resource name from path
        search_result = re.search(RESOURCE_PATH_GET + "([A-z0-9_]*)[/?].*", self.path)

        if search_result is None:
            self.error_headers()
            return

        resource_name = search_result.group(1)

        # Get area type for this resource name
        area_type = area_types_mapping.get(resource_name)

        # Check if area could be found
        if area_type is None:
            self.error_headers()
            return

        # Get query parameters of the request
        query_string = urlparse(self.path).query
        query_parameters = parse_qs(query_string)

        # Raise error if not all required parameters are contained in the request
        if not all(param in query_parameters for param in ["x_min", "y_min", "x_max", "y_max", "zoom"]):
            self.error_headers()
            return

        # Read request parameters
        x_min = float(query_parameters.get("x_min")[0])
        y_min = float(query_parameters.get("y_min")[0])
        x_max = float(query_parameters.get("x_max")[0])
        y_max = float(query_parameters.get("y_max")[0])
        zoom = float(query_parameters.get("zoom")[0])
        zoom = round(zoom)

        # Check if zoom is within range
        if (zoom < area_type[JSON_KEY_TYPE_ZOOM_MIN]) or (zoom > area_type[JSON_KEY_TYPE_ZOOM_MAX]):
            # Send empty response
            self.success_headers()
            self.wfile.write(bytes("{}", "UTF-8"))
            return

        # Get database table name from area type
        db_table_name = area_type[JSON_KEY_TYPE_TABLE_NAME]

        # Build bounding box query
        query = f"""SELECT CONCAT(
                    '{{
                        "type": "FeatureCollection",
                        "crs": {{
                            "type": "name",
                            "properties": {{
                                "name": "EPSG:4326"
                            }}
                        }},
                        "features": [', string_agg(CONCAT(
                            '{{
                                "type": "Feature",
                                "id": ', id, ',
                                "geometry": ', geojson, ',
                                "properties": {{
                                    "zoom": ', zoom,
                                '}}
                            }}'), ','), '
                        ]
                    }}')
                    FROM (
                        SELECT * FROM {db_table_name}
                        WHERE (zoom = {zoom}) AND
                        (geom && ST_MakeEnvelope({x_min}, {y_min}, {x_max}, {y_max}))
                    ) AS filter;"""

        # Replace line breaks in query
        query = query.replace("\n", "")

        # Send query to database
        result = database.queryForResult(query)

        # Get GeoJSON from result
        geo_json = result[0][0]

        # Send success headers
        self.success_headers()

        # Write feature collection as JSON into response
        self.wfile.write(bytes(geo_json, "UTF-8"))

    # Sets success headers for response
    def success_headers(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    # Indicates that an error occurred
    def error_headers(self):
        self.send_response(400)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(bytes("Invalid request.", "UTF-8"))


# Reads the area type definition from the JSON document and validates it against the schema
def read_area_types():
    global area_types_definition

    # Read in area types document file
    print(f"Reading area types document file \"{AREA_TYPES_DOCUMENT_FILE}\"...")
    with open(AREA_TYPES_DOCUMENT_FILE) as document_file:
        area_types = json.load(document_file)

    # Read in area types schema file
    print(f"Reading area types schema file \"{AREA_TYPES_SCHEMA_FILE}\"...")
    with open(AREA_TYPES_SCHEMA_FILE) as schema_file:
        area_schema = json.load(schema_file)

        # Validate document against the schema
        print("Validating area types definition...")
        validate(instance=area_types, schema=area_schema)

    # Set global area type dict if everything went fine
    area_types_definition = area_types[JSON_KEY_TYPES_LIST]


def parse_area_types():
    global area_types_definition, area_types_mapping

    for area_type in area_types_definition:
        resource = area_type[JSON_KEY_TYPE_RESOURCE]
        area_types_mapping[resource] = area_type


# Main function
def main():
    global database

    # Read area types definition from file and extract relevant information
    read_area_types()
    parse_area_types()

    print()

    # Connect to database
    print(f"Connecting to database \"{DATABASE_NAME}\" at \"{DATABASE_HOST}\" as user \"{DATABASE_USER}\"...")
    database = DatabaseConnection(host=DATABASE_HOST, database=DATABASE_NAME, user=DATABASE_USER,
                                  password=DATABASE_PASSWORD)
    print("Successfully connected")
    print(f"Starting server on port {PORT_NUMBER}...")

    # Initialize server
    server = None
    try:
        # Create basic web server
        server = HTTPServer(('', PORT_NUMBER), HTTPHandler)
        print("Server started")

        # Wait for incoming requests
        server.serve_forever()

    except KeyboardInterrupt:
        print('Interrupt: Shutting down server')
        if server is not None: server.socket.close()
        if database is not None: database.disconnect()


if __name__ == '__main__':
    main()
