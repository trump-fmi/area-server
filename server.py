#!/usr/bin/env python3
import json
import re
import gzip
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from database import DatabaseConnection
from jsonschema import validate

# Port to use
PORT_NUMBER = 8181

# File paths for area types JSON schema and document files
AREA_TYPES_DOCUMENT_FILE = "../area-types/area_types.json"
AREA_TYPES_SCHEMA_FILE = "../area-types/area_types_schema.json"

# Database settings
DATABASE_HOST = "trump-postgis"
DATABASE_NAME = "gis"
DATABASE_USER = "osm"
DATABASE_PASSWORD = "nieVooghee9fioSheicaizeiQueeyi2KaCh7boh2lei7xoo9CohtaeTe3mum"

# Resource paths to register the http handlers on
RESOURCE_AREA_TYPES = '/types'
RESOURCE_PATH_GET = '/get/'

# Coordinate projection of the served GeoJSON data
DATA_PROJECTION = "EPSG:3857"

# JSON key names of the area types definition
JSON_KEY_GROUPS_LIST = "groups"
JSON_KEY_GROUP_NAME = "name"
JSON_KEY_GROUP_TYPES = "types"
JSON_KEY_GROUP_TYPE_LABELS = "labels"
JSON_KEY_GROUP_TYPE_RESOURCE = "resource"
JSON_KEY_GROUP_TYPE_TABLE_NAME = "table_name"
JSON_KEY_GROUP_TYPE_FILTERS = "filter_parameters"
JSON_KEY_GROUP_TYPE_SEARCH_HIGHLIGHT = "search_highlight"
JSON_KEY_GROUP_TYPE_SIMPLIFICATION = "simplification"
JSON_KEY_GROUP_TYPE_Z_INDEX = "z_index"
JSON_KEY_GROUP_TYPE_ZOOM_MIN = "zoom_min"
JSON_KEY_GROUP_TYPE_ZOOM_MAX = "zoom_max"

# List of client-related JSON keys for area types (the values will be sent to the client on request)
CLIENT_AREA_TYPE_KEYS = [JSON_KEY_GROUP_TYPE_LABELS, JSON_KEY_GROUP_TYPE_RESOURCE, JSON_KEY_GROUP_TYPE_SEARCH_HIGHLIGHT,
                         JSON_KEY_GROUP_TYPE_SIMPLIFICATION, JSON_KEY_GROUP_TYPE_Z_INDEX, JSON_KEY_GROUP_TYPE_ZOOM_MIN,
                         JSON_KEY_GROUP_TYPE_ZOOM_MAX]

# Holds the area type groups definition (from JSON)
area_type_groups = {}

# Maps resource names to the corresponding area types
area_types_mapping = {}

# Will hold the JSON string containing client-related data about the area types
area_types_client_json = ""

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
            self.invalid_request_headers()
        return

    # Handles requests for area types
    def handle_area_types(self):
        # Send success response
        self.success_reply(area_types_client_json)

    # Handles requests for areas
    def handle_areas(self):
        global database

        # Extract resource name from path
        search_result = re.search(RESOURCE_PATH_GET + "([A-z0-9_]*)[/?].*", self.path)

        if search_result is None:
            self.invalid_request_headers()
            return

        resource_name = search_result.group(1)

        # Get area type for this resource name
        area_type = area_types_mapping.get(resource_name)

        # Check if area could be found
        if area_type is None:
            self.invalid_request_headers()
            return

        # Get query parameters of the request
        query_string = urlparse(self.path).query
        query_parameters = parse_qs(query_string)

        # Raise error if not all required parameters are contained in the request
        if not all(param in query_parameters for param in ["x_min", "y_min", "x_max", "y_max", "zoom"]):
            self.invalid_request_headers()
            return

        # Read request parameters
        x_min = float(query_parameters.get("x_min")[0])
        y_min = float(query_parameters.get("y_min")[0])
        x_max = float(query_parameters.get("x_max")[0])
        y_max = float(query_parameters.get("y_max")[0])
        zoom = float(query_parameters.get("zoom")[0])
        zoom = round(zoom)

        # Check if zoom is within range
        if (zoom < area_type[JSON_KEY_GROUP_TYPE_ZOOM_MIN]) or (zoom >= area_type[JSON_KEY_GROUP_TYPE_ZOOM_MAX]):
            # Send empty response
            self.empty_headers()
            return

        # Get database table name from area type
        db_table_name = area_type[JSON_KEY_GROUP_TYPE_TABLE_NAME]

        # Build bounding box query
        query = f"""SELECT CONCAT(
                    '{{
                        "type":"FeatureCollection",
                        "crs":{{
                            "type":"name",
                            "properties":{{
                                "name":"{DATA_PROJECTION}"
                            }}
                        }},
                        "features": [', string_agg(CONCAT(
                            '{{
                                "type":"Feature",
                                "id":', id, ',
                                "geometry":', geojson, ',
                                "properties":{{',
                                    CASE WHEN label ISNULL THEN '' ELSE CONCAT('"label":"', label, '",') END,
                                    CASE WHEN label_center ISNULL THEN '' ELSE CONCAT('
                                    "label_center":', label_center, ',
                                    "start_angle":', start_angle, ',
                                    "end_angle":', end_angle, ',
                                    "inner_radius":', inner_radius, ',
                                    "outer_radius":', outer_radius, ',') END,
                                    '"zoom":', zoom,
                                '}}
                            }}'), ','), '
                        ]
                    }}')
                    FROM (
                        SELECT * FROM {db_table_name}
                        WHERE (zoom = {zoom}) AND
                        (geom && ST_MakeEnvelope({x_min}, {y_min}, {x_max}, {y_max}))
                    ) AS filter;"""

        # Replace line breaks and multiple spaces from query
        query = query.replace("\n", "")
        query = re.sub(" {2,}", " ", query)

        # Try to issue the query at the database
        result = None
        try:
            result = database.queryForResult(query)
        except:
            # Database connection was lost
            print("Database connection lost, trying to reconnect...")

            # Try to reconnect
            try:
                db_connect()
                result = database.queryForResult(query)
                print(f"Reconnected successfully")
            except:
                print(f"Reconnect attempt failed")

        # Sanity check for result
        if result is None:
            print("Failed to retrieve data from database")
            self.internal_error_headers()
            return

        # Get GeoJSON from result
        geo_json = result[0][0]

        # Send success response
        self.success_reply(geo_json)

    def success_reply(self, content_string):
        content_bytes = bytes(content_string, 'UTF-8')
        content_gzip = gzip.compress(content_bytes)
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-type', 'application/json')
        self.send_header("Content-length", str(len(content_gzip)))
        self.send_header("Content-Encoding", "gzip")
        self.end_headers()
        self.wfile.write(content_gzip)
        self.wfile.flush()

    # Sets headers for empty response
    def empty_headers(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    # Indicates that the request was malformed
    def invalid_request_headers(self):
        self.send_response(400)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(bytes("Invalid request.", "UTF-8"))

    # Indicates that an internal error occurred
    def internal_error_headers(self):
        self.send_response(500)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(bytes("An internal error occurred.", "UTF-8"))


# Reads the area type definition from the JSON document and validates it against the schema
def read_area_types():
    global area_type_groups

    # Read in area types document file
    print(f"Reading area types document file \"{AREA_TYPES_DOCUMENT_FILE}\"...")
    with open(AREA_TYPES_DOCUMENT_FILE) as document_file:
        area_type_groups = json.load(document_file)

    # Read in area types schema file
    print(f"Reading area types schema file \"{AREA_TYPES_SCHEMA_FILE}\"...")
    with open(AREA_TYPES_SCHEMA_FILE) as schema_file:
        area_schema = json.load(schema_file)

        # Validate document against the schema
        print("Validating area types definition...")
        validate(instance=area_type_groups, schema=area_schema)

    # Set global area type dict if everything went fine
    area_type_groups = area_type_groups[JSON_KEY_GROUPS_LIST]


def parse_area_types():
    global area_type_groups, area_types_mapping, area_types_client_json

    # List holding the filtered client-related data
    client_data = []

    # Iterate over all defined area type groups
    for area_type_group in area_type_groups:

        # Create new object for this area type group that only contains client-related data
        filtered_group = {}
        filtered_group[JSON_KEY_GROUP_NAME] = area_type_group[JSON_KEY_GROUP_NAME]
        filtered_group[JSON_KEY_GROUP_TYPES] = []

        client_data.append(filtered_group)

        # Iterate over all area types of this group
        for area_type in area_type_group[JSON_KEY_GROUP_TYPES]:
            # Get resource name
            resource = area_type[JSON_KEY_GROUP_TYPE_RESOURCE]

            # Add area type to mappings
            area_types_mapping[resource] = area_type

            # Create new dict for this area type that only contains client-related data
            filtered_type = {}
            filtered_group[JSON_KEY_GROUP_TYPES].append(filtered_type)

            # Iterate over all keys of this area type
            for type_key in area_type:
                # Skip key if not flagged as client-related
                if type_key not in CLIENT_AREA_TYPE_KEYS: continue

                # Key is client-related, add it to filtered area type dict
                filtered_type[type_key] = area_type[type_key]

    # Dump client-related data to JSON string
    area_types_client_json = json.dumps(client_data, separators=(',', ':'))


def db_connect():
    global database
    database = DatabaseConnection(host=DATABASE_HOST, database=DATABASE_NAME, user=DATABASE_USER,
                                  password=DATABASE_PASSWORD)


# Main function
def main():
    global database

    # Read area types definition from file and extract relevant data
    read_area_types()
    parse_area_types()

    print()

    # Connect to database
    print(f"Connecting to database \"{DATABASE_NAME}\" at \"{DATABASE_HOST}\" as user \"{DATABASE_USER}\"...")
    db_connect()

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
