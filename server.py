#!/usr/bin/env python3
import json
import logging
import re
from database import DatabaseConnection
from jsonschema import validate
from aiohttp import web

# Port to use
from timing import TimeMeasure

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
RESOURCE_PATH_GET = '/get/{type}'

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

# Will hold the client-related data about the area types
area_types_client_list = []

# Reference to the database connection to use
database = None

# Headers added to every response
HEADERS = {"Access-Control-Allow-Origin": "*"}

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
    global area_type_groups, area_types_mapping, area_types_client_list

    # List holding the filtered client-related data
    area_types_client_list = []

    # Iterate over all defined area type groups
    for area_type_group in area_type_groups:

        # Create new object for this area type group that only contains client-related data
        filtered_group = {}
        filtered_group[JSON_KEY_GROUP_NAME] = area_type_group[JSON_KEY_GROUP_NAME]
        filtered_group[JSON_KEY_GROUP_TYPES] = []

        area_types_client_list.append(filtered_group)

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


def db_connect():
    global database
    database = DatabaseConnection(host=DATABASE_HOST, database=DATABASE_NAME, user=DATABASE_USER,
                                  password=DATABASE_PASSWORD)


def handle_types(request):
    return web.json_response(area_types_client_list, headers=HEADERS)


def handle_areas(request):
    global database

    requested_type = request.match_info.get('type', None)

    # Init time measure
    measure = TimeMeasure()

    if not requested_type:
        print("Bad Request: No area type specified")
        raise web.HTTPBadRequest(text="No area type specified.")

    # Get area type for this resource name
    area_type = area_types_mapping.get(requested_type)

    # Check if area could be found
    if area_type is None:
        print(f"Bad Request: Area type '{requested_type}' not available.")
        raise web.HTTPBadRequest(text=f"Area type '{requested_type}' not available.")

    # Get query parameters of the request
    query_parameters = request.query

    # Raise error if not all required parameters are contained in the request
    necessary_parameters = ["x_min", "y_min", "x_max", "y_max", "zoom"]
    if not all(param in query_parameters for param in necessary_parameters):
        print(f"Bad Request: Query parameters missing. Necessary: {necessary_parameters}")
        raise web.HTTPBadRequest(text=f"Query parameters missing. Necessary: {necessary_parameters}")

    # Read request parameters
    x_min = float(query_parameters["x_min"])
    y_min = float(query_parameters["y_min"])
    x_max = float(query_parameters["x_max"])
    y_max = float(query_parameters["y_max"])
    zoom = round(float(query_parameters["zoom"]))

    # Check if zoom is within range
    if (zoom < area_type[JSON_KEY_GROUP_TYPE_ZOOM_MIN]) or (zoom >= area_type[JSON_KEY_GROUP_TYPE_ZOOM_MAX]):
        raise web.HTTPNoContent()

    # Provide measure with meta data about request
    measure.set_meta_data(x_min, y_min, x_max, y_max, zoom, requested_type)

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
                        FROM {db_table_name}
                            WHERE (zoom = {zoom}) AND
                            (geom && ST_MakeEnvelope({x_min}, {y_min}, {x_max}, {y_max}));"""

    # Replace line breaks and multiple spaces from query
    query = query.replace("\n", "")
    query = re.sub(" {2,}", " ", query)

    # Try to issue the query at the database and measure timings
    measure.query_issued()
    result = database.query_for_result(query)
    measure.query_done()

    # Sanity check for result
    if result is None:
        print("Internal Server Error: Failed to retrieve data from database")
        raise web.HTTPInternalServerError(text="Failed to retrieve data from database")

    # Get GeoJSON from result
    geo_json = result[0][0]

    try:
        # Send success response
        return web.Response(text=geo_json, content_type="application/json", headers=HEADERS)

    finally:
        # Finish measuring
        measure.request_answered()
        measure.write_result()


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

    print(f"Successfully connected. Starting server on port {PORT_NUMBER}...")

    app = web.Application()
    logging.basicConfig(level=logging.DEBUG)
    app.add_routes([web.get('/', handle_types),
                    web.get(RESOURCE_AREA_TYPES, handle_types),
                    web.get(RESOURCE_PATH_GET, handle_areas)])

    web.run_app(app, port=PORT_NUMBER)


if __name__ == '__main__':
    main()
