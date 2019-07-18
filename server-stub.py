#!/usr/bin/env python3
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from database import DatabaseConnection

# Port to use
PORT_NUMBER = 8181

# Database settings
DATABASE_HOST = "localhost"
DATABASE_NAME = "gis"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = None

# Database table that holds the simplified data
DATABASE_TABLE = "simplified"

# Paths to register the http handlers on
PATH_AREA_TYPES = '/areaTypes'
PATH_AREAS = '/area/'

# Response template for area types requests
RESPONSE_AREA_TYPES = """{
   "pathPrefix":"area",
   "endpoints":[
      {
         "name":"Countries",
         "description":"Country borders",
         "key":"countries"
      }
   ]
}"""

# Reference to the database connection to use
database = None

# Stats
database_requests = [0] * 20
database_time_sum = [0] * 20


# Class that handles incoming HTTP requests
class HTTPHandler(BaseHTTPRequestHandler):
    # Handler for GET requests
    def do_GET(self):
        # Handle request depending on the request path
        if self.path == PATH_AREA_TYPES:
            self.handle_area_types()
        elif self.path.startswith(PATH_AREAS):
            self.handle_areas()
        else:
            self.sendError()
        return

    # Handles requests for area types
    def handle_area_types(self):
        self.success_headers()
        self.wfile.write(bytes(RESPONSE_AREA_TYPES, "UTF-8"))

    # Handles requests for areas
    def handle_areas(self):
        global database, database_requests, database_time_sum

        # Get query parameters of the request
        queryParameters = parse_qs(urlparse(self.path).query)

        # Raise error if parameter zoom was not provided
        if not "zoom" in queryParameters:
            self.sendError()

        # Read request parameters
        x_min = float(queryParameters.get("x_min")[0])
        y_min = float(queryParameters.get("y_min")[0])
        x_max = float(queryParameters.get("x_max")[0])
        y_max = float(queryParameters.get("y_max")[0])
        zoom = float(queryParameters.get("zoom")[0])
        zoom = round(zoom)

        # Start to measure required time
        time_database_start = time.time()

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
                        SELECT * FROM {DATABASE_TABLE}
                        WHERE (zoom = {zoom}) AND
                        (geom && ST_MakeEnvelope({x_min}, {y_min}, {x_max}, {y_max}))
                    ) AS filter;"""

        # Replace line breaks in query
        query = query.replace("\n", "")

        # Send query to database
        result = database.queryForResult(query)

        # Update stats
        database_requests[zoom] += 1
        database_time_sum[zoom] += (time.time() - time_database_start)

        # Get GeoJSON from result
        geoJSON = result[0][0]

        # Done, come to an end
        self.success_headers()

        # Write feature collection as JSON into response
        self.wfile.write(bytes(geoJSON, "UTF-8"))

    # Sets success headers for response
    def success_headers(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    # Indicates that an error occurred
    def sendError(self):
        self.send_response(400)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(bytes("Invalid request.", "UTF-8"))


# Periodically outputs the current stats at the console
def outputStats():
    global database_requests, database_time_sum
    print("Average query times:")
    print("---------------")
    for zoom in range(len(database_requests)):
        requests = database_requests[zoom]
        if requests < 1:
            continue
        print(f"Zoom {zoom}: {database_time_sum[zoom] / requests} seconds")

    # Output stats again in 60 seconds
    threading.Timer(60, outputStats).start()


# Main function
def main():
    global database

    # Connect to database
    database = DatabaseConnection(host=DATABASE_HOST, database=DATABASE_NAME, user=DATABASE_USER,
                                  password=DATABASE_PASSWORD)

    # Output stats after 60 seconds for the first time
    threading.Timer(60, outputStats).start()

    # Initialize server
    server = None
    try:
        # Create basic web server
        server = HTTPServer(('', PORT_NUMBER), HTTPHandler)
        print('Started server stub on port', PORT_NUMBER)

        # Wait for incoming requests
        server.serve_forever()

    except KeyboardInterrupt:
        print('Shutting down server stub.')
        if server is not None: server.socket.close()


if __name__ == '__main__':
    main()
