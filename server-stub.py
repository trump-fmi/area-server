#!/usr/bin/env python3
import json
import copy
import numpy
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from simplification.cutil import simplify_coords

# File of area data to use
FEATURE_DATA_FILE = "stuttgart.json"

# Threshold for starting the simplification
SIMPLIFICATION_THRESHOLD = 13 # Suggestion: 7 for countries, 13 for stuttgart
SIMPLIFICATION_FACTOR = 0.005 # Suggestion: 0.2 for countries, 0.01 for stuttgart

# Port to use
PORT_NUMBER = 8080

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

# Stores the parsed feature data
feature_data = {}


# Class that handles incoming HTTP requests
class HTTPHandler(BaseHTTPRequestHandler):
    global feature_data

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
        # Get query parameters of the request
        queryParameters = parse_qs(urlparse(self.path).query)

        # Raise error if parameter zoom was not provided
        if not "zoom" in queryParameters:
            self.sendError()

        # Get zoom from parameters
        zoom = queryParameters.get("zoom")[0]
        zoom = float(zoom)

        # Copy feature data
        feature_collection = copy.deepcopy(feature_data)

        # Iterate over all features
        for index, feature in enumerate(feature_collection.get("features")):
            # Check for threshold
            if zoom >= SIMPLIFICATION_THRESHOLD:
                break

            # Get coordinates of current feature
            coordinates = feature.get("geometry").get("coordinates")[0]

            # Simplify coordinates
            coordinates_simplified = simplify_coords(coordinates, (SIMPLIFICATION_THRESHOLD - zoom) * SIMPLIFICATION_FACTOR)

            # Replace coordinates in feature with simplified ones
            feature.get("geometry").update({
                "coordinates": [coordinates_simplified]
            })

        # Done, come to an end
        self.success_headers()

        # Write feature collection as JSON into response
        self.wfile.write(bytes(json.dumps(feature_collection), "UTF-8"))

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

# Main function
def main():
    global feature_data

    try:
        # Read in feature data file
        with open(FEATURE_DATA_FILE) as file:
            feature_data = json.load(file)

        # Create basic web server
        server = HTTPServer(('', PORT_NUMBER), HTTPHandler)
        print('Started server stub on port', PORT_NUMBER)

        # Wait for incoming requests
        server.serve_forever()

    except KeyboardInterrupt:
        print('Shutting down server stub.')
        server.socket.close()


if __name__ == '__main__':
    main()
