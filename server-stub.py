import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# Port to use for the stub
from data import COORDS_STUTTGART

PORT_NUMBER = 8080

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

# Response template for area data requests
RESPONSE_AREAS = {
    "type": "FeatureCollection",
    "crs": {
        "type": "name",
        "properties": {
            "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
        }
    },
    "features": [
        {
            "id": 12345,
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": []
            },
            "properties": {
                "name": "Stuttgart",
                "color": "#FF0000",
                "border-color": "#FFFF00",
                "opacity": 0.5
            }
        }
    ]
}

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
        # Get query parameters of the request
        queryParameters = parse_qs(urlparse(self.path).query)

        # Raise error if parameter zoom was not provided
        if not "zoom" in queryParameters:
            self.sendError()

        # Get zoom from parameters
        zoom = queryParameters.get("zoom")[0]
        zoom = round(float(zoom))

        # List of coordinates to use
        coordinates = []

        # Take not all coordinates if zoom less than 13 -> simplification
        if int(zoom) < 13:

            n = 13 - zoom + 1

            # Iterate over all available coordinates
            for index, coordinate in enumerate(COORDS_STUTTGART):
                # Add only every n-th coordinate to coordinates list
                if index % n == 0:
                    coordinates.append(coordinate)
        else:
            # Take all available coordinates
            coordinates = COORDS_STUTTGART.copy()

        # Copy response template object
        responseObject = RESPONSE_AREAS.copy()

        # Add coordinates to the response object
        responseObject.get("features")[0].get("geometry").update({
            "coordinates": [coordinates]
        })

        # Send success headers
        self.success_headers()

        # Write response object as JSON
        self.wfile.write(bytes(json.dumps(responseObject), "UTF-8"))

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


def main():
    try:
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
