from http.server import BaseHTTPRequestHandler, HTTPServer

# Port to use for the stub
PORT_NUMBER = 8080

PATH_AREA_TYPES = '/areaTypes'
PATH_AREAS = '/area/'

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

RESPONSE_AREAS = """{  
   "type":"FeatureCollection",
   "crs":{  
      "type":"name",
      "properties":{  
         "name":"urn:ogc:def:crs:OGC:1.3:CRS84"
      }
   },
   "features":[
      {  
         "type":"Feature",
         "geometry":{  
            "type":"Polygon",
            "coordinates":[
               [  
                  [9.212838, 48.728393],
                  [9.308429, 48.781225],
                  [9.243133, 48.825243],
                  [9.172391, 48.851026],
                  [9.105086, 48.832009],
                  [9.072961, 48.761447],
                  [9.066104, 48.723396],
                  [9.246481, 48.694931]
               ]
            ]
         },
         "properties":{  
            "name":"Stuttgart",
            "color":"#FF0000",
            "border-color":"#FFFF00",
            "opacity":0.5
         }
      }
   ]
}
"""


# Class that handles incoming HTTP requests
class HTTPHandler(BaseHTTPRequestHandler):

    # Handler for GET requests
    def do_GET(self):
        print('Request path:', self.path)

        # Handle request depending on the request path
        if self.path == PATH_AREA_TYPES:
            self.handle_area_types()
        elif self.path.startswith(PATH_AREAS):
            self.handle_areas()
        else:
            self.error()
        return

    # Handles requests for area types
    def handle_area_types(self):
        self.success_headers()
        self.wfile.write(bytes(RESPONSE_AREA_TYPES, "UTF-8"))

    # Handles requests for areas
    def handle_areas(self):
        self.success_headers()
        self.wfile.write(bytes(RESPONSE_AREAS, "UTF-8"))

    # Sets success headers for response
    def success_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    # Indicates that an error occurred
    def error(self):
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
