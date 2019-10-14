from timeit import default_timer as timer
import csv

# CSV file to use for storing logs
LOG_FILE = "timings.csv"


class TimeMeasure:

    def __init__(self):
        # Init fields
        self.x_min = 0
        self.y_min = 0
        self.x_max = 0
        self.y_max = 0
        self.zoom = 0
        self.area_type = ""
        self.time_to_query = 0
        self.time_to_data = 0
        self.time_to_reply = 0
        self.start = timer()

    def set_meta_data(self, x_min, y_min, x_max, y_max, zoom, area_type):
        self.x_min = x_min
        self.y_min = y_min
        self.x_max = x_max
        self.y_max = y_max
        self.zoom = zoom
        self.area_type = area_type

    def query_issued(self):
        self.time_to_query = timer() - self.start

    def query_done(self):
        self.time_to_data = timer() - self.start

    def request_answered(self):
        self.time_to_reply = timer() - self.start

    def write_result(self):
        # Open CSV file in appending mode
        with open(LOG_FILE, mode='a') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(
                [round(self.x_min, 8),
                 round(self.y_min, 8),
                 round(self.x_max, 8),
                 round(self.y_max, 8),
                 self.zoom, self.area_type,
                 round(self.time_to_query, 3),
                 round(self.time_to_data, 3),
                 round(self.time_to_reply, 3)])
