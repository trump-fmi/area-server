import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class DatabaseConnection:
    def __init__(self, host, database, user, password=None):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.connection.cursor()

    def query(self, query):
        self.cursor.execute(query)

    def queryForResult(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def disconnect(self):
        self.connection.close()
