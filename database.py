import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import pool
from psycopg2.extras import execute_values

class DatabaseConnection:
    def __init__(self, host, database, user, password=None):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user=user,
                                                                  password=password,
                                                                  host=host,
                                                                  database=database)

    def open_cursor(self):
        connection = self.connection_pool.getconn()
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection.cursor()

    def close_cursor(self, cursor):
        connection = cursor.connection
        cursor.close()
        self.connection_pool.putconn(connection)

    def write_query(self, query, template=None, query_tuples=None, page_size=1000, retry=0):
        try:
            cursor = self.open_cursor()
            if query_tuples and template:
                psycopg2.extras.execute_values(cursor, query, query_tuples, page_size=page_size, template=template)
            else:
                cursor.execute(query)
        except Exception as e:
            if retry < 5:
                print(f"Query '{query}' failed with exception: {e}. Retrying.")
                retry += 1
                self.write_query(query, template, query_tuples, page_size, retry)
            else:
                print(f"Query '{query}' failed with exception: {e}. Aborting.")
                exit(-1)
        finally:
            self.close_cursor(cursor)

    def query_for_result(self, query, retry=0):
        try:
            cursor = self.open_cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Exception as e:
            if retry < 5:
                print(f"Query '{query}' failed with exception: {e}. Retrying.")
                retry += 1
                return self.query_for_result(query, retry)
            else:
                print(f"Query '{query}' failed with exception: {e}. Aborting.")
                return None
        finally:
            self.close_cursor(cursor)

    def disconnect(self):
        if self.connection_pool:
            self.connection_pool.closeall()
