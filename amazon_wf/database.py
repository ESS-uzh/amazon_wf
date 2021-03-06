from psycopg2 import pool
import pdb



class Database:
    connection_pool = None

    @staticmethod
    def initialise(**kwargs):
        Database.connection_pool = pool.SimpleConnectionPool(1,
                                                            10,
                                                            **kwargs)
    @staticmethod
    def get_connection():
        return Database.connection_pool.getconn()


    @staticmethod
    def return_connection(connection):
        Database.connection_pool.putconn(connection)


    @staticmethod
    def close_all_connections():
        Database.connection_pool.closeall()


class CursorFromConnectionPool:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = Database.get_connection()
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if exception_value:
            self.conn.rollback()
        else:
            self.cursor.close()
            self.conn.commit()
        Database.return_connection(self.conn)

