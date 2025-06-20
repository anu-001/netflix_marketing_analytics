"""
Database connection manager for Netflix package
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG


class DBConnection:
    """
    A database connection manager for PostgreSQL
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DBConnection, cls).__new__(cls)
            cls._instance._conn = None
        return cls._instance

    def connect(self):
        """
        Establish a database connection if one doesn't exist
        """
        if self._conn is None:
            try:
                self._conn = psycopg2.connect(
                    host=DB_CONFIG["host"],
                    database=DB_CONFIG["database"],
                    user=DB_CONFIG["user"],
                    password=DB_CONFIG["password"],
                    port=DB_CONFIG["port"],
                )
            except Exception as e:
                print(f"Error connecting to the database: {e}")
                raise
        return self._conn

    def get_connection(self):
        """
        Return an existing connection or create a new one
        """
        return self.connect()

    def get_cursor(self, cursor_factory=None):
        """
        Return a cursor for executing queries
        """
        conn = self.get_connection()
        if cursor_factory:
            return conn.cursor(cursor_factory=cursor_factory)
        return conn.cursor()

    def get_dict_cursor(self):
        """
        Return a cursor that returns results as dictionaries
        """
        return self.get_cursor(cursor_factory=RealDictCursor)

    def commit(self):
        """
        Commit the current transaction
        """
        if self._conn is not None:
            self._conn.commit()

    def rollback(self):
        """
        Rollback the current transaction
        """
        if self._conn is not None:
            self._conn.rollback()

    def close(self):
        """
        Close the database connection
        """
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            print("Database connection closed")
