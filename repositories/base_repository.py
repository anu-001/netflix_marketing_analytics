"""
Base repository class for Netflix package
"""

from db.connection import DBConnection


class BaseRepository:
    """
    Base repository class that provides common CRUD operations
    """

    def __init__(self, table_name, id_column="id"):
        """
        Initialize the repository with table name and ID column

        Args:
            table_name (str): The name of the table
            id_column (str): The name of the primary key column
        """
        self.table_name = table_name
        self.id_column = id_column
        self.db = DBConnection()

    def get_all(self):
        """
        Retrieve all records from the table

        Returns:
            list: List of records as dictionaries
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(f"SELECT * FROM {self.table_name}")
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting all records from {self.table_name}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_id(self, id_value):
        """
        Retrieve a record by its ID

        Args:
            id_value: The ID of the record to retrieve

        Returns:
            dict: The record as a dictionary or None if not found
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE {self.id_column} = %s",
                (id_value,),
            )
            return cursor.fetchone()
        except Exception as e:
            print(
                f"Error getting record with ID {id_value} from {self.table_name}: {e}"
            )
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data):
        """
        Create a new record

        Args:
            data (dict): Dictionary containing column names and values

        Returns:
            dict: The created record as a dictionary
        """
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ", ".join(["%s" for _ in columns])
        columns_str = ", ".join(columns)

        try:
            cursor = self.db.get_dict_cursor()
            conn = self.db.get_connection()

            query = f"""
                INSERT INTO {self.table_name} ({columns_str})
                VALUES ({placeholders})
                RETURNING *
            """

            cursor.execute(query, values)
            result = cursor.fetchone()
            conn.commit()

            return result
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error creating record in {self.table_name}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def update(self, id_value, data):
        """
        Update a record

        Args:
            id_value: The ID of the record to update
            data (dict): Dictionary containing column names and values to update

        Returns:
            dict: The updated record as a dictionary
        """
        set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
        values = list(data.values()) + [id_value]

        try:
            cursor = self.db.get_dict_cursor()
            conn = self.db.get_connection()

            query = f"""
                UPDATE {self.table_name}
                SET {set_clause}
                WHERE {self.id_column} = %s
                RETURNING *
            """

            cursor.execute(query, values)
            result = cursor.fetchone()
            conn.commit()

            return result
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error updating record with ID {id_value} in {self.table_name}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def delete(self, id_value):
        """
        Delete a record

        Args:
            id_value: The ID of the record to delete

        Returns:
            bool: True if the record was deleted, False otherwise
        """
        try:
            cursor = self.db.get_dict_cursor()
            conn = self.db.get_connection()

            cursor.execute(
                f"DELETE FROM {self.table_name} WHERE {self.id_column} = %s",
                (id_value,),
            )

            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            if conn:
                conn.rollback()
            print(
                f"Error deleting record with ID {id_value} from {self.table_name}: {e}"
            )
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_field(self, field_name, field_value):
        """
        Retrieve records by a field value

        Args:
            field_name (str): The name of the field to search by
            field_value: The value to search for

        Returns:
            list: List of records as dictionaries
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE {field_name} = %s",
                (field_value,),
            )
            return cursor.fetchall()
        except Exception as e:
            print(
                f"Error getting records with {field_name} = {field_value} from {self.table_name}: {e}"
            )
            raise
        finally:
            if cursor:
                cursor.close()
