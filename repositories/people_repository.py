"""
People repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class PeopleRepository(BaseRepository):
    """
    Repository for managing people records
    """

    def __init__(self):
        super().__init__(table_name="public.people", id_column="person_id")

    def get_by_name(self, first_name=None, middle_name=None, last_name=None):
        """
        Search for people by name

        Args:
            first_name (str): First name to search for
            middle_name (str): Middle name to search for
            last_name (str): Last name to search for

        Returns:
            list: List of matching people records
        """
        conditions = []
        params = []

        if first_name:
            conditions.append("first_name ILIKE %s")
            params.append(f"%{first_name}%")

        if middle_name:
            conditions.append("middle_name ILIKE %s")
            params.append(f"%{middle_name}%")

        if last_name:
            conditions.append("last_name ILIKE %s")
            params.append(f"%{last_name}%")

        if not conditions:
            return []

        where_clause = " AND ".join(conditions)

        try:
            cursor = self.db.get_dict_cursor()
            query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            print(f"Error searching for people by name: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

