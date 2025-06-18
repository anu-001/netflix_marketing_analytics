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

    def get_by_full_name(self, full_name: str):
        """
        Get person by full name (as it appears in cast column)
        Tries different matching strategies to find the person

        Args:
            full_name (str): Full name to search for

        Returns:
            list: List of matching people records
        """
        try:
            cursor = self.db.get_dict_cursor()

            # Strategy 1: Try exact match by concatenating first_name, middle_name, and last_name
            cursor.execute(
                f"""SELECT * FROM {self.table_name} 
                    WHERE TRIM(CONCAT(
                        first_name, 
                        CASE WHEN middle_name IS NOT NULL AND middle_name != '' THEN ' ' || middle_name ELSE '' END,
                        CASE WHEN last_name IS NOT NULL AND last_name != '' THEN ' ' || last_name ELSE '' END
                    )) = %s""",
                (full_name.strip(),),
            )
            result = cursor.fetchall()

            if result:
                return result

            # Strategy 2: Try fuzzy matching - search in first_name or last_name
            cursor.execute(
                f"""SELECT * FROM {self.table_name} 
                    WHERE first_name ILIKE %s OR last_name ILIKE %s""",
                (f"%{full_name.strip()}%", f"%{full_name.strip()}%"),
            )
            result = cursor.fetchall()

            if result:
                return result

            # Strategy 3: Try matching by splitting the full name
            name_parts = full_name.strip().split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = name_parts[-1]
                cursor.execute(
                    f"""SELECT * FROM {self.table_name} 
                        WHERE first_name ILIKE %s AND last_name ILIKE %s""",
                    (f"%{first_name}%", f"%{last_name}%"),
                )
                result = cursor.fetchall()

            return result

        except Exception as e:
            print(f"Error getting person by full name: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

