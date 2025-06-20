"""
Ratings repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class RatingsRepository(BaseRepository):
    """
    Repository for managing ratings records
    """

    def __init__(self):
        super().__init__(table_name="public.ratings", id_column="rating_id")

    def get_by_rating(self, rating: str):
        """
        Get rating by code value (the rating code like 'PG-13', 'R', etc.)
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE code = %s",
                (rating,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting rating by code: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_name(self, name: str):
        """
        Get rating by name/code (alias for get_by_code for consistency)
        """
        return self.get_by_code(name)

    def create(self, data: dict):
        """
        Create a new rating record (rating_id auto-generated, code and description)
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (code, description) VALUES (%s, %s) RETURNING *",
                (data.get("code"), data.get("description"))
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating rating: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_code(self, code: str):
        """
        Get rating by code (e.g., 'PG-13', 'R', 'TV-MA', etc.)
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE code = %s",
                (code,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting rating by code: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
