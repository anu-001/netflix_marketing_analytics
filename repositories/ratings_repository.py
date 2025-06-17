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
        Get rating by rating value
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE rating = %s",
                (rating,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting rating by rating: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new rating record
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (rating, description) VALUES (%s, %s) RETURNING *",
                (data.get("rating"), data.get("description"))
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
