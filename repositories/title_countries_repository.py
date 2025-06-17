"""
Title Countries repository for Netflix package (Junction table)
"""

from repositories.base_repository import BaseRepository


class TitleCountriesRepository(BaseRepository):
    """
    Repository for managing title-countries relationships
    """

    def __init__(self):
        super().__init__(table_name="public.title_countries", id_column="title_country_id")

    def get_by_title_and_country(self, title_id, country_id):
        """
        Get relationship by title_id and country_id to avoid duplicates
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE title_id = %s AND country_id = %s",
                (title_id, country_id)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting title-country relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new title-country relationship
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (title_id, country_id) VALUES (%s, %s) RETURNING *",
                (data.get("title_id"), data.get("country_id"))
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating title-country relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_title_id(self, title_id):
        """
        Get all country relationships for a specific title
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE title_id = %s",
                (title_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting countries by title_id: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
