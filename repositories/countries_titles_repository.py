"""
Countries Titles repository for Netflix package (Junction table)
"""

from repositories.base_repository import BaseRepository


class CountriesTitlesRepository(BaseRepository):
    """
    Repository for managing countries-titles relationships
    """

    def __init__(self):
        super().__init__(table_name="public.countries_titles", id_column="country_title_id")

    def get_by_country_and_title(self, country_id, title_id):
        """
        Get relationship by country_id and title_id to avoid duplicates
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE country_id = %s AND title_id = %s",
                (country_id, title_id)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting country-title relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new country-title relationship
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (country_id, title_id) VALUES (%s, %s) RETURNING *",
                (data.get("country_id"), data.get("title_id"))
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating country-title relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_country_id(self, country_id):
        """
        Get all title relationships for a specific country
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE country_id = %s",
                (country_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting titles by country_id: {e}")
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
