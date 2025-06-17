"""
Countries repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class CountriesRepository(BaseRepository):
    """
    Repository for managing countries records
    """

    def __init__(self):
        super().__init__(table_name="public.countries", id_column="country_id")

    def get_by_description(self, description: str):
        """
        Get country by description (since table only has country_id and description)
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE description = %s",
                (description,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting country by description: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_country_name(self, country_name: str):
        """
        Get country by original country name (now stored directly as description)
        """
        try:
            cursor = self.db.get_dict_cursor()
            # Country name is now stored directly as description
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE description = %s",
                (country_name,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting country by country name: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new country record (table only has country_id and description)
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (description) VALUES (%s) RETURNING *",
                (data.get("description"),)
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating country: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if cursor:
                cursor.close()
