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

    def get_by_country_name(self, country_name: str):
        """
        Get country by country name
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE country_name = %s",
                (country_name,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting country by name: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new country record
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (country_name, country_code) VALUES (%s, %s) RETURNING *",
                (data.get("country_name"), data.get("country_code"))
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
