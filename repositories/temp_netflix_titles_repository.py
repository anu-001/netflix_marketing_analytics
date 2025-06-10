"""
Temp Netflix Titles repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class TempNetflixTitlesRepository(BaseRepository):
    """
    Repository for managing temporary Netflix titles records
    """

    def __init__(self):
        super().__init__(table_name="public.temp_netflix_titles")


    def get_null_directors(self):
        """
        Retrieve records with null directors
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE director = 'unavailable'"
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting records with null directors: {e}")
            raise
        finally:
            if cursor:
                cursor.close()


    def update_director(self, show_id: str, director: str):
        """
        Update the director of a record
        """

        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"UPDATE {self.table_name} SET director = %s WHERE show_id = %s",
                (director, show_id)
            )
            self.db.commit()
        except Exception as e:
            print(f"Error updating director: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_null_actors(self):
        """
        Retrieve records with null cast (actors)
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE {self.table_name}.cast IS NULL"
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting records with null cast: {e}")
            raise
        finally:
            if cursor:
                cursor.close()


    def update_cast(self, show_id: str, cast: str):
        """
        Update the cast of a record
        """

        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f'UPDATE {self.table_name} SET "cast" = %s WHERE show_id = %s',
                (cast, show_id)
            )
            self.db.commit()
        except Exception as e:
            print(f"Error updating cast: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_null_countries(self):
        """
        Retrieve records with null countries
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE country IS NULL"
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting records with null countries: {e}")
            raise
        finally:
            if cursor:
                cursor.close()


    def update_country(self, show_id: str, country: str):
        """
        Update the country of a record
        """

        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"UPDATE {self.table_name} SET country = %s WHERE show_id = %s",
                (country, show_id)
            )
            self.db.commit()
        except Exception as e:
            print(f"Error updating country: {e}")
            raise
        finally:
            if cursor:
                cursor.close()