"""
Director Titles repository for Netflix package (Junction table)
"""

from repositories.base_repository import BaseRepository


class DirectorTitlesRepository(BaseRepository):
    """
    Repository for managing director-titles relationships
    """

    def __init__(self):
        super().__init__(table_name="public.director_titles", id_column="director_title_id")

    def get_by_person_and_title(self, person_id, title_id):
        """
        Get director-title relationship by person_id and title_id to avoid duplicates
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE person_id = %s AND title_id = %s",
                (person_id, title_id)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting director-title relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new director-title relationship
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (person_id, title_id) VALUES (%s, %s) RETURNING *",
                (data.get("person_id"), data.get("title_id"))
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating director-title relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_person_id(self, person_id):
        """
        Get all director-title relationships for a specific person
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE person_id = %s",
                (person_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting director-titles by person_id: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_title_id(self, title_id):
        """
        Get all director-title relationships for a specific title
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE title_id = %s",
                (title_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting director-titles by title_id: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
