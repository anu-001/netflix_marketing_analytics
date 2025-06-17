"""
Actors repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class ActorsRepository(BaseRepository):
    """
    Repository for managing actors records
    """

    def __init__(self):
        super().__init__(table_name="public.actors", id_column="actor_id")

    def get_by_person_and_title(self, person_id, title_id):
        """
        Get actor by person_id and title_id to avoid duplicates
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE person_id = %s AND title_id = %s",
                (person_id, title_id)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting actor by person and title: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new actor record
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
            print(f"Error creating actor: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_person_id(self, person_id):
        """
        Get all actor records for a specific person
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE person_id = %s",
                (person_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting actors by person_id: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
