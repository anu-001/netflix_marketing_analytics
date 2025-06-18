"""
Actors repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class ActorsRepository(BaseRepository):
    """
    Repository for managing actors records
    The actors table only contains actor_id (which is a FK to people.person_id)
    """

    def __init__(self):
        super().__init__(table_name="public.actors", id_column="actor_id")

    def get_by_actor_id(self, actor_id):
        """
        Get actor by actor_id
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE actor_id = %s",
                (actor_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting actor by actor_id: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def actor_exists(self, actor_id):
        """
        Check if an actor already exists
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT COUNT(*) as count FROM {self.table_name} WHERE actor_id = %s",
                (actor_id,)
            )
            result = cursor.fetchone()
            return result['count'] > 0
        except Exception as e:
            print(f"Error checking if actor exists: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new actor record (only actor_id is needed)
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (actor_id) VALUES (%s) RETURNING *",
                (data.get("actor_id"),)
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
