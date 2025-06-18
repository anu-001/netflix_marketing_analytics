"""
Title Types repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class TitleTypesRepository(BaseRepository):
    """
    Repository for managing title types records
    """

    def __init__(self):
        super().__init__(table_name="public.title_types", id_column="title_type_id")

    def get_by_description(self, description: str):
        """
        Get title type by description
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE description = %s",
                (description,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting title type by description: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new title type record (only description column, title_type_id is auto-generated)
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
            print(f"Error creating title type: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
