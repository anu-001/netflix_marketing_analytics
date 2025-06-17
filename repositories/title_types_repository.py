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

    def get_by_type_name(self, type_name: str):
        """
        Get title type by type name
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE type_name = %s",
                (type_name,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting title type by name: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new title type record
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (type_name, description) VALUES (%s, %s) RETURNING *",
                (data.get("type_name"), data.get("description"))
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
