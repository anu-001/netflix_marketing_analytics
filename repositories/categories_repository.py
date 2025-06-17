"""
Categories repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class CategoriesRepository(BaseRepository):
    """
    Repository for managing categories records
    """

    def __init__(self):
        super().__init__(table_name="public.categories", id_column="category_id")

    def get_by_category_name(self, category_name: str):
        """
        Get category by category name
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE category_name = %s",
                (category_name,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting category by name: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new category record
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (category_name, description) VALUES (%s, %s) RETURNING *",
                (data.get("category_name"), data.get("description"))
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating category: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
