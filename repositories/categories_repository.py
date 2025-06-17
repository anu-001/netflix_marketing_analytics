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

    def get_by_description(self, description: str):
        """
        Get category by description (since table only has category_id and description)
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE description = %s",
                (description,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting category by description: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_category_name(self, category_name: str):
        """
        Get category by original category name (now needs normalization lookup)
        """
        try:
            # Import here to avoid circular import
            from controllers.categories_controller import CategoriesController
            
            # Normalize the category name first
            controller = CategoriesController()
            normalized_name = controller.normalize_category_name(category_name)
            
            cursor = self.db.get_dict_cursor()
            # Look for the normalized category name in the description field
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE description = %s",
                (normalized_name,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting category by category name: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new category record (table only has category_id and description)
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
            print(f"Error creating category: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
