"""
Categories Titles repository for Netflix package (Junction table)
"""

from repositories.base_repository import BaseRepository


class CategoriesTitlesRepository(BaseRepository):
    """
    Repository for managing categories-titles relationships
    """

    def __init__(self):
        super().__init__(table_name="public.categories_titles", id_column="category_title_id")

    def get_by_category_and_title(self, category_id, title_id):
        """
        Get relationship by category_id and title_id to avoid duplicates
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE category_id = %s AND title_id = %s",
                (category_id, title_id)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting category-title relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new category-title relationship
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (category_id, title_id) VALUES (%s, %s) RETURNING *",
                (data.get("category_id"), data.get("title_id"))
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating category-title relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_category_id(self, category_id):
        """
        Get all title relationships for a specific category
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE category_id = %s",
                (category_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting titles by category_id: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_title_id(self, title_id):
        """
        Get all category relationships for a specific title
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE title_id = %s",
                (title_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting categories by title_id: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
