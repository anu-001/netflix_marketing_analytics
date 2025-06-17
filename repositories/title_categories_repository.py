"""
Title Categories repository for Netflix package (Junction table)
"""

from repositories.base_repository import BaseRepository


class TitleCategoriesRepository(BaseRepository):
    """
    Repository for managing title-categories relationships
    """

    def __init__(self):
        super().__init__(table_name="public.title_categories", id_column="title_category_id")

    def get_by_title_and_category(self, title_id, category_id):
        """
        Get relationship by title_id and category_id to avoid duplicates
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE title_id = %s AND category_id = %s",
                (title_id, category_id)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting title-category relationship: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new title-category relationship
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name} (title_id, category_id) VALUES (%s, %s) RETURNING *",
                (data.get("title_id"), data.get("category_id"))
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating title-category relationship: {e}")
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
