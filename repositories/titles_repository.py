"""
Titles repository for Netflix package
"""

from repositories.base_repository import BaseRepository


class TitlesRepository(BaseRepository):
    """
    Repository for managing titles records
    """

    def __init__(self):
        super().__init__(table_name="public.titles", id_column="title_id")

    def get_by_show_id(self, show_id: str):
        """
        Get title by original show_id from temp table
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE show_id = %s",
                (show_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting title by show_id: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_by_title_name(self, title_name: str):
        """
        Get title by title name
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE title = %s",
                (title_name,)
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting title by name: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new title record
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"""INSERT INTO {self.table_name} 
                   (show_id, title, title_type_id, description, release_year, 
                    date_added, duration, rating_id) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
                (
                    data.get("show_id"),
                    data.get("title"), 
                    data.get("title_type_id"),
                    data.get("description"),
                    data.get("release_year"),
                    data.get("date_added"),
                    data.get("duration"),
                    data.get("rating_id")
                )
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating title: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def update(self, title_id, data: dict):
        """
        Update a title record
        """
        try:
            cursor = self.db.get_dict_cursor()
            
            # Build dynamic update query
            set_clauses = []
            values = []
            
            for key, value in data.items():
                if value is not None:
                    set_clauses.append(f"{key} = %s")
                    values.append(value)
            
            if not set_clauses:
                return None
                
            values.append(title_id)
            query = f"UPDATE {self.table_name} SET {', '.join(set_clauses)} WHERE {self.id_column} = %s RETURNING *"
            
            cursor.execute(query, values)
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error updating title: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
