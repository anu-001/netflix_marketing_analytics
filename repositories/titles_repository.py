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
        cursor = None
        try:
            if not show_id:
                print("Warning: Empty show_id provided to get_by_show_id")
                return []
                
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE show_id = %s",
                (show_id,)
            )
            result = cursor.fetchall()
            return result if result else []
        except Exception as e:
            print(f"Error getting title by show_id '{show_id}': {e}")
            print(f"Table name: {self.table_name}")
            return []  # Return empty list instead of raising exception
        finally:
            if cursor:
                cursor.close()

    def get_by_title_name(self, title_name: str):
        """
        Get title by title name
        """
        cursor = None
        try:
            if not title_name:
                print("Warning: Empty title_name provided to get_by_title_name")
                return []
                
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE title = %s",
                (title_name,)
            )
            result = cursor.fetchall()
            return result if result else []
        except Exception as e:
            print(f"Error getting title by name '{title_name}': {e}")
            print(f"Table name: {self.table_name}")
            return []  # Return empty list instead of raising exception
        finally:
            if cursor:
                cursor.close()

    def create(self, data: dict):
        """
        Create a new title record with new column structure
        """
        cursor = None
        try:
            if not data.get("name"):
                raise ValueError("Title name is required")
            if not data.get("code"):
                raise ValueError("Title code is required")
                
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"""INSERT INTO {self.table_name} 
                   (name, rating_id, duration_minutes, total_seasons, title_type_id, 
                    date_added, release_year, code, description) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
                (
                    data.get("name"),
                    data.get("rating_id"),
                    data.get("duration_minutes"),
                    data.get("total_seasons"),
                    data.get("title_type_id"),
                    data.get("date_added"),
                    data.get("release_year"),
                    data.get("code"),
                    data.get("description")
                )
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating title '{data.get('name', 'Unknown')}' ({data.get('code', 'Unknown')}): {e}")
            print(f"Title data: {data}")
            self.db.rollback()
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

    def get_by_code(self, code: str):
        """
        Get title by code (show_id)
        """
        cursor = None
        try:
            if not code:
                print("Warning: Empty code provided to get_by_code")
                return []
                
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE code = %s",
                (code,)
            )
            result = cursor.fetchall()
            return result if result else []
        except Exception as e:
            print(f"Error getting title by code '{code}': {e}")
            print(f"Table name: {self.table_name}")
            return []  # Return empty list instead of raising exception
        finally:
            if cursor:
                cursor.close()
