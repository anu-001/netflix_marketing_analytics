"""
Processing Status repository for Netflix package
"""

from repositories.base_repository import BaseRepository
from datetime import datetime


class ProcessingStatusRepository(BaseRepository):
    """
    Repository for tracking processing status of data imports
    """

    def __init__(self):
        super().__init__(table_name="public.processing_status", id_column="status_id")

    def create_processing_run(self, table_name: str, description: str = None):
        """
        Create a new processing run record
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"""INSERT INTO {self.table_name} 
                   (table_name, description, status, start_time, created_at) 
                   VALUES (%s, %s, %s, %s, %s) RETURNING *""",
                (table_name, description, 'started', datetime.now(), datetime.now())
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error creating processing run: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def update_processing_status(self, status_id: int, status: str, records_processed: int = 0, 
                               records_created: int = 0, records_skipped: int = 0, error_message: str = None):
        """
        Update processing status
        """
        try:
            cursor = self.db.get_dict_cursor()
            
            end_time = datetime.now() if status in ['completed', 'failed'] else None
            
            cursor.execute(
                f"""UPDATE {self.table_name} 
                   SET status = %s, records_processed = %s, records_created = %s, 
                       records_skipped = %s, error_message = %s, end_time = %s, updated_at = %s
                   WHERE {self.id_column} = %s RETURNING *""",
                (status, records_processed, records_created, records_skipped, 
                 error_message, end_time, datetime.now(), status_id)
            )
            result = cursor.fetchone()
            self.db.commit()
            return result
        except Exception as e:
            print(f"Error updating processing status: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_latest_processing_runs(self, table_name: str = None, limit: int = 10):
        """
        Get latest processing runs
        """
        try:
            cursor = self.db.get_dict_cursor()
            
            if table_name:
                cursor.execute(
                    f"""SELECT * FROM {self.table_name} 
                       WHERE table_name = %s 
                       ORDER BY created_at DESC LIMIT %s""",
                    (table_name, limit)
                )
            else:
                cursor.execute(
                    f"""SELECT * FROM {self.table_name} 
                       ORDER BY created_at DESC LIMIT %s""",
                    (limit,)
                )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting processing runs: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_processing_summary(self):
        """
        Get summary of all processing runs
        """
        try:
            cursor = self.db.get_dict_cursor()
            cursor.execute(
                f"""SELECT 
                       table_name,
                       COUNT(*) as total_runs,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_runs,
                       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
                       SUM(records_processed) as total_records_processed,
                       SUM(records_created) as total_records_created,
                       MAX(created_at) as last_run_time
                   FROM {self.table_name}
                   GROUP BY table_name
                   ORDER BY last_run_time DESC"""
            )
            return cursor.fetchall()
        except Exception as e:
            print(f"Error getting processing summary: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
