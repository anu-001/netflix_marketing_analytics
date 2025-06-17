"""
Base tracking controller for Netflix package
"""

from repositories.processing_status_repository import ProcessingStatusRepository
from datetime import datetime


class BaseTrackingController:
    """
    Base controller that provides tracking functionality for all data processing
    """

    def __init__(self):
        self.processing_repo = ProcessingStatusRepository()
        self.current_run_id = None
        self.records_processed = 0
        self.records_created = 0
        self.records_skipped = 0

    def start_processing_run(self, table_name: str, description: str = None):
        """
        Start a new processing run and return the run ID
        """
        try:
            run = self.processing_repo.create_processing_run(table_name, description)
            self.current_run_id = run['status_id']
            self.records_processed = 0
            self.records_created = 0
            self.records_skipped = 0
            print(f"🚀 Started processing run {self.current_run_id} for {table_name}")
            return self.current_run_id
        except Exception as e:
            print(f"❌ Error starting processing run: {e}")
            return None

    def update_processing_progress(self, status: str = 'processing', error_message: str = None):
        """
        Update the current processing run with progress
        """
        if not self.current_run_id:
            return
            
        try:
            self.processing_repo.update_processing_status(
                self.current_run_id, 
                status, 
                self.records_processed,
                self.records_created,
                self.records_skipped,
                error_message
            )
            
            if status in ['completed', 'failed']:
                print(f"📊 Processing run {self.current_run_id} {status}:")
                print(f"   ├── Records processed: {self.records_processed}")
                print(f"   ├── Records created: {self.records_created}")
                print(f"   └── Records skipped: {self.records_skipped}")
                
        except Exception as e:
            print(f"❌ Error updating processing progress: {e}")

    def increment_processed(self):
        """Increment processed count"""
        self.records_processed += 1

    def increment_created(self):
        """Increment created count"""
        self.records_created += 1

    def increment_skipped(self):
        """Increment skipped count"""
        self.records_skipped += 1

    def complete_processing_run(self):
        """Mark the current processing run as completed"""
        self.update_processing_progress('completed')

    def fail_processing_run(self, error_message: str):
        """Mark the current processing run as failed"""
        self.update_processing_progress('failed', error_message)

    def get_processing_summary(self):
        """Get summary of all processing runs"""
        return self.processing_repo.get_processing_summary()

    def get_table_status(self, table_name: str):
        """Get the latest processing status for a specific table"""
        runs = self.processing_repo.get_latest_processing_runs(table_name, 1)
        return runs[0] if runs else None

    def check_if_table_processed(self, table_name: str, max_age_hours: int = 24):
        """
        Check if a table has been successfully processed recently
        """
        latest = self.get_table_status(table_name)
        if not latest:
            return False
            
        if latest['status'] != 'completed':
            return False
            
        # Check if it's recent enough
        if latest['end_time']:
            time_diff = datetime.now() - latest['end_time']
            if time_diff.total_seconds() > max_age_hours * 3600:
                return False
                
        return True

    def should_skip_processing(self, table_name: str, force_reprocess: bool = False):
        """
        Determine if processing should be skipped for a table
        """
        if force_reprocess:
            return False
            
        return self.check_if_table_processed(table_name)

    def print_processing_dashboard(self):
        """
        Print a dashboard showing processing status of all tables
        """
        summary = self.get_processing_summary()
        
        print("\n" + "="*80)
        print("📊 NETFLIX DATA PROCESSING DASHBOARD")
        print("="*80)
        
        if not summary:
            print("No processing runs found.")
            return
            
        print(f"{'Table Name':<20} {'Runs':<6} {'✅ Done':<8} {'❌ Failed':<9} {'📝 Processed':<12} {'➕ Created':<10} {'Last Run':<19}")
        print("-" * 80)
        
        for row in summary:
            table_name = row['table_name'][:19]
            total_runs = row['total_runs']
            completed = row['completed_runs']
            failed = row['failed_runs']
            processed = row['total_records_processed'] or 0
            created = row['total_records_created'] or 0
            last_run = row['last_run_time'].strftime('%Y-%m-%d %H:%M') if row['last_run_time'] else 'Never'
            
            print(f"{table_name:<20} {total_runs:<6} {completed:<8} {failed:<9} {processed:<12} {created:<10} {last_run:<19}")
        
        print("="*80)
