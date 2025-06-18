"""
Title Types controller for Netflix package
Handles title_types table with title_type_id (auto) and description columns
"""

import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG
from repositories.title_types_repository import TitleTypesRepository
from controllers.base_tracking_controller import BaseTrackingController


class TitleTypesController(BaseTrackingController):
    """
    Controller for managing title_types table
    The title_types table contains title_type_id (auto) and description
    """

    def __init__(self):
        super().__init__()

    def create_temp_title_types_table(self):
        """
        Create a temporary title_types table from the type column in temp_netflix_titles.
        Extracts distinct type values.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_title_types", "Creating temporary title_types table from type column")
        
        try:
            # Connect to database
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Read distinct type values from temp_netflix_titles
            df = pd.read_sql(
                'SELECT DISTINCT "type" FROM public.temp_netflix_titles WHERE "type" IS NOT NULL AND "type" != \'unknown\' ORDER BY "type"',
                con=engine
            )

            title_types_list = []
            
            print(f"Processing {len(df)} distinct type values...")
            
            for _, record in df.iterrows():
                type_value = record["type"].strip() if record["type"] else None
                
                if type_value and type_value != "unknown":
                    title_types_list.append({
                        "type_description": type_value,
                        "processed": False
                    })

            print(f"Found {len(title_types_list)} unique title types")

            # Create DataFrame and save to database
            if title_types_list:
                title_types_df = pd.DataFrame(title_types_list)
                title_types_df.to_sql(
                    name="temp_title_types", 
                    con=engine, 
                    schema="public", 
                    if_exists="replace", 
                    index=False
                )
                print(f"Successfully created temp_title_types table with {len(title_types_list)} records")
            else:
                print("No title type data found to process")
            
            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"Error creating temp_title_types table: {e}")
            raise

    def populate_title_types_table_from_temp(self):
        """
        Populate the title_types table from temp_title_types where processed = FALSE.
        Only stores unique description values.
        """
        # Start tracking
        run_id = self.start_processing_run("title_types", "Populating title_types table from temp_title_types")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT type_description FROM public.temp_title_types WHERE processed = FALSE ORDER BY type_description',
                con=engine
            )
            
            if result_df.empty:
                print("No unprocessed title types found")
                self.complete_processing_run()
                return
                
            temp_title_types = result_df.to_dict(orient="records")
            title_types_repo = TitleTypesRepository()

            print(f"Processing {len(temp_title_types)} unprocessed title type records...")

            for record in temp_title_types:
                self.increment_processed()
                
                type_description = record["type_description"]
                
                print(f"🔍 Processing title type: {type_description}")

                # Check if title type already exists by description
                existing_title_type = title_types_repo.get_by_description(type_description)

                if not existing_title_type:
                    # Create new title type
                    created_title_type = title_types_repo.create({"description": type_description})
                    print(f"✅ Created new title type: {created_title_type}")
                    self.increment_created()
                else:
                    print(f"🟡 Title type already exists: {existing_title_type[0]}")
                    self.increment_skipped()

                # Mark as processed
                self.mark_as_processed(engine, type_description)
                
                # Update progress every 5 records
                if self.records_processed % 5 == 0:
                    self.update_processing_progress()

            print(f"\n📊 Summary:")
            print(f"   - Total title types processed: {self.records_processed}")
            print(f"   - New title types created: {self.records_created}")
            print(f"   - Title types skipped (already exist): {self.records_skipped}")

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"Error populating title_types table: {e}")
            raise

    def mark_as_processed(self, engine, type_description):
        """
        Mark title type as processed in temp_title_types table
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE public.temp_title_types SET processed = TRUE WHERE type_description = :type_description"),
                    {"type_description": type_description}
                )
                conn.commit()
                if result.rowcount > 0:
                    print(f"✅ Marked '{type_description}' as processed")
                else:
                    print(f"⚠️ No rows updated for '{type_description}'")
        except Exception as e:
            print(f"❌ Error marking '{type_description}' as processed: {e}")
            # Don't raise the exception - we want to continue processing other records

    def check_processing_status(self):
        """
        Check the processing status of temp_title_types table
        """
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            # Get processing statistics
            stats_df = pd.read_sql('''
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE processed = TRUE) as processed_records,
                    COUNT(*) FILTER (WHERE processed = FALSE) as unprocessed_records,
                    ROUND(COUNT(*) FILTER (WHERE processed = TRUE) * 100.0 / COUNT(*), 2) as completion_percentage
                FROM public.temp_title_types
            ''', con=engine)
            
            stats = stats_df.iloc[0]
            
            print("📊 TEMP_TITLE_TYPES PROCESSING STATUS:")
            print(f"   Total records: {stats['total_records']}")
            print(f"   Processed: {stats['processed_records']}")
            print(f"   Remaining: {stats['unprocessed_records']}")
            print(f"   Completion: {stats['completion_percentage']}%")
            
            return {
                'total': stats['total_records'],
                'processed': stats['processed_records'],
                'remaining': stats['unprocessed_records'],
                'completion_percentage': stats['completion_percentage']
            }
            
        except Exception as e:
            print(f"❌ Error checking processing status: {e}")
            return None
