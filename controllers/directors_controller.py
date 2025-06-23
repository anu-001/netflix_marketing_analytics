import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import re
import unicodedata

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.people_repository import PeopleRepository
from repositories.directors_repository import DirectorsRepository
from repositories.director_titles_repository import DirectorTitlesRepository
from repositories.titles_repository import TitlesRepository
from controllers.common_controller import CommonController


class DirectorsController:
    """
    Controller for managing directors with name parsing and temp_director table processing
    """

    def __init__(self):
        self.conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        self.engine = create_engine(self.conn_string)

    def create_temp_director_table(self):
        """
        Part 1: Parse Director Names and Populate temp_director Table
        
        Read director column from temp_netflix_titles, parse names by word count,
        check/create people records, and populate temp_director table.
        """
        print("🎬 Starting director name parsing and temp_director table creation...")
        
        # Get all records from temp_netflix_titles
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        records = temp_netflix_titles_repo.get_all()
        
        directors_list = []
        processed_count = 0
        
        for record in records:
            show_id = record.get("show_id")
            director_column = record.get("director")
            
            # Skip if no director data
            if not director_column or director_column.strip() in ["", "unknown", "Unknown"]:
                continue
                
            # Split director string by commas to extract individual names
            raw_director_names = director_column.split(",")
            
            for raw_name in raw_director_names:
                # Trim leading and trailing whitespace
                clean_name = raw_name.strip()
                if not clean_name:
                    continue
                    
                # Parse name by word count
                parsed_names = self.parse_director_name(clean_name)
                if not parsed_names:
                    continue
                    
                first_name = parsed_names["first_name"]
                middle_name = parsed_names["middle_name"]
                last_name = parsed_names["last_name"]
                
                # Check if matching person exists in people table
                director_id = self.get_or_create_person(first_name, middle_name, last_name)
                
                if director_id:
                    directors_list.append({
                        "first_name": first_name,
                        "middle_name": middle_name,
                        "last_name": last_name,
                        "director_id": director_id,
                        "show_id": show_id,
                        "processed": False
                    })
                    processed_count += 1
                    
                    if processed_count % 100 == 0:
                        print(f"   Processed {processed_count} director entries...")
        
        print(f"✅ Parsed {len(directors_list)} director entries from {len(records)} titles")
        
        # Create temp_director table
        if directors_list:
            directors_df = pd.DataFrame(directors_list)
            
            # Save to PostgreSQL temp_director table
            table_name = "temp_director"
            schema = "public"
            
            directors_df.to_sql(
                name=table_name, 
                con=self.engine.connect(), 
                schema=schema, 
                if_exists="replace", 
                index=False
            )
            print(f"✅ Successfully created '{table_name}' table with {len(directors_list)} records")
        else:
            print("⚠️ No director data found to process")

    def parse_director_name(self, full_name):
        """
        Parse director name by word count as specified in requirements:
        - 3 words: first_name, middle_name, last_name
        - 2 words: first_name, last_name (middle_name = NULL)
        - 1 word: first_name (middle_name = NULL, last_name = NULL)
        """
        if not full_name or not full_name.strip():
            return None
            
        # Clean and split the name
        words = full_name.strip().split()
        word_count = len(words)
        
        if word_count == 3:
            # Three words: first_name, middle_name, last_name
            # For "Jacinth Tan Yi Ting" -> first=Jacinth, middle=Tan, last=Yi Ting
            return {
                "first_name": words[0],
                "middle_name": words[1],
                "last_name": " ".join(words[2:]) if len(words) > 2 else words[2]
            }
        elif word_count == 2:
            # Two words: first_name, last_name; middle_name = NULL
            return {
                "first_name": words[0],
                "middle_name": None,
                "last_name": words[1]
            }
        elif word_count == 1:
            # One word: first_name; middle_name = NULL, last_name = NULL
            return {
                "first_name": words[0],
                "middle_name": None,
                "last_name": None
            }
        else:
            # More than 3 words: treat as first_name, middle_name, rest as last_name
            return {
                "first_name": words[0],
                "middle_name": words[1] if len(words) > 1 else None,
                "last_name": " ".join(words[2:]) if len(words) > 2 else None
            }
    
    def get_or_create_person(self, first_name, middle_name, last_name):
        """
        Check if matching person exists in people table, create if not found.
        Returns person_id (BIGINT) or None if error.
        """
        people_repo = PeopleRepository()
        
        # Check for exact match using first_name, middle_name, last_name
        existing_person = people_repo.get_by_name(first_name, middle_name, last_name)
        
        if existing_person:
            return existing_person[0]["person_id"]
        
        # No match found, create new person record
        try:
            person_data = {
                "first_name": first_name,
                "middle_name": middle_name,
                "last_name": last_name
            }
            
            created_person = people_repo.create(person_data)
            if created_person:
                print(f"   ✅ Created new person: {first_name} {middle_name or ''} {last_name or ''}".strip())
                return created_person["person_id"]
        except Exception as e:
            print(f"   ❌ Error creating person {first_name} {middle_name or ''} {last_name or ''}: {e}")
            
        return None

    def populate_directors_table_from_temp(self):
        """
        Part 2: Transfer Unique Directors to directors Table
        
        Select all distinct director_id values from temp_director where processed = FALSE,
        insert unique ones into directors table (which has structure: director_id BIGINT NOT NULL),
        then mark all matching rows as processed.
        """
        print("🎬 Starting transfer from temp_director to directors table...")
        
        try:
            # Get distinct director_id values from temp_director where processed = FALSE
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT DISTINCT director_id 
                        FROM public.temp_director 
                        WHERE processed = FALSE 
                        ORDER BY director_id
                    """)
                )
                distinct_director_ids = [row.director_id for row in result.fetchall()]
            
            if not distinct_director_ids:
                print("✅ No unprocessed director IDs found in temp_director table")
                return
                
            print(f"📋 Found {len(distinct_director_ids)} distinct unprocessed director IDs")
            
            inserted_count = 0
            existing_count = 0
            error_count = 0
            successfully_processed_director_ids = []
            
            for director_id in distinct_director_ids:
                try:
                    # Check if this director_id already exists in directors table
                    with self.engine.connect() as conn:
                        existing_check = conn.execute(
                            text("SELECT director_id FROM public.directors WHERE director_id = :director_id"),
                            {"director_id": director_id}
                        )
                        existing_director = existing_check.fetchone()
                    
                    if not existing_director:
                        # Insert new director into directors table
                        with self.engine.connect() as conn:
                            conn.execute(
                                text("INSERT INTO public.directors (director_id) VALUES (:director_id)"),
                                {"director_id": director_id}
                            )
                            conn.commit()
                            inserted_count += 1
                            successfully_processed_director_ids.append(director_id)
                            
                            if inserted_count % 50 == 0:
                                print(f"   ✅ Inserted {inserted_count} new directors...")
                    else:
                        existing_count += 1
                        successfully_processed_director_ids.append(director_id)  # Still mark as processed
                        if existing_count % 50 == 0:
                            print(f"   🟡 {existing_count} directors already exist...")
                    
                except Exception as e:
                    error_count += 1
                    print(f"   ❌ Error processing director_id {director_id}: {e}")
                    # Don't add to successfully_processed_director_ids if there was an error
            
            # Mark all rows with successfully processed director_ids as processed
            if successfully_processed_director_ids:
                print("🔄 Marking all matching rows as processed...")
                processed_count = 0
                
                try:
                    with self.engine.connect() as conn:
                        # Build the SQL for bulk update
                        director_ids_str = ','.join(map(str, successfully_processed_director_ids))
                        
                        update_result = conn.execute(
                            text(f"""
                                UPDATE public.temp_director 
                                SET processed = TRUE 
                                WHERE director_id IN ({director_ids_str}) AND processed = FALSE
                            """)
                        )
                        processed_count = update_result.rowcount
                        conn.commit()
                        
                except Exception as e:
                    print(f"   ❌ Error marking directors as processed: {e}")
                    # Try individual updates as fallback
                    for director_id in successfully_processed_director_ids:
                        try:
                            with self.engine.connect() as conn:
                                result = conn.execute(
                                    text("""
                                        UPDATE public.temp_director 
                                        SET processed = TRUE 
                                        WHERE director_id = :director_id AND processed = FALSE
                                    """),
                                    {"director_id": director_id}
                                )
                                processed_count += result.rowcount
                                conn.commit()
                        except Exception as inner_e:
                            print(f"   ❌ Error marking director_id {director_id} as processed: {inner_e}")
            
            print(f"✅ Transfer complete:")
            print(f"   • {inserted_count} new directors inserted")
            print(f"   • {existing_count} directors already existed") 
            print(f"   • {processed_count} temp_director rows marked as processed")
            print(f"   • {error_count} errors encountered")
            
        except Exception as e:
            print(f"❌ Error in populate_directors_table_from_temp: {e}")
            raise

    def mark_temp_director_as_processed(self, director_id, show_id=None):
        """
        Mark director record as processed in temp_director table
        If show_id is None, mark all records for this director_id
        """
        try:
            with self.engine.connect() as conn:
                if show_id is not None:
                    conn.execute(
                        text("""
                            UPDATE public.temp_director 
                            SET processed = TRUE 
                            WHERE director_id = :director_id AND show_id = :show_id
                        """),
                        {"director_id": director_id, "show_id": show_id}
                    )
                else:
                    conn.execute(
                        text("""
                            UPDATE public.temp_director 
                            SET processed = TRUE 
                            WHERE director_id = :director_id
                        """),
                        {"director_id": director_id}
                    )
                conn.commit()
        except Exception as e:
            print(f"❌ Error marking director as processed: {e}")
            raise

    def check_processing_status(self):
        """
        Check processing status of temp_director table
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
                            COUNT(CASE WHEN processed = FALSE THEN 1 END) as unprocessed_records
                        FROM public.temp_director
                    """)
                )
                status = result.fetchone()
                
                print(f"📊 temp_director processing status:")
                print(f"   Total records: {status.total_records}")
                print(f"   Processed: {status.processed_records}")
                print(f"   Unprocessed: {status.unprocessed_records}")
                
        except Exception as e:
            print(f"❌ Error checking processing status: {e}")

    # Legacy methods (keeping for backward compatibility)
    def create_temp_directors_table(self):
        """Legacy method - redirects to new method"""
        return self.create_temp_director_table()
