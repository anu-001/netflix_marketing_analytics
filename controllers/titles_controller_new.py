"""
Titles controller for Netflix package
Handles titles table with proper ETL pattern using temp_titles and processed flags
"""

import pandas as pd
import re
from datetime import datetime
from sqlalchemy import create_engine, text

from config import DB_CONFIG
from repositories.titles_repository import TitlesRepository
from repositories.title_types_repository import TitleTypesRepository
from repositories.ratings_repository import RatingsRepository
from controllers.base_tracking_controller import BaseTrackingController
from controllers.gemini_controller import GeminiController


class TitlesController(BaseTrackingController):
    """
    Controller for managing titles table ETL process.
    
    Processes titles from temp_netflix_titles table and creates normalized
    records in the titles table with proper foreign key relationships.
    """

    def __init__(self):
        super().__init__()
        self.titles_repo = TitlesRepository()
        self.title_types_repo = TitleTypesRepository()
        self.ratings_repo = RatingsRepository()
        self.gemini_controller = GeminiController()

    def create_temp_titles_table(self):
        """
        Create a temporary titles table from temp_netflix_titles.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_titles", "Creating temporary titles table from temp_netflix_titles")
        
        try:
            # Connect to database
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Read data from temp_netflix_titles
            df = pd.read_sql(
                '''SELECT show_id, title, type, description, release_year, 
                          date_added, duration, rating 
                   FROM public.temp_netflix_titles 
                   WHERE show_id IS NOT NULL 
                   ORDER BY show_id''',
                con=engine
            )

            titles_list = []
            
            print(f"Processing {len(df)} title records...")
            
            for _, record in df.iterrows():
                show_id = record.get("show_id")
                title = record.get("title")
                type_val = record.get("type")
                description = record.get("description")
                release_year = record.get("release_year")
                date_added = record.get("date_added")
                duration = record.get("duration")
                rating = record.get("rating")
                
                if show_id and title:
                    # Parse duration into minutes and seasons
                    duration_minutes, total_seasons = self.parse_duration(duration)
                    
                    titles_list.append({
                        "show_id": show_id,
                        "title": title,
                        "type": type_val,
                        "description": description,
                        "release_year": release_year,
                        "date_added": date_added,
                        "duration": duration,
                        "duration_minutes": duration_minutes,
                        "total_seasons": total_seasons,
                        "rating": rating,
                        "processed": False
                    })

            print(f"Found {len(titles_list)} unique titles")

            # Create DataFrame and save to database
            if titles_list:
                titles_df = pd.DataFrame(titles_list)
                titles_df.to_sql(
                    name="temp_titles", 
                    con=engine, 
                    schema="public", 
                    if_exists="replace", 
                    index=False
                )
                print(f"Successfully created temp_titles table with {len(titles_list)} records")
            else:
                print("No title data found to process")
            
            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"Error creating temp_titles table: {e}")
            raise

    def parse_duration(self, duration_str):
        """
        Parse duration string to extract minutes and seasons
        Examples:
        - "90 min" -> (90, None)
        - "2 Seasons" -> (None, 2)
        - "1 Season" -> (None, 1)
        """
        duration_minutes = None
        total_seasons = None
        
        if not duration_str or pd.isna(duration_str):
            return duration_minutes, total_seasons
        
        duration_str = str(duration_str).strip()
        
        # Check for minutes
        min_match = re.search(r'(\d+)\s*min', duration_str, re.IGNORECASE)
        if min_match:
            duration_minutes = int(min_match.group(1))
        
        # Check for seasons
        season_match = re.search(r'(\d+)\s*season', duration_str, re.IGNORECASE)
        if season_match:
            total_seasons = int(season_match.group(1))
        
        return duration_minutes, total_seasons

    def populate_titles_table_from_temp(self):
        """
        Populate the titles table from temp_titles where processed = FALSE.
        Uses Gemini AI for intelligent rating deduction when needed.
        """
        run_id = self.start_processing_run("titles", "Populating titles table from temp_titles")
        
        try:
            engine = self._get_db_engine()
            unprocessed_titles = self._load_unprocessed_titles(engine)
            
            if not unprocessed_titles:
                print("No unprocessed titles found")
                self.complete_processing_run()
                return
                
            self._process_title_records(engine, unprocessed_titles)
            
            print(f"\n📊 Processing Summary:")
            print(f"   - Total processed: {self.records_processed}")
            print(f"   - New titles created: {self.records_created}")  
            print(f"   - Titles skipped (already exist): {self.records_skipped}")

            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"Error populating titles table: {e}")
            raise

    def _get_db_engine(self):
        """Create database engine connection."""
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        return create_engine(conn_string)

    def _load_unprocessed_titles(self, engine):
        """Load unprocessed title records from temp_titles table."""
        # Show processing status
        self._show_processing_status(engine)
        
        # Load unprocessed records
        result_df = pd.read_sql(
            '''SELECT show_id, title, type, description, release_year, 
                      date_added, duration_minutes, total_seasons, rating
               FROM public.temp_titles 
               WHERE processed = FALSE 
               ORDER BY show_id''',
            con=engine
        )
        
        titles = result_df.to_dict(orient="records") if not result_df.empty else []
        print(f"Found {len(titles)} unprocessed title records")
        return titles

    def _show_processing_status(self, engine):
        """Display current processing status."""
        print("📊 Checking processing status...")
        with engine.connect() as conn:
            status_result = conn.execute(
                text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
                    COUNT(CASE WHEN processed = FALSE THEN 1 END) as unprocessed_records
                FROM public.temp_titles
                """)
            )
            status = status_result.fetchone()
            print(f"📋 Total: {status[0]} | ✅ Processed: {status[1]} | ⏳ Remaining: {status[2]}")

    def _process_title_records(self, engine, titles):
        """Process each title record."""
        for record in titles:
            self.increment_processed()
            show_id = record["show_id"]
            title_name = record["title"]
            
            print(f"🔍 Processing: {title_name} ({show_id})")

            try:
                if self._title_already_exists(show_id):
                    self._handle_existing_title(engine, show_id)
                    continue

                title_data = self._build_title_data(record)
                self._create_new_title(engine, title_data, show_id)
                
            except Exception as e:
                print(f"❌ Error processing '{title_name}' ({show_id}): {e}")
                continue  # Leave as unprocessed for retry
                
            if self.records_processed % 10 == 0:
                self.update_processing_progress()

    def _title_already_exists(self, show_id):
        """Check if title already exists by code."""
        try:
            existing_title = self.titles_repo.get_by_code(show_id)
            return len(existing_title) > 0
        except Exception as e:
            print(f"⚠️ Error checking existing title for '{show_id}': {e}")
            return False

    def _handle_existing_title(self, engine, show_id):
        """Handle case where title already exists."""
        print(f"🟡 Title already exists, skipping")
        self.increment_skipped()
        self.mark_as_processed(engine, show_id)

    def _build_title_data(self, record):
        """Build title data dictionary from record."""
        return {
            "name": record["title"],
            "rating_id": self.get_rating_id_with_gemini(
                record.get("rating"), 
                record["title"], 
                record.get("description"), 
                record.get("release_year")
            ),
            "duration_minutes": self._get_safe_value(record, "duration_minutes"),
            "total_seasons": self._get_safe_value(record, "total_seasons"),
            "title_type_id": self.get_title_type_id(record["type"]),
            "date_added": self.parse_date(record.get("date_added")),
            "release_year": self._get_safe_value(record, "release_year"),
            "code": record["show_id"],
            "description": self._get_safe_value(record, "description")
        }

    def _get_safe_value(self, record, key):
        """Get value from record, returning None for NaN values."""
        value = record.get(key)
        return value if pd.notna(value) else None

    def _create_new_title(self, engine, title_data, show_id):
        """Create new title record."""
        created_title = self.titles_repo.create(title_data)
        print(f"✅ Created: {created_title}")
        self.increment_created()
        self.mark_as_processed(engine, show_id)
        print(f"✅ Marked as processed: {show_id}")

    def get_rating_id(self, rating):
        """
        Get rating_id from ratings table by looking up the name
        """
        if not rating or rating == "unknown" or pd.isna(rating):
            return None
            
        try:
            existing_rating = self.ratings_repo.get_by_name(rating)
            if existing_rating:
                return existing_rating[0]["rating_id"]
        except Exception as e:
            print(f"Error getting rating ID for '{rating}': {e}")
        return None

    def is_valid_rating(self, rating):
        """
        Check if the rating value is actually a valid content rating (not duration, etc.)
        """
        if not rating or pd.isna(rating):
            return False
            
        rating_str = str(rating).strip()
        
        # Check if it looks like a duration (contains "min", "season", numbers only, etc.)
        invalid_patterns = [
            r'^\d+$',  # Just numbers
            r'\d+\s*min',  # Duration in minutes
            r'\d+\s*season',  # Seasons
            r'\d+\s*hr',  # Hours
        ]
        
        for pattern in invalid_patterns:
            if re.search(pattern, rating_str, re.IGNORECASE):
                return False
                
        # Check if it's a reasonable length for a rating
        if len(rating_str) > 10:  # Ratings are typically short
            return False
            
        return True

    def get_rating_id_with_gemini(self, original_rating, title, description=None, release_year=None):
        """
        Get rating_id, using Gemini AI to deduce rating if the original is missing or invalid
        """
        # First try to use the original rating if it's valid
        if self.is_valid_rating(original_rating):
            rating_id = self.get_rating_id(original_rating)
            if rating_id:
                return rating_id
                
        print(f"🤖 Original rating '{original_rating}' is invalid or not found. Using Gemini to deduce rating for: {title}")
        
        # Use Gemini to deduce the rating
        try:
            deduced_rating = self.gemini_controller.deduce_rating_from_title(
                title=title,
                description=description,
                release_year=release_year
            )
            
            # Try to find the deduced rating in the database
            rating_id = self.get_rating_id(deduced_rating)
            if rating_id:
                print(f"✅ Found existing rating ID for deduced rating '{deduced_rating}': {rating_id}")
                return rating_id
            
            # If deduced rating doesn't exist, create it
            print(f"➕ Creating new rating: {deduced_rating}")
            rating_data = {
                "code": deduced_rating,
                "description": f"AI-deduced rating for content analysis"
            }
            created_rating = self.ratings_repo.create(rating_data)
            print(f"✅ Created rating: {created_rating}")
            return created_rating["rating_id"]
            
        except Exception as e:
            print(f"⚠️ Error using Gemini for rating deduction: {e}")
            
            # Fallback: create a "Not Rated" entry
            fallback_rating = "NR"
            rating_id = self.get_rating_id(fallback_rating)
            if rating_id:
                return rating_id
                
            # Create NR rating if it doesn't exist
            rating_data = {
                "code": fallback_rating,
                "description": "Not Rated - fallback rating"
            }
            created_rating = self.ratings_repo.create(rating_data)
            return created_rating["rating_id"]

    def get_title_type_id(self, type_val):
        """
        Get title_type_id from title_types table by looking up the description.
        If title type doesn't exist, create it automatically.
        """
        if not type_val or type_val == "unknown" or pd.isna(type_val):
            # Create a default "Unknown" entry if type is missing
            type_val = "Unknown"
            
        try:
            # First, try to find existing title type
            existing_type = self.title_types_repo.get_by_description(type_val)
            if existing_type:
                return existing_type[0]["title_type_id"]
            
            # If title type doesn't exist, create it
            print(f"🔧 Creating missing title type: {type_val}")
            new_type_data = {
                "description": type_val
            }
            created_type = self.title_types_repo.create(new_type_data)
            print(f"✅ Created new title type: {created_type['description']} (ID: {created_type['title_type_id']})")
            return created_type["title_type_id"]
            
        except Exception as e:
            print(f"Error getting/creating title type ID for '{type_val}': {e}")
            raise

    def parse_date(self, date_str):
        """
        Parse date string to datetime object, handling null values gracefully
        Returns "unknown" for null/invalid dates
        """
        # Handle various null/empty cases
        if date_str is None or pd.isna(date_str) or str(date_str).strip().lower() in ['', 'null', 'none', 'nan']:
            return "unknown"
        
        try:
            date_str = str(date_str).strip()
            
            # Return "unknown" for empty string after stripping
            if not date_str:
                return "unknown"
            
            # Try common date formats
            date_formats = [
                "%B %d, %Y",  # "January 1, 2021"
                "%Y-%m-%d",   # "2021-01-01"
                "%m/%d/%Y",   # "01/01/2021"
                "%d/%m/%Y"    # "01/01/2021"
            ]
            
            for date_format in date_formats:
                try:
                    return datetime.strptime(date_str, date_format).date()
                except ValueError:
                    continue
                    
            print(f"⚠️ Could not parse date: {date_str}")
            return "unknown"
            
        except Exception as e:
            print(f"Error parsing date '{date_str}': {e}")
            return "unknown"

    def mark_as_processed(self, engine, show_id):
        """
        Mark title as processed in temp_titles table
        """
        try:
            with engine.connect() as conn:
                # Start a transaction
                trans = conn.begin()
                try:
                    result = conn.execute(
                        text("UPDATE public.temp_titles SET processed = TRUE WHERE show_id = :show_id"),
                        {"show_id": show_id}
                    )
                    trans.commit()
                    if result.rowcount > 0:
                        print(f"✅ Marked '{show_id}' as processed")
                    else:
                        print(f"⚠️ No rows updated for '{show_id}'")
                except Exception as e:
                    trans.rollback()
                    raise e
        except Exception as e:
            print(f"❌ Error marking '{show_id}' as processed: {e}")
            # Don't raise the exception - we want to continue processing other records

    def check_processing_status(self):
        """
        Check the processing status of temp_titles table
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
                FROM public.temp_titles
            ''', con=engine)
            
            stats = stats_df.iloc[0]
            
            print("📊 TEMP_TITLES PROCESSING STATUS:")
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
