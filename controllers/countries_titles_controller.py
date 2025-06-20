import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG
from repositories.countries_repository import CountriesRepository
from repositories.titles_repository import TitlesRepository
from repositories.countries_titles_repository import CountriesTitlesRepository
from controllers.base_tracking_controller import BaseTrackingController


class CountriesTitlesController(BaseTrackingController):
    """
    Optimized controller for managing countries-titles relationships with temp table tracking
    """

    def __init__(self):
        super().__init__()
        self.countries_repo = CountriesRepository()
        self.titles_repo = TitlesRepository()
        self.countries_titles_repo = CountriesTitlesRepository()
        
        # Cache for countries to avoid repeated database lookups
        self._country_cache = {}
        self._title_cache = {}

    def create_temp_countries_titles_table(self):
        """
        Create temp_countries_titles table from temp_netflix_titles data
        """
        # Start tracking
        run_id = self.start_processing_run("temp_countries_titles", "Creating temp_countries_titles table")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            print("📊 Loading data from temp_netflix_titles...")
            
            # Load data and split countries
            with engine.connect() as conn:
                # Drop existing temp table
                conn.execute(text("DROP TABLE IF EXISTS public.temp_countries_titles"))
                
                # Create temp table with processed flag
                create_table_sql = '''
                CREATE TABLE public.temp_countries_titles (
                    id SERIAL PRIMARY KEY,
                    show_id VARCHAR(20) NOT NULL,
                    country_name VARCHAR(255) NOT NULL,
                    processed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
                conn.execute(text(create_table_sql))
                conn.commit()
                
                print("✅ Created temp_countries_titles table")

            # Load data from temp_netflix_titles
            df = pd.read_sql(
                '''SELECT show_id, country 
                   FROM public.temp_netflix_titles 
                   WHERE country IS NOT NULL 
                   AND country != '' 
                   AND TRIM(country) != ''
                   ORDER BY show_id''',
                con=engine
            )
            
            if df.empty:
                print("⚠️ No country data found in temp_netflix_titles")
                self.complete_processing_run()
                return

            # Split countries and create records
            country_records = []
            for _, row in df.iterrows():
                show_id = row['show_id']
                countries = str(row['country']).split(',')
                
                for country in countries:
                    country = country.strip()
                    if country and country.lower() != 'unknown':
                        country_records.append({
                            'show_id': show_id,
                            'country_name': country,
                            'processed': False
                        })

            if country_records:
                # Insert into temp table
                temp_df = pd.DataFrame(country_records)
                temp_df.to_sql(
                    'temp_countries_titles',
                    con=engine,
                    schema='public',
                    if_exists='append',
                    index=False
                )
                print(f"✅ Created {len(country_records)} country-title relationship records")
            else:
                print("⚠️ No valid country data found")

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"❌ Error creating temp_countries_titles table: {e}")
            raise

    def populate_countries_titles_table_from_temp(self):
        """
        Populate countries_titles table from temp_countries_titles where processed = FALSE
        """
        # Start tracking
        run_id = self.start_processing_run("countries_titles", "Populating countries_titles table from temp")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Check processing status first
            print("📊 Checking processing status...")
            with engine.connect() as conn:
                status_result = conn.execute(
                    text("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
                        COUNT(CASE WHEN processed = FALSE THEN 1 END) as unprocessed_records
                    FROM public.temp_countries_titles
                    """)
                )
                status = status_result.fetchone()
                print(f"📋 Total records: {status[0]}")
                print(f"✅ Processed records: {status[1]}")
                print(f"⏳ Unprocessed records: {status[2]}")

            # Load unprocessed records
            result_df = pd.read_sql(
                '''SELECT id, show_id, country_name
                   FROM public.temp_countries_titles 
                   WHERE processed = FALSE 
                   ORDER BY id''',
                con=engine
            )
            
            if result_df.empty:
                print("✅ No unprocessed country-title relationships found")
                self.complete_processing_run()
                return
                
            temp_records = result_df.to_dict(orient="records")
            print(f"🔄 Processing {len(temp_records)} unprocessed country-title relationships...")

            for record in temp_records:
                self.increment_processed()
                
                record_id = record["id"]
                show_id = record["show_id"]
                country_name = record["country_name"]
                
                try:
                    print(f"🔍 Processing: {show_id} -> {country_name}")

                    # Get title_id
                    title_id = self._get_title_id_by_code(show_id)
                    if not title_id:
                        print(f"⚠️ Skipping - title not found for show_id: {show_id}")
                        self._mark_as_processed(engine, record_id)
                        self.increment_skipped()
                        continue

                    # Get or create country_id
                    country_id = self._get_or_create_country(country_name)
                    if not country_id:
                        print(f"⚠️ Skipping - could not get/create country: {country_name}")
                        self._mark_as_processed(engine, record_id)
                        self.increment_skipped()
                        continue

                    # Check if relationship already exists
                    existing_relationship = self.countries_titles_repo.get_by_country_and_title(country_id, title_id)
                    if existing_relationship and len(existing_relationship) > 0:
                        print(f"🟡 Relationship already exists: country_id={country_id}, title_id={title_id}")
                        self.increment_skipped()
                        self._mark_as_processed(engine, record_id)
                        continue

                    # Create new relationship
                    relationship_data = {
                        "country_id": country_id,
                        "title_id": title_id
                    }
                    
                    created_relationship = self.countries_titles_repo.create(relationship_data)
                    print(f"✅ Created relationship: {created_relationship}")
                    self.increment_created()

                    # Mark as processed
                    self._mark_as_processed(engine, record_id)
                    
                except Exception as e:
                    print(f"❌ Error processing record {record_id} ({show_id} -> {country_name}): {e}")
                    # Continue processing other records
                    continue
                
                # Update progress every 10 records
                if self.records_processed % 10 == 0:
                    self.update_processing_progress()

            print(f"\n📊 Summary:")
            print(f"   - Total relationships processed: {self.records_processed}")
            print(f"   - New relationships created: {self.records_created}")
            print(f"   - Relationships skipped (already exist): {self.records_skipped}")

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"❌ Error populating countries_titles table: {e}")
            raise

    def _get_or_create_country(self, country_name):
        """
        Get country_id by name, create if not exists (with caching)
        """
        if not country_name or country_name.strip() == "":
            country_name = "unknown"
        
        country_name = country_name.strip()
        
        # Check cache first
        if country_name in self._country_cache:
            return self._country_cache[country_name]
        
        try:
            # Try to find existing country
            existing_country = self.countries_repo.get_by_description(country_name)
            if existing_country and len(existing_country) > 0:
                country_id = existing_country[0]["country_id"]
                self._country_cache[country_name] = country_id
                return country_id
            
            # Create new country if not found
            print(f"➕ Creating new country: {country_name}")
            new_country = self.countries_repo.create({"description": country_name})
            country_id = new_country["country_id"]
            self._country_cache[country_name] = country_id
            return country_id
            
        except Exception as e:
            print(f"❌ Error getting/creating country '{country_name}': {e}")
            # Return "unknown" country as fallback
            if country_name != "unknown":
                return self._get_or_create_country("unknown")
            else:
                raise e
    
    def _get_title_id_by_code(self, show_id):
        """
        Get title_id by show_id/code (with caching)
        """
        # Check cache first
        if show_id in self._title_cache:
            return self._title_cache[show_id]
        
        try:
            existing_title = self.titles_repo.get_by_code(show_id)
            if existing_title and len(existing_title) > 0:
                title_id = existing_title[0]["title_id"]
                self._title_cache[show_id] = title_id
                return title_id
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting title for show_id '{show_id}': {e}")
            return None

    def _mark_as_processed(self, engine, record_id):
        """
        Mark record as processed in temp_countries_titles table
        """
        try:
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(
                        text("UPDATE public.temp_countries_titles SET processed = TRUE WHERE id = :record_id"),
                        {"record_id": record_id}
                    )
                    trans.commit()
                    if result.rowcount > 0:
                        print(f"✅ Marked record {record_id} as processed")
                    else:
                        print(f"⚠️ No rows updated for record {record_id}")
                except Exception as e:
                    trans.rollback()
                    raise e
        except Exception as e:
            print(f"❌ Error marking record {record_id} as processed: {e}")

    def check_processing_status(self):
        """
        Check the processing status of temp_countries_titles table
        """
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            with engine.connect() as conn:
                # Check if temp table exists
                table_check = conn.execute(
                    text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'temp_countries_titles'
                    )
                    """)
                )
                
                if not table_check.fetchone()[0]:
                    print("⚠️ temp_countries_titles table does not exist")
                    return None
                
                # Get processing status
                status_result = conn.execute(
                    text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                        COUNT(CASE WHEN processed = FALSE THEN 1 END) as unprocessed
                    FROM public.temp_countries_titles
                    """)
                )
                
                status = status_result.fetchone()
                status_dict = {
                    'total': status[0],
                    'processed': status[1],
                    'unprocessed': status[2]
                }
                
                print(f"📊 Processing Status:")
                print(f"   Total: {status_dict['total']}")
                print(f"   Processed: {status_dict['processed']}")
                print(f"   Unprocessed: {status_dict['unprocessed']}")
                
                return status_dict
                
        except Exception as e:
            print(f"❌ Error checking processing status: {e}")
            return None
