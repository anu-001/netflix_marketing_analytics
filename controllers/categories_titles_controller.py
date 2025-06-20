import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG
from repositories.categories_repository import CategoriesRepository
from repositories.titles_repository import TitlesRepository
from repositories.categories_titles_repository import CategoriesTitlesRepository
from controllers.base_tracking_controller import BaseTrackingController


class CategoriesTitlesController(BaseTrackingController):
    """
    Optimized controller for managing categories-titles relationships with temp table tracking
    """

    def __init__(self):
        super().__init__()
        self.categories_repo = CategoriesRepository()
        self.titles_repo = TitlesRepository()
        self.categories_titles_repo = CategoriesTitlesRepository()
        
        # Cache for categories to avoid repeated database lookups
        self._category_cache = {}
        self._title_cache = {}

    def create_temp_categories_titles_table(self):
        """
        Create temp_categories_titles table from temp_netflix_titles data
        """
        # Start tracking
        run_id = self.start_processing_run("temp_categories_titles", "Creating temp_categories_titles table")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            print("📊 Loading data from temp_netflix_titles...")
            
            # Load data and split categories
            with engine.connect() as conn:
                # Drop existing temp table
                conn.execute(text("DROP TABLE IF EXISTS public.temp_categories_titles"))
                
                # Create temp table with processed flag
                create_table_sql = '''
                CREATE TABLE public.temp_categories_titles (
                    id SERIAL PRIMARY KEY,
                    show_id VARCHAR(20) NOT NULL,
                    category_name VARCHAR(255) NOT NULL,
                    processed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
                conn.execute(text(create_table_sql))
                conn.commit()
                
                print("✅ Created temp_categories_titles table")

            # Load data from temp_netflix_titles
            df = pd.read_sql(
                '''SELECT show_id, listed_in 
                   FROM public.temp_netflix_titles 
                   WHERE listed_in IS NOT NULL 
                   AND listed_in != '' 
                   AND TRIM(listed_in) != ''
                   ORDER BY show_id''',
                con=engine
            )
            
            if df.empty:
                print("⚠️ No category data found in temp_netflix_titles")
                self.complete_processing_run()
                return

            # Split categories and create records
            category_records = []
            for _, row in df.iterrows():
                show_id = row['show_id']
                categories = str(row['listed_in']).split(',')
                
                for category in categories:
                    category = category.strip()
                    if category and category.lower() != 'unknown':
                        category_records.append({
                            'show_id': show_id,
                            'category_name': category,
                            'processed': False
                        })

            if category_records:
                # Insert into temp table
                temp_df = pd.DataFrame(category_records)
                temp_df.to_sql(
                    'temp_categories_titles',
                    con=engine,
                    schema='public',
                    if_exists='append',
                    index=False
                )
                print(f"✅ Created {len(category_records)} category-title relationship records")
            else:
                print("⚠️ No valid category data found")

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"❌ Error creating temp_categories_titles table: {e}")
            raise

    def populate_categories_titles_table_from_temp(self):
        """
        Populate categories_titles table from temp_categories_titles where processed = FALSE
        """
        # Start tracking
        run_id = self.start_processing_run("categories_titles", "Populating categories_titles table from temp")
        
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
                    FROM public.temp_categories_titles
                    """)
                )
                status = status_result.fetchone()
                print(f"📋 Total records: {status[0]}")
                print(f"✅ Processed records: {status[1]}")
                print(f"⏳ Unprocessed records: {status[2]}")

            # Load unprocessed records
            result_df = pd.read_sql(
                '''SELECT id, show_id, category_name
                   FROM public.temp_categories_titles 
                   WHERE processed = FALSE 
                   ORDER BY id''',
                con=engine
            )
            
            if result_df.empty:
                print("✅ No unprocessed category-title relationships found")
                self.complete_processing_run()
                return
                
            temp_records = result_df.to_dict(orient="records")
            print(f"🔄 Processing {len(temp_records)} unprocessed category-title relationships...")

            for record in temp_records:
                self.increment_processed()
                
                record_id = record["id"]
                show_id = record["show_id"]
                category_name = record["category_name"]
                
                try:
                    print(f"🔍 Processing: {show_id} -> {category_name}")

                    # Get title_id
                    title_id = self._get_title_id_by_code(show_id)
                    if not title_id:
                        print(f"⚠️ Skipping - title not found for show_id: {show_id}")
                        self._mark_as_processed(engine, record_id)
                        self.increment_skipped()
                        continue

                    # Get or create category_id
                    category_id = self._get_or_create_category(category_name)
                    if not category_id:
                        print(f"⚠️ Skipping - could not get/create category: {category_name}")
                        self._mark_as_processed(engine, record_id)
                        self.increment_skipped()
                        continue

                    # Check if relationship already exists
                    existing_relationship = self.categories_titles_repo.get_by_category_and_title(category_id, title_id)
                    if existing_relationship and len(existing_relationship) > 0:
                        print(f"🟡 Relationship already exists: category_id={category_id}, title_id={title_id}")
                        self.increment_skipped()
                        self._mark_as_processed(engine, record_id)
                        continue

                    # Create new relationship
                    relationship_data = {
                        "category_id": category_id,
                        "title_id": title_id
                    }
                    
                    created_relationship = self.categories_titles_repo.create(relationship_data)
                    print(f"✅ Created relationship: {created_relationship}")
                    self.increment_created()

                    # Mark as processed
                    self._mark_as_processed(engine, record_id)
                    
                except Exception as e:
                    print(f"❌ Error processing record {record_id} ({show_id} -> {category_name}): {e}")
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
            print(f"❌ Error populating categories_titles table: {e}")
            raise

    def _get_or_create_category(self, category_name):
        """
        Get category_id by name, create if not exists (with caching)
        """
        if not category_name or category_name.strip() == "":
            category_name = "unknown"
        
        category_name = category_name.strip()
        
        # Check cache first
        if category_name in self._category_cache:
            return self._category_cache[category_name]
        
        try:
            # Try to find existing category
            existing_category = self.categories_repo.get_by_description(category_name)
            if existing_category and len(existing_category) > 0:
                category_id = existing_category[0]["category_id"]
                self._category_cache[category_name] = category_id
                return category_id
            
            # Create new category if not found
            print(f"➕ Creating new category: {category_name}")
            new_category = self.categories_repo.create({"description": category_name})
            category_id = new_category["category_id"]
            self._category_cache[category_name] = category_id
            return category_id
            
        except Exception as e:
            print(f"❌ Error getting/creating category '{category_name}': {e}")
            # Return "unknown" category as fallback
            if category_name != "unknown":
                return self._get_or_create_category("unknown")
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
        Mark record as processed in temp_categories_titles table
        """
        try:
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(
                        text("UPDATE public.temp_categories_titles SET processed = TRUE WHERE id = :record_id"),
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
        Check the processing status of temp_categories_titles table
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
                        AND table_name = 'temp_categories_titles'
                    )
                    """)
                )
                
                if not table_check.fetchone()[0]:
                    print("⚠️ temp_categories_titles table does not exist")
                    return None
                
                # Get processing status
                status_result = conn.execute(
                    text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                        COUNT(CASE WHEN processed = FALSE THEN 1 END) as unprocessed
                    FROM public.temp_categories_titles
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
