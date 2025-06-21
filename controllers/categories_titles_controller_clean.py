import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG
from repositories.categories_repository import CategoriesRepository
from repositories.titles_repository import TitlesRepository
from repositories.categories_titles_repository import CategoriesTitlesRepository
from controllers.base_tracking_controller import BaseTrackingController


class CategoriesTitlesController(BaseTrackingController):
    """
    Optimized controller for managing categories-titles relationships
    """

    def __init__(self):
        super().__init__()
        self.categories_repo = CategoriesRepository()
        self.titles_repo = TitlesRepository()
        self.categories_titles_repo = CategoriesTitlesRepository()
        
        # Cache for categories to avoid repeated database lookups
        self._category_cache = {}
        self._title_cache = {}
        
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
            return self._get_or_create_category("unknown")
    
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
            
            print(f"⚠️ Title not found for show_id: {show_id}")
            return None
            
        except Exception as e:
            print(f"❌ Error getting title for show_id '{show_id}': {e}")
            return None

    def populate_categories_titles_table(self):
        """
        Optimized method to populate categories_titles table from temp_netflix_titles
        """
        # Start tracking
        run_id = self.start_processing_run("categories_titles", "Populating categories_titles table from temp_netflix_titles")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load all temp_netflix_titles with categories data
            print("📊 Loading temp_netflix_titles data...")
            result_df = pd.read_sql(
                '''SELECT show_id, listed_in 
                   FROM public.temp_netflix_titles 
                   WHERE listed_in IS NOT NULL 
                   AND listed_in != '' 
                   AND listed_in != 'unknown'
                   ORDER BY show_id''',
                con=engine
            )
            
            if result_df.empty:
                print("⚠️ No titles with categories found")
                self.complete_processing_run()
                return
                
            temp_titles = result_df.to_dict(orient="records")
            print(f"🔍 Found {len(temp_titles)} titles with category data")
            
            # Process each title's categories
            total_relationships = 0
            successful_relationships = 0
            skipped_relationships = 0
            
            for record in temp_titles:
                self.increment_processed()
                show_id = record["show_id"]
                listed_in = record["listed_in"]
                
                # Get title_id
                title_id = self._get_title_id_by_code(show_id)
                if not title_id:
                    print(f"⚠️ Skipping {show_id} - title not found in titles table")
                    continue
                
                # Split categories and process each one
                category_names = [cat.strip() for cat in listed_in.split(",") if cat.strip()]
                
                for category_name in category_names:
                    total_relationships += 1
                    
                    try:
                        # Get or create category
                        category_id = self._get_or_create_category(category_name)
                        
                        # Check if relationship already exists
                        existing_relationship = self.categories_titles_repo.get_by_category_and_title(
                            category_id, title_id
                        )
                        
                        if existing_relationship and len(existing_relationship) > 0:
                            skipped_relationships += 1
                            continue
                        
                        # Create new relationship
                        relationship_data = {
                            "category_id": category_id,
                            "title_id": title_id
                        }
                        
                        created_relationship = self.categories_titles_repo.create(relationship_data)
                        successful_relationships += 1
                        
                        if successful_relationships % 100 == 0:
                            print(f"✅ Created {successful_relationships} category-title relationships...")
                            
                    except Exception as e:
                        print(f"❌ Error creating relationship for category '{category_name}' and title '{show_id}': {e}")
                        continue
                
                # Update progress every 50 titles
                if self.records_processed % 50 == 0:
                    self.update_processing_progress()
                    print(f"📊 Processed {self.records_processed} titles...")

            print(f"\n📊 Summary:")
            print(f"   - Total titles processed: {self.records_processed}")
            print(f"   - Total relationships found: {total_relationships}")
            print(f"   - New relationships created: {successful_relationships}")
            print(f"   - Relationships skipped (already exist): {skipped_relationships}")
            print(f"   - Categories in cache: {len(self._category_cache)}")
            print(f"   - Titles in cache: {len(self._title_cache)}")

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"❌ Error populating categories_titles table: {e}")
            raise

    def check_processing_status(self):
        """
        Check the current status of categories_titles processing
        """
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            with engine.connect() as conn:
                # Check categories_titles table
                result = conn.execute(text("SELECT COUNT(*) FROM public.categories_titles"))
                categories_titles_count = result.fetchone()[0]
                
                # Check categories table
                result = conn.execute(text("SELECT COUNT(*) FROM public.categories"))
                categories_count = result.fetchone()[0]
                
                # Check titles with category data
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM public.temp_netflix_titles 
                    WHERE listed_in IS NOT NULL 
                    AND listed_in != '' 
                    AND listed_in != 'unknown'
                """))
                titles_with_categories = result.fetchone()[0]
                
                print(f"📊 Categories-Titles Processing Status:")
                print(f"   - Categories in database: {categories_count}")
                print(f"   - Category-Title relationships: {categories_titles_count}")
                print(f"   - Titles with category data: {titles_with_categories}")
                
                return {
                    "categories_count": categories_count,
                    "relationships_count": categories_titles_count,
                    "titles_with_categories": titles_with_categories
                }
                
        except Exception as e:
            print(f"❌ Error checking processing status: {e}")
            return None
