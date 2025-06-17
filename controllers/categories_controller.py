import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.categories_repository import CategoriesRepository
from controllers.base_tracking_controller import BaseTrackingController


class CategoriesController(BaseTrackingController):
    """
    Controller for managing categories
    """

    def __init__(self):
        super().__init__()

    def create_temp_categories_table(self):
        """
        Create a temporary categories table from the temporary Netflix titles repository.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_categories", "Creating temporary categories table from Netflix data")
        
        try:
            temp_netflix_titles_repo = TempNetflixTitlesRepository()
            records = temp_netflix_titles_repo.get_all()

            categories_list = []
            
            for record in records:
                # Check if the listed_in field exists and is not None
                if record.get("listed_in") and record["listed_in"] != "unknown":
                    # Split the categories string by commas
                    raw_categories = record["listed_in"].split(",")
                    
                    # Clean up each category name
                    for category in raw_categories:
                        clean_category = category.strip()
                        if clean_category:  # Only add non-empty categories
                            categories_list.append(clean_category)

            # Remove duplicates from the categories list
            categories_list = list(set(categories_list))
            # Sort the list alphabetically
            categories_list.sort()
            # Print the number of unique categories found
            print(f"\nFound {len(categories_list)} unique categories in the temporary Netflix titles repository.")

            # Create Pandas DataFrame from the categories list with two columns: category_name and processed
            categories_df = pd.DataFrame(categories_list, columns=["category_name"])
            categories_df["processed"] = False

            # Save the DataFrame to a PostgreSQL database table
            table_name = "temp_categories"
            schema = "public"
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            categories_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
            print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")
            
            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            raise

    def populate_categories_table_from_temp(self):
        """
        Fill in the categories table using categories from temp_categories where processed = FALSE.
        """
        # Start tracking
        run_id = self.start_processing_run("categories", "Populating categories table from temporary data")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT category_name FROM public.temp_categories WHERE processed = FALSE ORDER BY category_name',
                con=engine
            )
            temp_categories = result_df.to_dict(orient="records")

            records_processed = 0
            records_created = 0
            records_skipped = 0

            for record in temp_categories:
                self.increment_processed()
                print("\n", record)
                
                category_name = record["category_name"]
                print(f"🔍 Processing category: {category_name}")
                
                # Normalize the category name
                normalized_name = self.normalize_category_name(category_name)
                print(f"📝 Normalized: '{category_name}' → '{normalized_name}'")

                # Check if normalized category already exists
                categories_repo = CategoriesRepository()
                existing = categories_repo.get_by_description(normalized_name)

                if not existing:
                    # Create new category with normalized name as description
                    created = categories_repo.create({
                        "description": normalized_name
                    })
                    print(f"✅ Created: {created}")
                    self.increment_created()
                else:
                    print(f"🟡 Already exists: {existing[0]}")
                    self.increment_skipped()

                # Mark as processed
                self.mark_as_processed_by_category_name(engine, category_name)
                
                # Update progress every 10 records
                if self.records_processed % 10 == 0:
                    self.update_processing_progress()

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            raise

    def mark_as_processed_by_category_name(self, engine, category_name):
        """
        Mark category as processed in temp_categories table
        """
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("UPDATE public.temp_categories SET processed = TRUE WHERE category_name = :category_name"),
                    {"category_name": category_name}
                )
                conn.commit()
                print(f"✅ Marked '{category_name}' as processed")
        except Exception as e:
            print(f"❌ Error marking '{category_name}' as processed: {e}")
            raise

    def clean_existing_categories_descriptions(self):
        """
        Clean up existing categories that have "Genre/Category: " prefix in their descriptions
        """
        # Start tracking
        run_id = self.start_processing_run("categories_cleanup", "Cleaning up existing categories descriptions")
        
        try:
            categories_repo = CategoriesRepository()
            
            # Get all categories using the repository
            all_categories = categories_repo.get_all()
            
            records_processed = 0
            records_updated = 0
            old_format_categories = []
            
            # Filter categories with old format
            for category in all_categories:
                if category["description"].startswith("Genre/Category: "):
                    old_format_categories.append(category)

            print(f"Found {len(old_format_categories)} categories with old format to clean up")

            for record in old_format_categories:
                self.increment_processed()
                category_id = record["category_id"]
                old_description = record["description"]
                
                # Extract the clean category name by removing the prefix
                clean_description = old_description.replace("Genre/Category: ", "")
                
                print(f"🔄 Updating category_id {category_id}: '{old_description}' → '{clean_description}'")
                
                # Update the description using repository
                updated = categories_repo.update(category_id, {"description": clean_description})
                
                if updated:
                    print(f"✅ Updated category_id {category_id}")
                    self.increment_created()  # Using created counter for updated records
                else:
                    print(f"❌ Failed to update category_id {category_id}")
            
                # Update progress every 10 records
                if self.records_processed % 10 == 0:
                    self.update_processing_progress()

            # Complete tracking
            self.complete_processing_run()
            print(f"🎉 Cleanup complete! Updated {self.records_created} category descriptions")
            
        except Exception as e:
            self.fail_processing_run(str(e))
            raise

    def normalize_category_name(self, category_name):
        """
        Normalize category names to be more concise and avoid duplicates
        """
        # Convert to lowercase for processing
        normalized = category_name.strip()
        
        # Handle compound categories - take the most specific/important part
        normalization_rules = {
            # Comedy categories
            "Stand-Up Comedy & Talk Shows": "Talk Shows",
            "Stand-Up Comedy": "Comedy",
            
            # TV categories - prefer the genre over the medium
            "TV Action & Adventure": "Action & Adventure",
            "TV Comedies": "Comedy",
            "TV Dramas": "Drama",
            "TV Horror": "Horror",
            "TV Mysteries": "Mystery",
            "TV Sci-Fi & Fantasy": "Sci-Fi & Fantasy",
            "TV Thrillers": "Thriller",
            
            # Simplify plural forms to singular where appropriate
            "Comedies": "Comedy",
            "Dramas": "Drama",
            "Documentaries": "Documentary",
            "Thrillers": "Thriller",
            
            # Movie categories - remove "Movies" suffix when genre is clear
            "Action & Adventure": "Action & Adventure",
            "Horror Movies": "Horror",
            "Romantic Movies": "Romance",
            "Sports Movies": "Sports",
            "Classic Movies": "Classic",
            "Independent Movies": "Independent",
            "International Movies": "International",
            "Cult Movies": "Cult",
            "LGBTQ Movies": "LGBTQ",
            
            # TV Show categories - remove "TV Shows" suffix
            "British TV Shows": "British",
            "Crime TV Shows": "Crime",
            "International TV Shows": "International",  
            "Korean TV Shows": "Korean",
            "Romantic TV Shows": "Romance",
            "Spanish-Language TV Shows": "Spanish",
            "Teen TV Shows": "Teen",
            "TV Shows": "General TV",
            
            # Kids categories
            "Children & Family Movies": "Family",
            "Kids' TV": "Kids",
            
            # Special categories
            "Classic & Cult TV": "Classic",
            "Science & Nature TV": "Science & Nature",
            "Music & Musicals": "Music",
            "Faith & Spirituality": "Faith",
            
            # Keep these as-is but ensure consistency
            "Anime Features": "Anime",
            "Anime Series": "Anime",
            "Reality TV": "Reality",
            "Docuseries": "Documentary"
        }
        
        # Apply normalization rules
        if normalized in normalization_rules:
            normalized = normalization_rules[normalized]
        
        return normalized

    def normalize_existing_categories(self):
        """
        Normalize existing categories and remove duplicates
        """
        # Start tracking
        run_id = self.start_processing_run("categories_normalization", "Normalizing existing categories and removing duplicates")
        
        try:
            categories_repo = CategoriesRepository()
            
            # Get all categories
            all_categories = categories_repo.get_all()
            
            print(f"Found {len(all_categories)} categories to normalize")
            
            # Track normalized categories to avoid duplicates
            normalized_categories = {}  # normalized_name -> category_id
            categories_to_delete = []   # category_ids to delete (duplicates)
            categories_to_update = []   # (category_id, new_description)
            
            # First pass: identify normalizations and duplicates
            for category in all_categories:
                category_id = category["category_id"]
                current_description = category["description"]
                
                # Remove any old prefix first
                if current_description.startswith("Genre/Category: "):
                    current_description = current_description.replace("Genre/Category: ", "")
                
                # Normalize the category name
                normalized_name = self.normalize_category_name(current_description)
                
                print(f"🔍 Category {category_id}: '{current_description}' → '{normalized_name}'")
                
                if normalized_name in normalized_categories:
                    # This is a duplicate - mark for deletion
                    print(f"🗑️  Duplicate found: category_id {category_id} (keeping {normalized_categories[normalized_name]})")
                    categories_to_delete.append(category_id)
                else:
                    # This is the first occurrence - keep it and possibly update
                    normalized_categories[normalized_name] = category_id
                    if current_description != normalized_name:
                        categories_to_update.append((category_id, normalized_name))
            
            print(f"\n📊 Normalization Plan:")
            print(f"   ├── Categories to update: {len(categories_to_update)}")
            print(f"   └── Categories to delete (duplicates): {len(categories_to_delete)}")
            
            # Second pass: update descriptions
            for category_id, new_description in categories_to_update:
                self.increment_processed()
                print(f"🔄 Updating category_id {category_id} to '{new_description}'")
                
                updated = categories_repo.update(category_id, {"description": new_description})
                if updated:
                    print(f"✅ Updated category_id {category_id}")
                    self.increment_created()
                else:
                    print(f"❌ Failed to update category_id {category_id}")
            
            # Third pass: delete duplicates
            for category_id in categories_to_delete:
                self.increment_processed()
                print(f"🗑️  Deleting duplicate category_id {category_id}")
                
                deleted = categories_repo.delete(category_id)
                if deleted:
                    print(f"✅ Deleted category_id {category_id}")
                else:
                    print(f"❌ Failed to delete category_id {category_id}")
            
            # Complete tracking
            self.complete_processing_run()
            print(f"🎉 Normalization complete!")
            print(f"   ├── Updated: {self.records_created} categories")
            print(f"   └── Deleted: {len(categories_to_delete)} duplicates")
            print(f"   📊 Final unique categories: {len(normalized_categories)}")
            
        except Exception as e:
            self.fail_processing_run(str(e))
            raise
