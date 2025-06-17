import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.categories_repository import CategoriesRepository
from repositories.titles_repository import TitlesRepository
from repositories.categories_titles_repository import CategoriesTitlesRepository
from controllers.base_tracking_controller import BaseTrackingController


class CategoriesTitlesController(BaseTrackingController):
    """
    Controller for managing categories-titles relationships
    """

    def __init__(self):
        super().__init__()

    def create_temp_categories_titles_table(self):
        """
        Create a temporary categories_titles table from the temporary Netflix titles repository.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_categories_titles", "Creating temporary categories-titles table from Netflix data")
        
        try:
            temp_netflix_titles_repo = TempNetflixTitlesRepository()
            records = temp_netflix_titles_repo.get_all()

            categories_titles_list = []
            
            for record in records:
                # Check if the listed_in field exists and is not None
                if record["listed_in"] and record["listed_in"] != "unknown":
                    # Split the listed_in string by commas
                    raw_category_names = record["listed_in"].split(",")
                    
                    # Clean up each category name and associate with show_id
                    for name in raw_category_names:
                        clean_name = name.strip()
                        if clean_name:  # Only add non-empty names
                            categories_titles_list.append({
                                "category_name": clean_name,
                                "show_id": record["show_id"],
                                "processed": False
                            })

            print(f"\nFound {len(categories_titles_list)} category-title relationships in the temporary Netflix titles repository.")

            # Create Pandas DataFrame
            categories_titles_df = pd.DataFrame(categories_titles_list)

            # Save the DataFrame to a PostgreSQL database table
            table_name = "temp_categories_titles"
            schema = "public"
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            categories_titles_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
            print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")
            
            # Complete tracking
            self.complete_processing_run(run_id, len(categories_titles_list), len(categories_titles_list), 0)
            
        except Exception as e:
            self.fail_processing_run(run_id, str(e))
            raise

    def populate_categories_titles_table_from_temp(self):
        """
        Fill in the categories_titles table using data from temp_categories_titles where processed = FALSE.
        """
        # Start tracking
        run_id = self.start_processing_run("categories_titles", "Populating categories-titles table from temporary data")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT category_name, show_id FROM public.temp_categories_titles WHERE processed = FALSE ORDER BY category_name',
                con=engine
            )
            temp_categories_titles = result_df.to_dict(orient="records")

            records_processed = 0
            records_created = 0
            records_skipped = 0

            for record in temp_categories_titles:
                print("\n", record)
                
                category_name = record["category_name"]
                show_id = record["show_id"]
                
                print(f"🔍 Processing category: {category_name} for show: {show_id}")

                # Find the category in the categories table
                categories_repo = CategoriesRepository()
                existing_category = categories_repo.get_by_category_name(category_name)

                if not existing_category:
                    print(f"⚠️ Category not found in categories table: {category_name}")
                    self.mark_as_processed(engine, category_name, show_id)
                    records_processed += 1
                    records_skipped += 1
                    continue

                category_id = existing_category[0]["category_id"]
                print(f"✅ Found category_id: {category_id}")

                # Get the actual title_id from the titles table using show_id
                titles_repo = TitlesRepository()
                existing_title = titles_repo.get_by_show_id(show_id)
                
                if not existing_title:
                    print(f"⚠️ Title not found in titles table for show_id: {show_id}")
                    self.mark_as_processed(engine, category_name, show_id)
                    records_processed += 1
                    records_skipped += 1
                    continue
                    
                title_id = existing_title[0]["title_id"]
                print(f"✅ Found title_id: {title_id}")

                # Check if category-title relationship already exists
                categories_titles_repo = CategoriesTitlesRepository()
                existing_relationship = categories_titles_repo.get_by_category_and_title(category_id, title_id)

                if not existing_relationship:
                    # Create new category-title relationship
                    created = categories_titles_repo.create({
                        "category_id": category_id,
                        "title_id": title_id
                    })
                    print(f"✅ Created category-title relationship: {created}")
                    records_created += 1
                else:
                    print(f"🟡 Category-title relationship already exists: {existing_relationship[0]}")
                    records_skipped += 1

                self.mark_as_processed(engine, category_name, show_id)
                records_processed += 1

            # Complete tracking
            self.complete_processing_run(run_id, records_processed, records_created, records_skipped)
            
        except Exception as e:
            self.fail_processing_run(run_id, str(e))
            raise

    def mark_as_processed(self, engine, category_name, show_id):
        """
        Mark category as processed in temp_categories_titles table
        """
        try:
            with engine.connect() as connection:
                connection.execute(
                    text("UPDATE public.temp_categories_titles SET processed = TRUE WHERE category_name = :category_name AND show_id = :show_id"),
                    {"category_name": category_name, "show_id": show_id}
                )
                connection.commit()
        except Exception as e:
            print(f"Error marking category as processed: {e}")
