import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.categories_repository import CategoriesRepository


class CategoriesController:
    """
    Controller for managing categories
    """

    def __init__(self):
        pass

    def create_temp_categories_table(self):
        """
        Create a temporary categories table from the temporary Netflix titles repository.
        """
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

    def populate_categories_table_from_temp(self):
        """
        Fill in the categories table using categories from temp_categories where processed = FALSE.
        """
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)

        # Load unprocessed records
        result_df = pd.read_sql(
            'SELECT category_name FROM public.temp_categories WHERE processed = FALSE ORDER BY category_name',
            con=engine
        )
        temp_categories = result_df.to_dict(orient="records")

        for record in temp_categories:
            print("\n", record)
            
            category_name = record["category_name"]
            print(f"🔍 Processing category: {category_name}")

            # Check if category already exists
            categories_repo = CategoriesRepository()
            existing = categories_repo.get_by_category_name(category_name)

            if not existing:
                # Create new category with a default description
                created = categories_repo.create({
                    "category_name": category_name,
                    "description": f"Genre/Category: {category_name}"
                })
                print(f"✅ Created: {created}")
            else:
                print(f"🟡 Already exists: {existing[0]}")

            # Mark as processed
            self.mark_as_processed_by_category_name(engine, category_name)

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
