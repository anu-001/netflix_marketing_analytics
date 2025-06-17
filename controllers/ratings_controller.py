import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.ratings_repository import RatingsRepository
from controllers.base_tracking_controller import BaseTrackingController


class RatingsController(BaseTrackingController):
    """
    Controller for managing ratings
    """

    def __init__(self):
        super().__init__()

    def create_temp_ratings_table(self):
        """
        Create a temporary ratings table in the PostgreSQL database from the temporary Netflix titles repository.
        """
        # Get all records from the temporary Netflix titles repository
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        records = temp_netflix_titles_repo.get_all()

        ratings_list = []
        
        for record in records:
            # Check if the rating field exists and is not None
            if record["rating"] and record["rating"] != "unknown":
                ratings_list.append(record["rating"].strip())

        # Remove duplicates from the ratings list
        ratings_list = list(set(ratings_list))
        # Sort the list alphabetically
        ratings_list.sort()
        # Print the number of unique ratings found
        print(f"\nFound {len(ratings_list)} unique ratings in the temporary Netflix titles repository.")

        # Create Pandas DataFrame from the ratings list with two columns: rating and processed
        ratings_df = pd.DataFrame(ratings_list, columns=["rating"])
        ratings_df["processed"] = False

        # Save the DataFrame to a PostgreSQL database table
        table_name = "temp_ratings"
        schema = "public"
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)
        ratings_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
        print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")

    def populate_ratings_table_from_temp(self):
        """
        Fill in the ratings table using ratings from temp_ratings where processed = FALSE.
        """
        # Start tracking this processing run
        self.start_processing_run("ratings", "Populating ratings table from temp_ratings")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT rating FROM public.temp_ratings WHERE processed = FALSE ORDER BY rating',
                con=engine
            )
            temp_ratings = result_df.to_dict(orient="records")

            print(f"Found {len(temp_ratings)} unprocessed ratings")

            for record in temp_ratings:
                self.increment_processed()
                print("\n", record)
                
                rating_value = record["rating"]
                print(f"🔍 Processing rating: {rating_value}")

                # Check if rating already exists
                ratings_repo = RatingsRepository()
                existing = ratings_repo.get_by_rating(rating_value)

                if not existing:
                    # Create new rating with a default description
                    created = ratings_repo.create({
                        "rating": rating_value,
                        "description": f"Rating: {rating_value}"
                    })
                    print(f"✅ Created: {created}")
                    self.increment_created()
                else:
                    print(f"🟡 Already exists: {existing[0]}")
                    self.increment_skipped()

                # Mark as processed
                self.mark_as_processed_by_rating(engine, rating_value)
                
                # Update progress every 10 records
                if self.records_processed % 10 == 0:
                    self.update_processing_progress()

            # Complete the processing run
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            raise

    def mark_as_processed_by_rating(self, engine, rating_value):
        """
        Mark rating as processed in temp_ratings table
        """
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("UPDATE public.temp_ratings SET processed = TRUE WHERE rating = :rating"),
                    {"rating": rating_value}
                )
                conn.commit()
                print(f"✅ Marked '{rating_value}' as processed")
        except Exception as e:
            print(f"❌ Error marking '{rating_value}' as processed: {e}")
            raise
