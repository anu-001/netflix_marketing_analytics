import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.countries_repository import CountriesRepository
from repositories.titles_repository import TitlesRepository
from repositories.countries_titles_repository import CountriesTitlesRepository
from controllers.base_tracking_controller import BaseTrackingController


class CountriesTitlesController(BaseTrackingController):
    """
    Controller for managing countries-titles relationships
    """

    def __init__(self):
        super().__init__()

    def create_temp_countries_titles_table(self):
        """
        Create a temporary countries_titles table from the temporary Netflix titles repository.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_countries_titles", "Creating temporary countries-titles table from Netflix data")
        
        try:
            temp_netflix_titles_repo = TempNetflixTitlesRepository()
            records = temp_netflix_titles_repo.get_all()

            countries_titles_list = []
            
            for record in records:
                # Check if the country field exists and is not None
                if record["country"] and record["country"] != "unknown":
                    # Split the country string by commas
                    raw_country_names = record["country"].split(",")
                    
                    # Clean up each country name and associate with show_id
                    for name in raw_country_names:
                        clean_name = name.strip()
                        if clean_name:  # Only add non-empty names
                            countries_titles_list.append({
                                "country_name": clean_name,
                                "show_id": record["show_id"],
                                "processed": False
                            })

            print(f"\nFound {len(countries_titles_list)} country-title relationships in the temporary Netflix titles repository.")

            # Create Pandas DataFrame
            countries_titles_df = pd.DataFrame(countries_titles_list)

            # Save the DataFrame to a PostgreSQL database table
            table_name = "temp_countries_titles"
            schema = "public"
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            countries_titles_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
            print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")
            
            # Complete tracking
            self.complete_processing_run(run_id, len(countries_titles_list), len(countries_titles_list), 0)
            
        except Exception as e:
            self.fail_processing_run(run_id, str(e))
            raise

    def populate_countries_titles_table_from_temp(self):
        """
        Fill in the countries_titles table using data from temp_countries_titles where processed = FALSE.
        """
        # Start tracking
        run_id = self.start_processing_run("countries_titles", "Populating countries-titles table from temporary data")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT country_name, show_id FROM public.temp_countries_titles WHERE processed = FALSE ORDER BY country_name',
                con=engine
            )
            temp_countries_titles = result_df.to_dict(orient="records")

            records_processed = 0
            records_created = 0
            records_skipped = 0

            for record in temp_countries_titles:
                print("\n", record)
                
                country_name = record["country_name"]
                show_id = record["show_id"]
                
                print(f"🔍 Processing country: {country_name} for show: {show_id}")

                # Find the country in the countries table
                countries_repo = CountriesRepository()
                existing_country = countries_repo.get_by_country_name(country_name)

                if not existing_country:
                    print(f"⚠️ Country not found in countries table: {country_name}")
                    self.mark_as_processed(engine, country_name, show_id)
                    records_processed += 1
                    records_skipped += 1
                    continue

                country_id = existing_country[0]["country_id"]
                print(f"✅ Found country_id: {country_id}")

                # Get the actual title_id from the titles table using show_id
                titles_repo = TitlesRepository()
                existing_title = titles_repo.get_by_show_id(show_id)
                
                if not existing_title:
                    print(f"⚠️ Title not found in titles table for show_id: {show_id}")
                    self.mark_as_processed(engine, country_name, show_id)
                    records_processed += 1
                    records_skipped += 1
                    continue
                    
                title_id = existing_title[0]["title_id"]
                print(f"✅ Found title_id: {title_id}")

                # Check if country-title relationship already exists
                countries_titles_repo = CountriesTitlesRepository()
                existing_relationship = countries_titles_repo.get_by_country_and_title(country_id, title_id)

                if not existing_relationship:
                    # Create new country-title relationship
                    created = countries_titles_repo.create({
                        "country_id": country_id,
                        "title_id": title_id
                    })
                    print(f"✅ Created country-title relationship: {created}")
                    records_created += 1
                else:
                    print(f"🟡 Country-title relationship already exists: {existing_relationship[0]}")
                    records_skipped += 1

                self.mark_as_processed(engine, country_name, show_id)
                records_processed += 1

            # Complete tracking
            self.complete_processing_run(run_id, records_processed, records_created, records_skipped)
            
        except Exception as e:
            self.fail_processing_run(run_id, str(e))
            raise

    def mark_as_processed(self, engine, country_name, show_id):
        """
        Mark country as processed in temp_countries_titles table
        """
        try:
            with engine.connect() as connection:
                connection.execute(
                    text("UPDATE public.temp_countries_titles SET processed = TRUE WHERE country_name = :country_name AND show_id = :show_id"),
                    {"country_name": country_name, "show_id": show_id}
                )
                connection.commit()
        except Exception as e:
            print(f"Error marking country as processed: {e}")
