import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.countries_repository import CountriesRepository
from controllers.base_tracking_controller import BaseTrackingController


class CountriesController(BaseTrackingController):
    """
    Controller for managing countries
    """

    def __init__(self):
        super().__init__()

    def normalize_country_name(self, country_name):
        """
        Normalize country names to ensure uniqueness and consistency.
        """
        if not country_name or country_name.strip() == "":
            return None
            
        # Clean and normalize the country name
        normalized = country_name.strip()
        
        # Handle common country name variations
        country_mappings = {
            "United States": "United States",
            "USA": "United States", 
            "US": "United States",
            "U.S.A.": "United States",
            "U.S.": "United States",
            "America": "United States",
            "United Kingdom": "United Kingdom",
            "UK": "United Kingdom",
            "U.K.": "United Kingdom",
            "Britain": "United Kingdom",
            "Great Britain": "United Kingdom",
            "England": "United Kingdom",
            "South Korea": "South Korea",
            "Korea": "South Korea",
            "Republic of Korea": "South Korea",
            "Russia": "Russia",
            "Russian Federation": "Russia",
            "Soviet Union": "Russia",
            "USSR": "Russia",
            "China": "China",
            "People's Republic of China": "China",
            "PRC": "China",
            "Hong Kong": "Hong Kong",
            "Hong Kong SAR": "Hong Kong",
            "Taiwan": "Taiwan",
            "Republic of China": "Taiwan"
        }
        
        # Apply mappings
        if normalized in country_mappings:
            normalized = country_mappings[normalized]
            
        return normalized

    def create_temp_countries_table(self):
        """
        Create a temporary countries table from the temporary Netflix titles repository.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_countries", "Creating temporary countries table from Netflix data")
        
        try:
            temp_netflix_titles_repo = TempNetflixTitlesRepository()
            records = temp_netflix_titles_repo.get_all()

            countries_list = []
            
            for record in records:
                # Check if the country field exists and is not None
                if record.get("country") and record["country"] != "unknown":
                    # Split the countries string by commas
                    raw_countries = record["country"].split(",")
                    
                    # Clean up each country name and normalize
                    for country in raw_countries:
                        clean_country = country.strip()
                        if clean_country:  # Only add non-empty countries
                            normalized_country = self.normalize_country_name(clean_country)
                            if normalized_country:
                                countries_list.append(normalized_country)

            # Remove duplicates from the countries list
            countries_list = list(set(countries_list))
            # Sort the list alphabetically
            countries_list.sort()
            # Print the number of unique countries found
            print(f"\nFound {len(countries_list)} unique countries in the temporary Netflix titles repository.")

            # Create Pandas DataFrame from the countries list with two columns: country_name and processed
            countries_df = pd.DataFrame(countries_list, columns=["country_name"])
            countries_df["processed"] = False

            # Save the DataFrame to a PostgreSQL database table
            table_name = "temp_countries"
            schema = "public"
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            countries_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
            print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")
            
            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            raise

    def populate_countries_table_from_temp(self):
        """
        Fill in the countries table using countries from temp_countries where processed = FALSE.
        """
        # Start tracking
        run_id = self.start_processing_run("countries", "Populating countries table from temporary data")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT country_name FROM public.temp_countries WHERE processed = FALSE ORDER BY country_name',
                con=engine
            )
            temp_countries = result_df.to_dict(orient="records")

            for record in temp_countries:
                self.increment_processed()
                print("\n", record)
                
                country_name = record["country_name"]
                print(f"🔍 Processing country: {country_name}")

                # Check if country already exists by description (store raw country name as description)
                countries_repo = CountriesRepository()
                existing = countries_repo.get_by_description(country_name)

                if not existing:
                    # Create new country with raw country name as description
                    created = countries_repo.create({
                        "description": country_name
                    })
                    print(f"✅ Created: {created}")
                    self.increment_created()
                else:
                    print(f"🟡 Already exists: {existing[0]}")
                    self.increment_skipped()

                # Mark as processed
                self.mark_as_processed_by_country_name(engine, country_name)
                
                # Update progress every 10 records
                if self.records_processed % 10 == 0:
                    self.update_processing_progress()

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            raise

    def mark_as_processed_by_country_name(self, engine, country_name):
        """
        Mark country as processed in temp_countries table
        """
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("UPDATE public.temp_countries SET processed = TRUE WHERE country_name = :country_name"),
                    {"country_name": country_name}
                )
                conn.commit()
                print(f"✅ Marked '{country_name}' as processed")
        except Exception as e:
            print(f"❌ Error marking '{country_name}' as processed: {e}")
            raise
