import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.countries_repository import CountriesRepository


class CountriesController:
    """
    Controller for managing countries
    """

    def __init__(self):
        pass

    def create_temp_countries_table(self):
        """
        Create a temporary countries table from the temporary Netflix titles repository.
        """
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        records = temp_netflix_titles_repo.get_all()

        countries_list = []
        
        for record in records:
            # Check if the country field exists and is not None
            if record["country"] and record["country"] != "unknown":
                # Split the countries string by commas
                raw_countries = record["country"].split(",")
                
                # Clean up each country name
                for country in raw_countries:
                    clean_country = country.strip()
                    if clean_country:  # Only add non-empty countries
                        countries_list.append(clean_country)

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

    def populate_countries_table_from_temp(self):
        """
        Fill in the countries table using countries from temp_countries where processed = FALSE.
        """
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)

        # Load unprocessed records
        result_df = pd.read_sql(
            'SELECT country_name FROM public.temp_countries WHERE processed = FALSE ORDER BY country_name',
            con=engine
        )
        temp_countries = result_df.to_dict(orient="records")

        for record in temp_countries:
            print("\n", record)
            
            country_name = record["country_name"]
            print(f"🔍 Processing country: {country_name}")

            # Check if country already exists
            countries_repo = CountriesRepository()
            existing = countries_repo.get_by_country_name(country_name)

            if not existing:
                # Create new country (country_code can be added later or derived)
                created = countries_repo.create({
                    "country_name": country_name,
                    "country_code": None  # Can be populated later with ISO codes
                })
                print(f"✅ Created: {created}")
            else:
                print(f"🟡 Already exists: {existing[0]}")

            # Mark as processed
            self.mark_as_processed_by_country_name(engine, country_name)

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
