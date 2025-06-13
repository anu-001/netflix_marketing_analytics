import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import re
import unicodedata

from config import DB_CONFIG

from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.people_repository import PeopleRepository

from controllers.common_controller import CommonController

class PeopleController:

    def __init__(self):
        pass 


    def create_temp_people_table(self):
        """
        Create a temporary people table in the PostgreSQL database from the temporary Netflix titles repository.
        """

        # Create a list of people from the temporary Netflix titles repository (directors and cast)
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        records = temp_netflix_titles_repo.get_all()

        people_list = []
        
        for record in records:

            # Print the record
            print("\n", record)

            # Check if the director field exists and is not None
            if record["director"] and record["director"] != "unknown":

                # Split the director string by commas
                raw_director_names = record["director"].split(",")

                # Remove leading and trailing spaces from each name
                for name in raw_director_names:
                    clean_name = name.strip()
                    people_list.append(clean_name)

            # Check if the cast field exists and is not None
            if record["cast"] and record["cast"] != "unknown":

                # Split the cast string by commas
                raw_cast_names = record["cast"].split(",")
                
                # Remove leading and trailing spaces from each name
                for name in raw_cast_names:
                    clean_name = name.strip()
                    people_list.append(clean_name)


        # Remove duplicates from the people list
        people_list = list(set(people_list))
        # Sort the list alphabetically
        people_list.sort()
        # Print the number of unique people found
        print(f"\nFound {len(people_list)} unique people in the temporary Netflix titles repository.")

        # Create Pandas DataFrame from the people list with two columns: name and processed.  The default value for processed is False
        people_df = pd.DataFrame(people_list, columns=["name"])
        people_df["processed"] = False

        # Save the DataFrame to a PostgreSQL database table
        table_name = "temp_people"
        schema = "public"

        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)
        people_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
        print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")        

        


    def populate_people_table_from_cast(self):
        """
        Fill in the people table with directors and cast from the temporary Netflix titles repository.
        """
        # Get all records from the temporary Netflix titles repository
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        cast = temp_netflix_titles_repo.get_all()

        # Iterate over the records
        for record in cast[:2]:
            # Print the record
            print("\n", record)

            # Check if the cast field exists and is not None
            if record["cast"]:
                # Split the cast string by commas
                raw_cast_names = record["cast"].split(",")
                # Remove leading and trailing spaces from each name
                cast_list = []
                for name in raw_cast_names:
                    clean_name = name.strip()
                    cast_list.append(clean_name)

                print(f"Cast list: {cast_list}")

                # Get the cast's first, middle and last names from Gemini
                for cast_name in cast_list:

                    # Initialize the CommonController to use the parse_full_name method
                    common_controller = CommonController()
                    parsed_full_name = common_controller.parse_full_name(cast_name)

                    # Extract first, middle, and last names from the parsed result
                    first_name = parsed_full_name.get("first_name")
                    middle_name = parsed_full_name.get("middle_name")
                    last_name = parsed_full_name.get("last_name")

                    # Replace "unknown" with Python's None type
                    first_name = first_name if first_name != "unknown" else None
                    middle_name = middle_name if middle_name != "unknown" else None
                    last_name = last_name if last_name != "unknown" else None

                    print(f"First name: {first_name}, Middle name: {middle_name}, Last name: {last_name}")

                    # Find out if the person already exists in the people table
                    people_repo = PeopleRepository()
                    existing_people = people_repo.get_by_name(
                        first_name=first_name,
                        middle_name=middle_name,
                        last_name=last_name
                    )
                    
                    if not existing_people:
                        # If the person does not exist, create a new record

                        record_created = people_repo.create(
                            {
                                "first_name": first_name,
                                "middle_name": middle_name,
                                "last_name": last_name
                            }
                        )

                        print(f"Created new person: {record_created}")
                        print(f"person_id: {record_created['person_id']}")
                    else:
                        print(f"Person already exists: {existing_people[0]}")

                    print("\n")

##########################################################

       # Normalization function for both matching and processing
    def normalize_name(self, name):
        name = (
            name.strip()
            .strip("'\"")
            .replace("‘", "")
            .replace("’", "")
            .replace("“", "")
            .replace("”", "")
            .replace("\u200b", "")
            .replace("\u00a0", " ")
            .replace("-", "")  # remove hyphens
            .strip()
        )
        # Unicode normalization (critical for Ł, é, etc.)
        name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
        return name

    def populate_people_table_from_temp(self):
        """
        Fill in the people table using names from temp_people where processed = FALSE.
        """
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)

        # Load unprocessed records
        result_df = pd.read_sql(
            'SELECT name FROM public.temp_people WHERE processed = FALSE ORDER BY name',
            con=engine
        )
        temp_people = result_df.to_dict(orient="records")

        for record in temp_people[:100]:  # You can adjust batch size anytime
            print("\n", record)

            raw_name = record["name"]

            # ✅ Clean name for processing
            full_name = self.normalize_name(raw_name)
            print(f"🔍 Processing (normalized): {full_name}")

            # Parse with Gemini
            common_controller = CommonController()
            parsed = common_controller.parse_full_name(full_name)

            # ✅ Skip if parsing failed format
            if not isinstance(parsed, dict):
                print(f"⚠️ Skipping '{full_name}' — unexpected format: {type(parsed).__name__}")
                self.mark_as_processed_by_name(engine, raw_name)
                continue

            first_name = parsed.get("first_name")
            middle_name = parsed.get("middle_name")
            last_name = parsed.get("last_name")

            first_name = first_name if first_name != "unknown" else None
            middle_name = middle_name if middle_name != "unknown" else None
            last_name = last_name if last_name != "unknown" else None

            if first_name is None:
                print(f"⚠️ Fallback — using full name as first_name for: '{full_name}'")
                first_name = full_name
                middle_name = None
                last_name = None

            if not first_name or first_name.strip() == "":
                print(f"⚠️ Skipping — no valid first name: '{full_name}'")
                self.mark_as_processed_by_name(engine, raw_name)
                continue

            people_repo = PeopleRepository()
            existing = people_repo.get_by_name(first_name, middle_name, last_name)

            if not existing:
                created = people_repo.create({
                    "first_name": first_name,
                    "middle_name": middle_name,
                    "last_name": last_name
                })
                print(f"✅ Created: {created}")
            else:
                print(f"🟡 Already exists: {existing[0]}")

            self.mark_as_processed_by_name(engine, raw_name)

    def mark_as_processed_by_name(self, engine, original_name):
        """
        Mark processed using normalized matching (Python-side)
        """
        try:
            # Load all unprocessed names again
            result_df = pd.read_sql(
                'SELECT name FROM public.temp_people WHERE processed = FALSE',
                con=engine
            )
            temp_people = result_df.to_dict(orient="records")

            target_normalized = self.normalize_name(original_name)

            for row in temp_people:
                db_name = row["name"]
                db_normalized = self.normalize_name(db_name)

                if db_normalized == target_normalized:
                    connection = engine.connect()
                    transaction = connection.begin()
                    connection.execute(
                        text("""
                            UPDATE public.temp_people
                            SET processed = TRUE
                            WHERE name = :name
                        """),
                        {"name": db_name}
                    )
                    transaction.commit()
                    connection.close()
                    print(f"✔️ Marked '{db_name}' as processed (matched to '{original_name}')")
                    return  # ✅ done for this match

            print(f"⚠️ Could not find normalized match for '{original_name}'")

        except Exception as e:
            print(f"❌ Failed to update '{original_name}': {e}")




