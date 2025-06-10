import json
import pandas as pd
from sqlalchemy import create_engine

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