import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import re
import unicodedata

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.people_repository import PeopleRepository
from repositories.actors_repository import ActorsRepository
from repositories.titles_repository import TitlesRepository
from controllers.common_controller import CommonController


class ActorsController:
    """
    Controller for managing actors
    """

    def __init__(self):
        pass

    def create_temp_actors_table(self):
        """
        Create a temporary actors table from the temporary Netflix titles repository.
        """
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        records = temp_netflix_titles_repo.get_all()

        actors_list = []
        
        for record in records:
            # Check if the cast field exists and is not None
            if record["cast"] and record["cast"] != "unknown":
                # Split the cast string by commas
                raw_cast_names = record["cast"].split(",")
                
                # Clean up each actor name and associate with show_id
                for name in raw_cast_names:
                    clean_name = name.strip()
                    if clean_name:  # Only add non-empty names
                        actors_list.append({
                            "actor_name": clean_name,
                            "show_id": record["show_id"],
                            "processed": False
                        })

        print(f"\nFound {len(actors_list)} actor-title relationships in the temporary Netflix titles repository.")

        # Create Pandas DataFrame
        actors_df = pd.DataFrame(actors_list)

        # Save the DataFrame to a PostgreSQL database table
        table_name = "temp_actors"
        schema = "public"
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)
        actors_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
        print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")

    def normalize_name(self, name):
        """
        Normalize name for comparison (same as in PeopleController)
        """
        if not name:
            return ""
        
        # Basic cleanup
        name = re.sub(r'[^\w\s]', '', name)
        name = re.sub(r'\s+', ' ', name)
        name = name.strip().lower()
        
        # Remove accents
        name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
        return name

    def populate_actors_table_from_temp(self):
        """
        Fill in the actors table using data from temp_actors where processed = FALSE.
        """
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)

        # Load unprocessed records
        result_df = pd.read_sql(
            'SELECT actor_name, show_id FROM public.temp_actors WHERE processed = FALSE ORDER BY actor_name',
            con=engine
        )
        temp_actors = result_df.to_dict(orient="records")

        for record in temp_actors[:100]:  # Process in batches
            print("\n", record)
            
            raw_name = record["actor_name"]
            show_id = record["show_id"]
            
            # Clean name for processing
            full_name = self.normalize_name(raw_name)
            print(f"🔍 Processing actor: {full_name} for show: {show_id}")

            # Parse with Gemini
            common_controller = CommonController()
            parsed = common_controller.parse_full_name(full_name)

            # Skip if parsing failed
            if not isinstance(parsed, dict):
                print(f"⚠️ Skipping '{full_name}' — unexpected format: {type(parsed).__name__}")
                self.mark_as_processed(engine, raw_name, show_id)
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
                self.mark_as_processed(engine, raw_name, show_id)
                continue

            # Find the person in the people table
            people_repo = PeopleRepository()
            existing_person = people_repo.get_by_name(first_name, middle_name, last_name)

            if not existing_person:
                print(f"⚠️ Person not found in people table: {first_name} {middle_name} {last_name}")
                self.mark_as_processed(engine, raw_name, show_id)
                continue

            person_id = existing_person[0]["person_id"]
            print(f"✅ Found person_id: {person_id}")

            # Get the actual title_id from the titles table using show_id
            titles_repo = TitlesRepository()
            existing_title = titles_repo.get_by_show_id(show_id)
            
            if not existing_title:
                print(f"⚠️ Title not found in titles table for show_id: {show_id}")
                self.mark_as_processed(engine, raw_name, show_id)
                continue
                
            title_id = existing_title[0]["title_id"]
            print(f"✅ Found title_id: {title_id}")

            # Check if actor-title relationship already exists
            actors_repo = ActorsRepository()
            existing_actor = actors_repo.get_by_person_and_title(person_id, title_id)

            if not existing_actor:
                # Create new actor relationship
                created = actors_repo.create({
                    "person_id": person_id,
                    "title_id": title_id
                })
                print(f"✅ Created actor relationship: {created}")
            else:
                print(f"🟡 Actor relationship already exists: {existing_actor[0]}")

            self.mark_as_processed(engine, raw_name, show_id)

    def mark_as_processed(self, engine, actor_name, show_id):
        """
        Mark actor as processed in temp_actors table
        """
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("UPDATE public.temp_actors SET processed = TRUE WHERE actor_name = :actor_name AND show_id = :show_id"),
                    {"actor_name": actor_name, "show_id": show_id}
                )
                conn.commit()
                print(f"✅ Marked '{actor_name}' for show '{show_id}' as processed")
        except Exception as e:
            print(f"❌ Error marking '{actor_name}' as processed: {e}")
            raise
