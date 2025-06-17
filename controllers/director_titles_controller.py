import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import re
import unicodedata

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.people_repository import PeopleRepository
from repositories.director_titles_repository import DirectorTitlesRepository
from repositories.titles_repository import TitlesRepository
from controllers.common_controller import CommonController
from controllers.base_tracking_controller import BaseTrackingController


class DirectorTitlesController(BaseTrackingController):
    """
    Controller for managing director-title relationships
    """

    def __init__(self):
        super().__init__()

    def create_temp_director_titles_table(self):
        """
        Create a temporary director_titles table from the temporary Netflix titles repository.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_director_titles", "Creating temporary director-titles table from Netflix data")
        
        try:
            temp_netflix_titles_repo = TempNetflixTitlesRepository()
            records = temp_netflix_titles_repo.get_all()

            director_titles_list = []
            
            for record in records:
                # Check if the director field exists and is not None
                if record["director"] and record["director"] != "unknown":
                    # Split the director string by commas
                    raw_director_names = record["director"].split(",")
                    
                    # Clean up each director name and associate with show_id
                    for name in raw_director_names:
                        clean_name = name.strip()
                        if clean_name:  # Only add non-empty names
                            director_titles_list.append({
                                "director_name": clean_name,
                                "show_id": record["show_id"],
                                "processed": False
                            })

            print(f"\nFound {len(director_titles_list)} director-title relationships in the temporary Netflix titles repository.")

            # Create Pandas DataFrame
            director_titles_df = pd.DataFrame(director_titles_list)

            # Save the DataFrame to a PostgreSQL database table
            table_name = "temp_director_titles"
            schema = "public"
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            director_titles_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
            print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")
            
            # Complete tracking
            self.complete_processing_run(run_id, len(director_titles_list), len(director_titles_list), 0)
            
        except Exception as e:
            self.fail_processing_run(run_id, str(e))
            raise

    def normalize_name(self, name):
        """
        Normalize name for comparison (same as in PeopleController)
        """
        if not name:
            return ""
        
        # Remove extra whitespace and convert to lowercase
        normalized = re.sub(r'\s+', ' ', name.strip().lower())
        
        # Remove diacritics/accents
        normalized = unicodedata.normalize('NFD', normalized)
        normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
        
        return normalized

    def populate_director_titles_table_from_temp(self):
        """
        Fill in the director_titles table using data from temp_director_titles where processed = FALSE.
        """
        # Start tracking
        run_id = self.start_processing_run("director_titles", "Populating director-titles table from temporary data")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT director_name, show_id FROM public.temp_director_titles WHERE processed = FALSE ORDER BY director_name',
                con=engine
            )
            temp_director_titles = result_df.to_dict(orient="records")

            records_processed = 0
            records_created = 0
            records_skipped = 0

            for record in temp_director_titles[:100]:  # Process in batches
                print("\n", record)
                
                raw_name = record["director_name"]
                show_id = record["show_id"]
                
                # Clean name for processing
                full_name = self.normalize_name(raw_name)
                print(f"🔍 Processing director: {full_name} for show: {show_id}")

                # Parse with Gemini
                common_controller = CommonController()
                parsed = common_controller.parse_full_name(full_name)

                # Skip if parsing failed
                if not isinstance(parsed, dict):
                    print(f"⚠️ Skipping '{full_name}' — unexpected format: {type(parsed).__name__}")
                    self.mark_as_processed(engine, raw_name, show_id)
                    records_processed += 1
                    records_skipped += 1
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
                    records_processed += 1
                    records_skipped += 1
                    continue

                # Find the person in the people table
                people_repo = PeopleRepository()
                existing_person = people_repo.get_by_name(first_name, middle_name, last_name)

                if not existing_person:
                    print(f"⚠️ Person not found in people table: {first_name} {middle_name} {last_name}")
                    self.mark_as_processed(engine, raw_name, show_id)
                    records_processed += 1
                    records_skipped += 1
                    continue

                person_id = existing_person[0]["person_id"]
                print(f"✅ Found person_id: {person_id}")

                # Get the actual title_id from the titles table using show_id
                titles_repo = TitlesRepository()
                existing_title = titles_repo.get_by_show_id(show_id)
                
                if not existing_title:
                    print(f"⚠️ Title not found in titles table for show_id: {show_id}")
                    self.mark_as_processed(engine, raw_name, show_id)
                    records_processed += 1
                    records_skipped += 1
                    continue
                    
                title_id = existing_title[0]["title_id"]
                print(f"✅ Found title_id: {title_id}")

                # Check if director-title relationship already exists
                director_titles_repo = DirectorTitlesRepository()
                existing_director_title = director_titles_repo.get_by_person_and_title(person_id, title_id)

                if not existing_director_title:
                    # Create new director-title relationship
                    created = director_titles_repo.create({
                        "person_id": person_id,
                        "title_id": title_id
                    })
                    print(f"✅ Created director-title relationship: {created}")
                    records_created += 1
                else:
                    print(f"🟡 Director-title relationship already exists: {existing_director_title[0]}")
                    records_skipped += 1

                self.mark_as_processed(engine, raw_name, show_id)
                records_processed += 1

            # Complete tracking
            self.complete_processing_run(run_id, records_processed, records_created, records_skipped)
            
        except Exception as e:
            self.fail_processing_run(run_id, str(e))
            raise

    def mark_as_processed(self, engine, director_name, show_id):
        """
        Mark director as processed in temp_director_titles table
        """
        try:
            with engine.connect() as connection:
                connection.execute(
                    text("UPDATE public.temp_director_titles SET processed = TRUE WHERE director_name = :director_name AND show_id = :show_id"),
                    {"director_name": director_name, "show_id": show_id}
                )
                connection.commit()
        except Exception as e:
            print(f"Error marking director as processed: {e}")
