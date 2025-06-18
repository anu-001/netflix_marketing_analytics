import pandas as pd
from sqlalchemy import create_engine, text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.people_repository import PeopleRepository
from repositories.actors_repository import ActorsRepository
from controllers.base_tracking_controller import BaseTrackingController


class ActorsController(BaseTrackingController):
    """
    Controller for managing actors
    The actors table only contains actor_id (FK to people.person_id)
    """

    def __init__(self):
        super().__init__()

    def create_temp_actors_table(self):
        """
        Create a temporary actors table from the cast column in temp_netflix_titles.
        Each actor name gets a separate row with processed flag.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_actors", "Creating temporary actors table from cast column")
        
        try:
            temp_netflix_titles_repo = TempNetflixTitlesRepository()
            records = temp_netflix_titles_repo.get_all()

            actors_list = []
            
            for record in records:
                # Check if the cast field exists and is not None/empty
                if record.get("cast") and record["cast"] != "unknown" and record["cast"].strip():
                    # Split the cast string by commas
                    raw_cast_names = record["cast"].split(",")
                    
                    # Clean up each actor name
                    for name in raw_cast_names:
                        clean_name = name.strip()
                        if clean_name:  # Only add non-empty names
                            actors_list.append({
                                "actor_name": clean_name,
                                "show_id": record["show_id"],
                                "processed": False
                            })

            print(f"\nFound {len(actors_list)} actor entries from cast column.")

            # Create Pandas DataFrame
            actors_df = pd.DataFrame(actors_list)

            # Save the DataFrame to PostgreSQL database table
            table_name = "temp_actors"
            schema = "public"
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            actors_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
            print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")
            
            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            raise

    def populate_actors_table_from_temp(self, batch_size=100):
        """
        Fill the actors table using data from temp_actors where processed = FALSE.
        Only stores unique actor_id values (person_id from people table).
        Supports resumable processing with batch processing.
        
        Args:
            batch_size (int): Number of records to process in each batch
        """
        # Start tracking
        run_id = self.start_processing_run("actors", f"Populating actors table (batch size: {batch_size})")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Check how many records are unprocessed
            unprocessed_count_df = pd.read_sql(
                'SELECT COUNT(*) as total FROM public.temp_actors WHERE processed = FALSE',
                con=engine
            )
            total_unprocessed = unprocessed_count_df['total'].iloc[0]
            print(f"📊 Found {total_unprocessed} unprocessed actor records")
            
            if total_unprocessed == 0:
                print("✅ All actors have already been processed!")
                self.complete_processing_run()
                return

            people_repo = PeopleRepository()
            actors_repo = ActorsRepository()
            unique_actors_added = set()  # Track unique actors added to avoid duplicates
            batch_number = 0

            while True:
                batch_number += 1
                print(f"\n🔄 Processing batch {batch_number} (up to {batch_size} records)...")
                
                # Load next batch of unprocessed records
                result_df = pd.read_sql(
                    f'SELECT actor_name, show_id FROM public.temp_actors WHERE processed = FALSE ORDER BY actor_name LIMIT {batch_size}',
                    con=engine
                )
                
                if result_df.empty:
                    print("✅ No more unprocessed records found. Processing complete!")
                    break
                
                temp_actors = result_df.to_dict(orient="records")
                print(f"📦 Processing {len(temp_actors)} records in batch {batch_number}")

                for record in temp_actors:
                    try:
                        self.increment_processed()
                        
                        actor_name = record["actor_name"]
                        show_id = record["show_id"]
                        
                        print(f"🔍 Processing actor: {actor_name}")

                        # Find the person in the people table by full name
                        existing_person = people_repo.get_by_full_name(actor_name)

                        if not existing_person:
                            print(f"⚠️ Person not found in people table: {actor_name}")
                            self.mark_as_processed_safe(engine, actor_name, show_id)
                            self.increment_skipped()
                            continue

                        person_id = existing_person[0]["person_id"]
                        print(f"✅ Found person_id: {person_id}")

                        # Add to actors table if not already added in this run
                        if person_id not in unique_actors_added:
                            if not actors_repo.actor_exists(person_id):
                                created_actor = actors_repo.create({"actor_id": person_id})
                                print(f"✅ Created NEW actor entry: {created_actor}")
                                self.increment_created()
                                unique_actors_added.add(person_id)
                            else:
                                print(f"🟡 Actor already exists in actors table: {person_id}")
                                unique_actors_added.add(person_id)
                        else:
                            print(f"🔵 Actor already processed in this run: {person_id}")

                        # Mark as processed in temp_actors
                        self.mark_as_processed_safe(engine, actor_name, show_id)
                        
                    except Exception as record_error:
                        print(f"❌ Error processing record {actor_name}: {record_error}")
                        # Still mark as processed to avoid infinite loops
                        self.mark_as_processed_safe(engine, actor_name, show_id)
                        self.increment_failed()
                        continue

                # Progress update after each batch
                self.update_processing_progress()
                print(f"✅ Completed batch {batch_number}")

            print(f"\n📊 Final Summary:")
            print(f"   - Unique actors added to actors table: {len(unique_actors_added)}")
            print(f"   - Total records processed: {self.records_processed}")
            print(f"   - Records created: {self.records_created}")
            print(f"   - Records skipped: {self.records_skipped}")

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            print(f"💥 Critical error in actors processing: {e}")
            self.fail_processing_run(str(e))
            raise

    def mark_as_processed_safe(self, engine, actor_name, show_id):
        """
        Safely mark actor as processed in temp_actors table with error handling
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE public.temp_actors SET processed = TRUE WHERE actor_name = :actor_name AND show_id = :show_id"),
                    {"actor_name": actor_name, "show_id": show_id}
                )
                conn.commit()
                if result.rowcount > 0:
                    print(f"✅ Marked '{actor_name}' for show '{show_id}' as processed")
                else:
                    print(f"⚠️ No rows updated for '{actor_name}' and show '{show_id}'")
        except Exception as e:
            print(f"❌ Error marking '{actor_name}' for show '{show_id}' as processed: {e}")
            # Don't raise the exception - we want to continue processing other records

    def check_processing_status(self):
        """
        Check the processing status of temp_actors table
        """
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            # Get processing statistics
            stats_df = pd.read_sql('''
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE processed = TRUE) as processed_records,
                    COUNT(*) FILTER (WHERE processed = FALSE) as unprocessed_records,
                    ROUND(COUNT(*) FILTER (WHERE processed = TRUE) * 100.0 / COUNT(*), 2) as completion_percentage
                FROM public.temp_actors
            ''', con=engine)
            
            stats = stats_df.iloc[0]
            
            print("📊 TEMP_ACTORS PROCESSING STATUS:")
            print(f"   Total records: {stats['total_records']}")
            print(f"   Processed: {stats['processed_records']}")
            print(f"   Remaining: {stats['unprocessed_records']}")
            print(f"   Completion: {stats['completion_percentage']}%")
            
            return {
                'total': stats['total_records'],
                'processed': stats['processed_records'],
                'remaining': stats['unprocessed_records'],
                'completion_percentage': stats['completion_percentage']
            }
            
        except Exception as e:
            print(f"❌ Error checking processing status: {e}")
            return None
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
