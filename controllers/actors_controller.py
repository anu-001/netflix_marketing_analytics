"""
Actors controller for Netflix package
Handles actors table with only actor_id column (FK to people.person_id)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG
from repositories.actors_repository import ActorsRepository
from repositories.people_repository import PeopleRepository
from controllers.base_tracking_controller import BaseTrackingController


class ActorsController(BaseTrackingController):
    """
    Controller for managing actors table
    The actors table contains only actor_id (which references people.person_id)
    """

    def __init__(self):
        super().__init__()

    def create_temp_actors_table(self):
        """
        Create a temporary actors table from the cast column in temp_netflix_titles.
        Extracts individual actor names and associates them with show_id.
        """
        # Start tracking
        run_id = self.start_processing_run("temp_actors", "Creating temporary actors table from cast column")
        
        try:
            # Connect to database
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Read all temp_netflix_titles records
            df = pd.read_sql(
                'SELECT show_id, "cast" FROM public.temp_netflix_titles WHERE "cast" IS NOT NULL AND "cast" != \'unknown\'',
                con=engine
            )

            actors_list = []
            
            print(f"Processing {len(df)} records with cast data...")
            
            for _, record in df.iterrows():
                show_id = record["show_id"]
                cast_string = record["cast"]
                
                if cast_string and cast_string != "unknown":
                    # Split the cast string by commas
                    actor_names = cast_string.split(",")
                    
                    # Clean up each actor name
                    for name in actor_names:
                        clean_name = name.strip()
                        if clean_name:  # Only add non-empty names
                            actors_list.append({
                                "actor_name": clean_name,
                                "show_id": record["show_id"],
                                "processed": False
                            })

            print(f"Found {len(actors_list)} actor-title relationships")

            # Create DataFrame and save to database
            if actors_list:
                actors_df = pd.DataFrame(actors_list)
                actors_df.to_sql(
                    name="temp_actors", 
                    con=engine, 
                    schema="public", 
                    if_exists="replace", 
                    index=False
                )
                print(f"Successfully created temp_actors table with {len(actors_list)} records")
            else:
                print("No actor data found to process")
            
            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"Error creating temp_actors table: {e}")
            raise

    def populate_actors_table_from_temp(self):
        """
        Populate the actors table from temp_actors where processed = FALSE.
        Only stores unique actor_id values (person_id from people table).
        """
        # Start tracking
        run_id = self.start_processing_run("actors", "Populating actors table from temp_actors")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT actor_name, show_id FROM public.temp_actors WHERE processed = FALSE ORDER BY actor_name',
                con=engine
            )
            
            if result_df.empty:
                print("No unprocessed actors found")
                self.complete_processing_run()
                return
                
            temp_actors = result_df.to_dict(orient="records")
            people_repo = PeopleRepository()
            actors_repo = ActorsRepository()
            unique_actors_added = set()

            print(f"Processing {len(temp_actors)} unprocessed actor records...")

            for record in temp_actors:
                self.increment_processed()
                
                actor_name = record["actor_name"]
                show_id = record["show_id"]
                
                print(f"🔍 Processing actor: {actor_name}")

                # Find the person in the people table by full name
                existing_person = people_repo.get_by_full_name(actor_name)

                if not existing_person:
                    print(f"⚠️ Person not found: {actor_name}")
                    self.mark_as_processed(engine, actor_name, show_id)
                    self.increment_skipped()
                    continue

                person_id = existing_person[0]["person_id"]
                print(f"✅ Found person_id: {person_id}")

                # Add to actors table if not already added (only unique actor_ids)
                if person_id not in unique_actors_added:
                    if not actors_repo.actor_exists(person_id):
                        created_actor = actors_repo.create({"actor_id": person_id})
                        print(f"✅ Created new actor: {created_actor}")
                        self.increment_created()
                        unique_actors_added.add(person_id)
                    else:
                        print(f"🟡 Actor already exists: {person_id}")
                        unique_actors_added.add(person_id)

                # Mark as processed
                self.mark_as_processed(engine, actor_name, show_id)
                
                # Progress update every 50 records
                if self.records_processed % 50 == 0:
                    self.update_processing_progress()

            print(f"\n📊 Summary:")
            print(f"   - Unique actors added: {len(unique_actors_added)}")
            print(f"   - Total records processed: {self.records_processed}")

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"Error populating actors table: {e}")
            raise

    def mark_as_processed(self, engine, actor_name, show_id):
        """
        Mark actor as processed in temp_actors table
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE public.temp_actors SET processed = TRUE WHERE actor_name = :actor_name AND show_id = :show_id"),
                    {"actor_name": actor_name, "show_id": show_id}
                )
                conn.commit()
                if result.rowcount > 0:
                    print(f"✅ Marked as processed: {actor_name}")
        except Exception as e:
            print(f"❌ Error marking as processed: {e}")

    def check_processing_status(self):
        """
        Check the processing status of temp_actors table
        """
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            stats_df = pd.read_sql('''
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE processed = TRUE) as processed_records,
                    COUNT(*) FILTER (WHERE processed = FALSE) as unprocessed_records,
                    ROUND(COUNT(*) FILTER (WHERE processed = TRUE) * 100.0 / NULLIF(COUNT(*), 0), 2) as completion_percentage
                FROM public.temp_actors
            ''', con=engine)
            
            if not stats_df.empty:
                stats = stats_df.iloc[0]
                print("📊 TEMP_ACTORS PROCESSING STATUS:")
                print(f"   Total records: {stats['total_records']}")
                print(f"   Processed: {stats['processed_records']}")
                print(f"   Remaining: {stats['unprocessed_records']}")
                print(f"   Completion: {stats['completion_percentage']}%")
            
        except Exception as e:
            print(f"❌ Error checking status: {e}")
