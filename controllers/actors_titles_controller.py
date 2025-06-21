import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG
from repositories.people_repository import PeopleRepository
from repositories.actors_repository import ActorsRepository
from repositories.titles_repository import TitlesRepository
from controllers.base_tracking_controller import BaseTrackingController


class ActorsTitlesController(BaseTrackingController):
    """
    Optimized controller for managing actors-titles relationships with temp table tracking
    Note: actors_titles table has actor_id (from actors table) and title_id
    Creates missing actors by adding to people table first, then actors table
    """

    def __init__(self):
        super().__init__()
        self.people_repo = PeopleRepository()
        self.actors_repo = ActorsRepository()
        self.titles_repo = TitlesRepository()
        
        # Cache for efficient lookups
        self._people_cache = {}  # Maps person names to actor_id (from actors table)
        self._title_cache = {}   # Maps show_id to title_id

    def create_temp_actors_titles_table(self):
        """
        Create temp_actors_titles table from temp_netflix_titles data
        """
        # Start tracking
        run_id = self.start_processing_run("temp_actors_titles", "Creating temp_actors_titles table")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            print("📊 Loading data from temp_netflix_titles...")
            
            # Load data and split actors
            with engine.connect() as conn:
                # Drop existing temp table
                conn.execute(text("DROP TABLE IF EXISTS public.temp_actors_titles"))
                
                # Create temp table with processed flag
                create_table_sql = '''
                CREATE TABLE public.temp_actors_titles (
                    id SERIAL PRIMARY KEY,
                    show_id VARCHAR(20) NOT NULL,
                    actor_name VARCHAR(255) NOT NULL,
                    processed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
                conn.execute(text(create_table_sql))
                conn.commit()
                
                print("✅ Created temp_actors_titles table")

            # Load data from temp_netflix_titles
            df = pd.read_sql(
                '''SELECT show_id, "cast" 
                   FROM public.temp_netflix_titles 
                   WHERE "cast" IS NOT NULL 
                   AND "cast" != '' 
                   AND TRIM("cast") != ''
                   AND "cast" != 'unknown'
                   ORDER BY show_id''',
                con=engine
            )
            
            if df.empty:
                print("⚠️ No actor data found in temp_netflix_titles")
                self.complete_processing_run()
                return

            # Split actors and create records
            actor_records = []
            for _, row in df.iterrows():
                show_id = row['show_id']
                cast_list = str(row['cast']).split(',')
                
                for actor in cast_list:
                    actor = actor.strip()
                    if actor and actor.lower() != 'unknown':
                        actor_records.append({
                            'show_id': show_id,
                            'actor_name': actor,
                            'processed': False
                        })

            if actor_records:
                # Insert into temp table
                temp_df = pd.DataFrame(actor_records)
                temp_df.to_sql(
                    'temp_actors_titles',
                    con=engine,
                    schema='public',
                    if_exists='append',
                    index=False
                )
                print(f"✅ Created {len(actor_records)} actors-titles relationship records")
            else:
                print("⚠️ No valid actor data found")

            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"❌ Error creating temp_actors_titles table: {e}")
            raise

    def check_processing_status(self):
        """
        Check processing status of temp_actors_titles table
        """
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            with engine.connect() as conn:
                # Check if temp table exists
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'temp_actors_titles'
                    )
                """))
                table_exists = result.scalar()
                
                if not table_exists:
                    print("⚠️ temp_actors_titles table does not exist. Run create_temp_actors_titles_table() first.")
                    return
                
                # Get counts
                result = conn.execute(text("SELECT COUNT(*) FROM public.temp_actors_titles"))
                total_count = result.scalar()
                
                result = conn.execute(text("SELECT COUNT(*) FROM public.temp_actors_titles WHERE processed = TRUE"))
                processed_count = result.scalar()
                
                result = conn.execute(text("SELECT COUNT(*) FROM public.temp_actors_titles WHERE processed = FALSE"))
                unprocessed_count = result.scalar()
                
                print(f"📊 Actors-Titles Processing Status:")
                print(f"   Total records: {total_count}")
                print(f"   Processed: {processed_count}")
                print(f"   Remaining: {unprocessed_count}")
                
                if unprocessed_count > 0:
                    print(f"   Progress: {(processed_count/total_count)*100:.1f}%")
                else:
                    print(f"   Progress: 100% ✅")
                    
        except Exception as e:
            print(f"❌ Error checking processing status: {e}")

    def populate_actors_titles_table_from_temp(self, batch_size=500):
        """
        Populate actors_titles table from temp_actors_titles where processed = FALSE
        Uses efficient caching and batch processing
        """
        run_id = self.start_processing_run("actors_titles", "Populating actors_titles table from temp data")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            # Get total unprocessed count
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM public.temp_actors_titles WHERE processed = FALSE"))
                total_unprocessed = result.scalar()
                
            if total_unprocessed == 0:
                print("✅ All records already processed!")
                self.complete_processing_run()
                return
                
            print(f"📊 Processing {total_unprocessed} unprocessed actors-titles relationships...")
            
            # Build caches for efficiency
            print("🔄 Building caches for efficient processing...")
            self._build_people_cache(engine)
            self._build_title_cache(engine)
            
            # Process in batches
            offset = 0
            total_processed = 0
            total_created = 0
            total_skipped = 0
            
            while offset < total_unprocessed:
                print(f"\n🔄 Processing batch {offset//batch_size + 1} (records {offset+1}-{min(offset+batch_size, total_unprocessed)})")
                
                # Get batch of unprocessed records
                batch_df = pd.read_sql(
                    '''SELECT id, show_id, actor_name 
                       FROM public.temp_actors_titles 
                       WHERE processed = FALSE 
                       ORDER BY id 
                       LIMIT %s OFFSET %s''',
                    con=engine,
                    params=(batch_size, offset)
                )
                
                if batch_df.empty:
                    break
                
                batch_processed, batch_created, batch_skipped = self._process_batch(engine, batch_df)
                
                total_processed += batch_processed
                total_created += batch_created
                total_skipped += batch_skipped
                
                print(f"✅ Batch complete: {batch_processed} processed, {batch_created} created, {batch_skipped} skipped")
                
                offset += batch_size
            
            print(f"\n🎉 Actors-titles processing complete!")
            print(f"   Total processed: {total_processed}")
            print(f"   New relationships created: {total_created}")
            print(f"   Skipped (duplicates/errors): {total_skipped}")
            
            # Complete tracking
            self.complete_processing_run()
            
        except Exception as e:
            self.fail_processing_run(str(e))
            print(f"❌ Error populating actors_titles table: {e}")
            raise

    def _build_people_cache(self, engine):
        """
        Build cache of people who exist as actors for efficient lookup
        Maps person names to actor_id (only for people who exist in actors table)
        """
        print("   📋 Caching people data (only those who exist as actors)...")
        
        # Join people and actors to get only people who are actors
        people_df = pd.read_sql(
            """SELECT p.person_id as actor_id,
                      LOWER(TRIM(p.first_name)) as first_name, 
                      LOWER(TRIM(COALESCE(p.last_name, ''))) as last_name
               FROM public.people p
               INNER JOIN public.actors a ON p.person_id = a.actor_id""",
            con=engine
        )
        
        for _, row in people_df.iterrows():
            # Create multiple cache keys for flexible matching
            full_name = f"{row['first_name']} {row['last_name']}".strip()
            self._people_cache[full_name] = row['actor_id']
            self._people_cache[row['first_name']] = row['actor_id']  # Just first name
            
        print(f"   ✅ Cached {len(people_df)} people records (who exist as actors)")

    def _build_title_cache(self, engine):
        """
        Build cache of all titles for efficient lookup
        """
        print("   📋 Caching title data...")
        titles_df = pd.read_sql(
            "SELECT title_id, code FROM public.titles",
            con=engine
        )
        
        for _, row in titles_df.iterrows():
            self._title_cache[row['code']] = row['title_id']
            
        print(f"   ✅ Cached {len(titles_df)} title records")

    def _process_batch(self, engine, batch_df):
        """
        Process a batch of actors-titles relationships
        """
        batch_processed = 0
        batch_created = 0
        batch_skipped = 0
        
        # Process each record individually to avoid transaction abortion issues
        for _, row in batch_df.iterrows():
            record_id = row['id']
            show_id = row['show_id']
            actor_name = row['actor_name'].strip()
            
            # Use individual transaction for each record
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    # Find actor_id using cache
                    actor_id = self._find_actor_id(actor_name)
                    if not actor_id:
                        print(f"   ⚠️ Actor not found in people table: {actor_name}")
                        self._mark_as_processed_in_trans(conn, record_id)
                        trans.commit()
                        batch_processed += 1
                        batch_skipped += 1
                        continue
                    
                    # Find title_id using cache
                    title_id = self._title_cache.get(show_id)
                    if not title_id:
                        print(f"   ⚠️ Title not found for show_id: {show_id}")
                        self._mark_as_processed_in_trans(conn, record_id)
                        trans.commit()
                        batch_processed += 1
                        batch_skipped += 1
                        continue
                    
                    # Check if relationship already exists
                    existing = self._check_existing_relationship(conn, actor_id, title_id)
                    if existing:
                        batch_skipped += 1
                    else:
                        # Create new relationship
                        self._create_relationship(conn, actor_id, title_id)
                        batch_created += 1
                    
                    # Mark as processed
                    self._mark_as_processed_in_trans(conn, record_id)
                    batch_processed += 1
                    
                    # Commit the transaction
                    trans.commit()
                    
                except Exception as e:
                    print(f"   ❌ Error processing actor {actor_name}: {e}")
                    trans.rollback()
                    
                    # Try to mark as processed in a separate transaction to avoid losing progress
                    try:
                        with engine.connect() as conn2:
                            trans2 = conn2.begin()
                            self._mark_as_processed_in_trans(conn2, record_id)
                            trans2.commit()
                            batch_processed += 1
                            batch_skipped += 1
                    except:
                        # If we can't even mark as processed, skip this record
                        print(f"   ❌ Failed to mark record {record_id} as processed")
                        pass
        
        return batch_processed, batch_created, batch_skipped

    def _check_existing_relationship(self, conn, actor_id, title_id):
        """
        Check if actor-title relationship already exists using the provided connection
        Note: actor_id here is actually person_id from people table
        """
        try:
            result = conn.execute(
                text("SELECT COUNT(*) FROM public.actors_titles WHERE actor_id = :actor_id AND title_id = :title_id"),
                {"actor_id": actor_id, "title_id": title_id}
            )
            return result.scalar() > 0
        except Exception as e:
            print(f"   ⚠️ Error checking existing relationship: {e}")
            return False

    def _create_relationship(self, conn, actor_id, title_id):
        """
        Create new actor-title relationship using the provided connection
        Note: actor_id here is actually person_id from people table
        """
        conn.execute(
            text("INSERT INTO public.actors_titles (actor_id, title_id) VALUES (:actor_id, :title_id)"),
            {"actor_id": actor_id, "title_id": title_id}
        )

    def _mark_as_processed_in_trans(self, conn, record_id):
        """
        Mark a record as processed in temp_actors_titles table using the provided connection
        """
        conn.execute(
            text("UPDATE public.temp_actors_titles SET processed = TRUE WHERE id = :id"),
            {"id": record_id}
        )

    def _find_actor_id(self, actor_name):
        """
        Find actor_id using cached people data with flexible matching
        If actor not found, create new person and actor records
        """
        actor_name_lower = actor_name.lower().strip()
        
        # Try exact match first
        if actor_name_lower in self._people_cache:
            return self._people_cache[actor_name_lower]
        
        # Try first name only if full name doesn't work
        parts = actor_name_lower.split()
        if len(parts) > 0:
            first_name = parts[0]
            if first_name in self._people_cache:
                return self._people_cache[first_name]
        
        # If no match found, create new person and actor
        return self._create_missing_actor(actor_name)

    def _create_missing_actor(self, actor_name):
        """
        Create missing actor by:
        1. Adding to people table
        2. Adding person_id to actors table as actor_id
        3. Updating cache
        Returns actor_id for use in actors_titles
        """
        try:
            # Parse actor name
            parts = actor_name.strip().split()
            first_name = parts[0] if parts else "unknown"
            last_name = " ".join(parts[1:]) if len(parts) > 1 else None
            
            # Create person record
            person_data = {
                "first_name": first_name,
                "middle_name": None,
                "last_name": last_name
            }
            
            print(f"   ➕ Creating missing actor: {actor_name}")
            
            # Check if person already exists (by name)
            existing_people = self.people_repo.get_by_name(first_name, None, last_name)
            if existing_people:
                person_id = existing_people[0]["person_id"]
                print(f"   ✅ Found existing person with ID: {person_id}")
            else:
                # Create new person
                new_person = self.people_repo.create(person_data)
                if not new_person:
                    print(f"   ❌ Failed to create person for: {actor_name}")
                    return None
                person_id = new_person["person_id"]
                print(f"   ✅ Created new person with ID: {person_id}")
            
            # Check if this person is already an actor
            if self.actors_repo.actor_exists(person_id):
                print(f"   ✅ Person {person_id} already exists as actor")
                actor_id = person_id
            else:
                # Create actor record (actor_id = person_id)
                actor_data = {"actor_id": person_id}
                new_actor = self.actors_repo.create(actor_data)
                if not new_actor:
                    print(f"   ❌ Failed to create actor for person ID: {person_id}")
                    return None
                actor_id = new_actor["actor_id"]
                print(f"   ✅ Created new actor with ID: {actor_id}")
            
            # Update cache
            actor_name_lower = actor_name.lower().strip()
            self._people_cache[actor_name_lower] = actor_id
            if len(parts) > 0:
                self._people_cache[parts[0].lower()] = actor_id
            
            return actor_id
            
        except Exception as e:
            print(f"   ❌ Error creating actor for {actor_name}: {e}")
            # Try to rollback any pending transactions
            try:
                self.people_repo.db.rollback()
                self.actors_repo.db.rollback()
            except:
                pass
            return None

