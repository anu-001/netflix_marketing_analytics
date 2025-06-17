import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.title_types_repository import TitleTypesRepository


class TitleTypesController:
    """
    Controller for managing title types
    """

    def __init__(self):
        pass

    def create_temp_title_types_table(self):
        """
        Create a temporary title types table from the temporary Netflix titles repository.
        """
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        records = temp_netflix_titles_repo.get_all()

        title_types_list = []
        
        for record in records:
            # Check if the type field exists and is not None
            if record["type"] and record["type"] != "unknown":
                title_types_list.append(record["type"].strip())

        # Remove duplicates from the title types list
        title_types_list = list(set(title_types_list))
        # Sort the list alphabetically
        title_types_list.sort()
        # Print the number of unique title types found
        print(f"\nFound {len(title_types_list)} unique title types in the temporary Netflix titles repository.")

        # Create Pandas DataFrame from the title types list with two columns: type_name and processed
        title_types_df = pd.DataFrame(title_types_list, columns=["type_name"])
        title_types_df["processed"] = False

        # Save the DataFrame to a PostgreSQL database table
        table_name = "temp_title_types"
        schema = "public"
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)
        title_types_df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
        print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")

    def populate_title_types_table_from_temp(self):
        """
        Fill in the title_types table using types from temp_title_types where processed = FALSE.
        """
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)

        # Load unprocessed records
        result_df = pd.read_sql(
            'SELECT type_name FROM public.temp_title_types WHERE processed = FALSE ORDER BY type_name',
            con=engine
        )
        temp_title_types = result_df.to_dict(orient="records")

        for record in temp_title_types:
            print("\n", record)
            
            type_name = record["type_name"]
            print(f"🔍 Processing title type: {type_name}")

            # Check if title type already exists
            title_types_repo = TitleTypesRepository()
            existing = title_types_repo.get_by_type_name(type_name)

            if not existing:
                # Create new title type with a default description
                created = title_types_repo.create({
                    "type_name": type_name,
                    "description": f"Content type: {type_name}"
                })
                print(f"✅ Created: {created}")
            else:
                print(f"🟡 Already exists: {existing[0]}")

            # Mark as processed
            self.mark_as_processed_by_type_name(engine, type_name)

    def mark_as_processed_by_type_name(self, engine, type_name):
        """
        Mark title type as processed in temp_title_types table
        """
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("UPDATE public.temp_title_types SET processed = TRUE WHERE type_name = :type_name"),
                    {"type_name": type_name}
                )
                conn.commit()
                print(f"✅ Marked '{type_name}' as processed")
        except Exception as e:
            print(f"❌ Error marking '{type_name}' as processed: {e}")
            raise
