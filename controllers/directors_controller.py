import re
import unicodedata
import pandas as pd
from sqlalchemy import create_engine, text

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from controllers.common_controller import CommonController
from repositories.people_repository import PeopleRepository

class DirectorsController:

    def __init__(self):
        self.engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        self.temp_repo = TempNetflixTitlesRepository()
        self.people_repo = PeopleRepository()

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
            .replace("-", "")
            .strip()
        )
        return unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')

    def create_temp_directors_table(self):
        records = self.temp_repo.get_all()

        directors = []
        for r in records:
            if r["director"] and r["director"].lower() != "unknown":
                names = r["director"].split(",")
                for name in names:
                    directors.append(name.strip())

        directors = sorted(set(directors))
        df = pd.DataFrame(directors, columns=["name"])
        df["processed"] = False

        df.to_sql(name="temp_directors", con=self.engine.connect(), schema="public", if_exists="replace", index=False)
        print(f"✅ Saved {len(directors)} unique directors to 'temp_directors' table.")

    def populate_directors_table_from_temp(self, limit=5):
        print("🚀 Starting populate_directors_table_from_temp()...\n")
        result_df = pd.read_sql(
            'SELECT name FROM public.temp_directors WHERE processed = FALSE ORDER BY name',
            con=self.engine
        )
        temp_directors = result_df.to_dict(orient="records")
        inserted_count = 0
        already_exists = 0
        not_found = 0
        parse_failed = 0

        for record in temp_directors[:limit]:
            raw_name = record["name"]
            normalized = self.normalize_name(raw_name)
            print(f"\n🎯 Processing: {normalized}")

            parsed = CommonController().parse_full_name(normalized)
            if not isinstance(parsed, dict):
                print(f"⚠️ Skipping invalid parse: {normalized}")
                self.mark_as_processed_by_name(raw_name)
                parse_failed += 1
                continue

            first = parsed.get("first_name") if parsed.get("first_name") != "unknown" else None
            middle = parsed.get("middle_name") if parsed.get("middle_name") != "unknown" else None
            last = parsed.get("last_name") if parsed.get("last_name") != "unknown" else None

            print(f"🔍 Parsed → First: {first}, Middle: {middle}, Last: {last}")

            if not first:
                first = normalized

            print("👤 Searching in people table...")
            match = self.people_repo.get_by_name(first, middle, last)
            print(f"🧾 People match result: {match}")

            if not match:
                print(f"❌ Not found in people: {normalized}")
                self.mark_as_processed_by_name(raw_name)
                not_found += 1
                continue

            person_id = match[0]["person_id"]

            with self.engine.begin() as conn:
                exists = conn.execute(
                    text("SELECT 1 FROM directors WHERE person_id = :pid"),
                    {"pid": person_id}
                ).fetchone()

                if exists:
                    print(f"🟡 Already in directors: {person_id}")
                    already_exists += 1
                else:
                    conn.execute(
                        text("INSERT INTO directors (person_id) VALUES (:pid)"),
                        {"pid": person_id}
                    )
                    print(f"✅ Inserted person_id into directors: {person_id}")
                    inserted_count += 1


            self.mark_as_processed_by_name(raw_name)

        print("🚀 Running from csv_importer.py")
        print(f"🧠 Pulling from temp_directors...")

        result_df = pd.read_sql(
            'SELECT name FROM public.temp_directors WHERE processed = FALSE ORDER BY name',
            con=self.engine
        )
        print(f"📦 Fetched {len(result_df)} unprocessed names")


        print(f"\n📊 Summary:")
        print(f"✅ Inserted: {inserted_count}")
        print(f"🟡 Already existed: {already_exists}")
        print(f"❌ Not found in people: {not_found}")
        print(f"⚠️ Parse failed: {parse_failed}")
        print("🎉 Done!\n")

    def mark_as_processed_by_name(self, original_name, table_name="temp_directors"):
        try:
            result_df = pd.read_sql(
                f'SELECT name FROM public.{table_name} WHERE processed = FALSE',
                con=self.engine
            )
            temp_names = result_df.to_dict(orient="records")
            target_norm = self.normalize_name(original_name)

            for row in temp_names:
                db_name = row["name"]
                db_norm = self.normalize_name(db_name)

                if db_norm == target_norm:
                    with self.engine.begin() as connection:
                        connection.execute(
                            text(f"""
                                UPDATE public.{table_name}
                                SET processed = TRUE
                                WHERE name = :name
                            """),
                            {"name": db_name}
                        )
                    print(f"✔️ Marked '{db_name}' as processed")
                    return

            print(f"⚠️ Could not find normalized match for '{original_name}'")

        except Exception as e:
            print(f"❌ Failed to mark processed for '{original_name}': {e}")
