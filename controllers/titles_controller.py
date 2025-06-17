import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime

from config import DB_CONFIG
from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository
from repositories.titles_repository import TitlesRepository
from repositories.title_types_repository import TitleTypesRepository
from repositories.ratings_repository import RatingsRepository
from repositories.categories_repository import CategoriesRepository
from repositories.countries_repository import CountriesRepository
from repositories.title_categories_repository import TitleCategoriesRepository
from repositories.title_countries_repository import TitleCountriesRepository


class TitlesController:
    """
    Controller for managing titles
    """

    def __init__(self):
        pass

    def populate_titles_table_from_temp(self):
        """
        Fill in the titles table using data from temp_netflix_titles where processed = FALSE.
        This also creates the relationships with categories and countries.
        """
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)

        # First, add processed column to temp_netflix_titles if it doesn't exist
        try:
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE public.temp_netflix_titles ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT FALSE"))
                conn.commit()
        except Exception as e:
            print(f"Note: {e}")

        # Load unprocessed records
        result_df = pd.read_sql(
            '''SELECT show_id, title, type, description, release_year, date_added, 
               duration, rating, listed_in, country 
               FROM public.temp_netflix_titles 
               WHERE processed = FALSE OR processed IS NULL 
               ORDER BY show_id''',
            con=engine
        )
        temp_titles = result_df.to_dict(orient="records")

        print(f"Found {len(temp_titles)} unprocessed titles")

        for record in temp_titles[:100]:  # Process in batches
            print("\n", record)
            
            show_id = record["show_id"]
            title_name = record["title"]
            title_type = record["type"]
            description = record.get("description")
            release_year = record.get("release_year")
            date_added = record.get("date_added")
            duration = record.get("duration")
            rating = record.get("rating")
            listed_in = record.get("listed_in")
            country = record.get("country")
            
            print(f"🔍 Processing title: {title_name} ({title_type})")

            # Check if title already exists
            titles_repo = TitlesRepository()
            existing_title = titles_repo.get_by_show_id(show_id)

            if existing_title:
                print(f"🟡 Title already exists: {existing_title[0]}")
                self.mark_as_processed(engine, show_id)
                continue

            # Get title_type_id
            title_type_id = self.get_title_type_id(title_type)
            if not title_type_id:
                print(f"⚠️ Title type not found: {title_type}")
                self.mark_as_processed(engine, show_id)
                continue

            # Get rating_id
            rating_id = self.get_rating_id(rating)

            # Parse date_added
            parsed_date_added = self.parse_date(date_added)

            # Create the title record
            try:
                created_title = titles_repo.create({
                    "show_id": show_id,
                    "title": title_name,
                    "title_type_id": title_type_id,
                    "description": description,
                    "release_year": release_year,
                    "date_added": parsed_date_added,
                    "duration": duration,
                    "rating_id": rating_id
                })
                print(f"✅ Created title: {created_title}")
                
                title_id = created_title["title_id"]

                # Create category relationships
                if listed_in and listed_in != "unknown":
                    self.create_title_category_relationships(title_id, listed_in)

                # Create country relationships
                if country and country != "unknown":
                    self.create_title_country_relationships(title_id, country)

            except Exception as e:
                print(f"❌ Error creating title: {e}")

            self.mark_as_processed(engine, show_id)

    def get_title_type_id(self, title_type):
        """
        Get title_type_id from title_types table
        """
        if not title_type or title_type == "unknown":
            return None
            
        try:
            title_types_repo = TitleTypesRepository()
            existing = title_types_repo.get_by_type_name(title_type)
            if existing:
                return existing[0]["title_type_id"]
        except Exception as e:
            print(f"Error getting title type ID: {e}")
        return None

    def get_rating_id(self, rating):
        """
        Get rating_id from ratings table
        """
        if not rating or rating == "unknown":
            return None
            
        try:
            ratings_repo = RatingsRepository()
            existing = ratings_repo.get_by_rating(rating)
            if existing:
                return existing[0]["rating_id"]
        except Exception as e:
            print(f"Error getting rating ID: {e}")
        return None

    def create_title_category_relationships(self, title_id, listed_in):
        """
        Create relationships between title and categories
        """
        if not listed_in:
            return
            
        categories = [cat.strip() for cat in listed_in.split(",")]
        categories_repo = CategoriesRepository()
        title_categories_repo = TitleCategoriesRepository()
        
        for category_name in categories:
            if not category_name:
                continue
                
            try:
                # Get category_id
                existing_category = categories_repo.get_by_category_name(category_name)
                if not existing_category:
                    print(f"⚠️ Category not found: {category_name}")
                    continue
                    
                category_id = existing_category[0]["category_id"]
                
                # Check if relationship already exists
                existing_relationship = title_categories_repo.get_by_title_and_category(title_id, category_id)
                if not existing_relationship:
                    # Create relationship
                    created = title_categories_repo.create({
                        "title_id": title_id,
                        "category_id": category_id
                    })
                    print(f"✅ Created title-category relationship: {created}")
                else:
                    print(f"🟡 Title-category relationship already exists")
                    
            except Exception as e:
                print(f"❌ Error creating title-category relationship: {e}")

    def create_title_country_relationships(self, title_id, country):
        """
        Create relationships between title and countries
        """
        if not country:
            return
            
        countries = [ctry.strip() for ctry in country.split(",")]
        countries_repo = CountriesRepository()
        title_countries_repo = TitleCountriesRepository()
        
        for country_name in countries:
            if not country_name:
                continue
                
            try:
                # Get country_id
                existing_country = countries_repo.get_by_country_name(country_name)
                if not existing_country:
                    print(f"⚠️ Country not found: {country_name}")
                    continue
                    
                country_id = existing_country[0]["country_id"]
                
                # Check if relationship already exists
                existing_relationship = title_countries_repo.get_by_title_and_country(title_id, country_id)
                if not existing_relationship:
                    # Create relationship
                    created = title_countries_repo.create({
                        "title_id": title_id,
                        "country_id": country_id
                    })
                    print(f"✅ Created title-country relationship: {created}")
                else:
                    print(f"🟡 Title-country relationship already exists")
                    
            except Exception as e:
                print(f"❌ Error creating title-country relationship: {e}")

    def parse_date(self, date_str):
        """
        Parse date string to proper format
        """
        if not date_str or date_str == "unknown":
            return None
            
        try:
            # Try common date formats
            for fmt in ["%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
        except Exception as e:
            print(f"Error parsing date '{date_str}': {e}")
        return None

    def mark_as_processed(self, engine, show_id):
        """
        Mark title as processed in temp_netflix_titles table
        """
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("UPDATE public.temp_netflix_titles SET processed = TRUE WHERE show_id = :show_id"),
                    {"show_id": show_id}
                )
                conn.commit()
                print(f"✅ Marked '{show_id}' as processed")
        except Exception as e:
            print(f"❌ Error marking '{show_id}' as processed: {e}")
            raise
