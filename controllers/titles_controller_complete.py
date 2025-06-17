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
from repositories.categories_titles_repository import CategoriesTitlesRepository
from repositories.countries_titles_repository import CountriesTitlesRepository
from controllers.base_tracking_controller import BaseTrackingController


class TitlesControllerComplete(BaseTrackingController):
    """
    Complete controller for managing titles with both old and new junction table naming
    """

    def __init__(self):
        super().__init__()

    def populate_titles_table_from_temp_with_corrected_junctions(self):
        """
        Fill in the titles table using data from temp_netflix_titles where processed = FALSE.
        Also creates relationships using both old and new naming conventions.
        """
        # Start tracking
        run_id = self.start_processing_run("titles_complete", "Populating titles table with corrected junction tables")
        
        try:
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)

            # Load unprocessed records
            result_df = pd.read_sql(
                'SELECT * FROM public.temp_netflix_titles WHERE processed = FALSE ORDER BY show_id',
                con=engine
            )
            temp_titles = result_df.to_dict(orient="records")

            records_processed = 0
            records_created = 0
            records_skipped = 0

            for record in temp_titles[:100]:  # Process in batches
                print("\n", record)
                
                show_id = record["show_id"]
                title = record["title"]
                type_value = record["type"]
                director = record.get("director")
                cast = record.get("cast")
                country = record.get("country")
                date_added = record.get("date_added")
                release_year = record.get("release_year")
                rating = record.get("rating")
                duration = record.get("duration")
                listed_in = record.get("listed_in")
                description = record.get("description")
                
                print(f"🔍 Processing title: {title} (ID: {show_id})")

                # Check if title already exists
                titles_repo = TitlesRepository()
                existing_title = titles_repo.get_by_show_id(show_id)
                
                if existing_title:
                    print(f"🟡 Title already exists: {existing_title[0]['title']}")
                    title_id = existing_title[0]["title_id"]
                    records_skipped += 1
                else:
                    # Get foreign keys
                    type_id = self.get_type_id(type_value)
                    rating_id = self.get_rating_id(rating)
                    
                    if not type_id:
                        print(f"⚠️ Type not found: {type_value}")
                        self.mark_as_processed(engine, show_id)
                        records_processed += 1
                        records_skipped += 1
                        continue
                    
                    # Parse date
                    parsed_date_added = self.parse_date(date_added)
                    
                    # Create the main title record
                    created_title = titles_repo.create({
                        "show_id": show_id,
                        "title": title,
                        "type_id": type_id,
                        "director": director,
                        "cast": cast,
                        "country": country,
                        "date_added": parsed_date_added,
                        "release_year": release_year,
                        "rating_id": rating_id,
                        "duration": duration,
                        "listed_in": listed_in,
                        "description": description
                    })
                    
                    print(f"✅ Created title: {created_title}")
                    title_id = created_title["title_id"]
                    records_created += 1

                # Create junction table relationships using BOTH naming conventions
                self.create_title_category_relationships_old(title_id, listed_in)
                self.create_title_country_relationships_old(title_id, country)
                self.create_categories_titles_relationships_new(title_id, listed_in)
                self.create_countries_titles_relationships_new(title_id, country)

                self.mark_as_processed(engine, show_id)
                records_processed += 1

            # Complete tracking
            self.complete_processing_run(run_id, records_processed, records_created, records_skipped)
            
        except Exception as e:
            self.fail_processing_run(run_id, str(e))
            raise

    def get_type_id(self, type_name):
        """
        Get type ID from type name
        """
        if not type_name:
            return None
            
        try:
            title_types_repo = TitleTypesRepository()
            existing_type = title_types_repo.get_by_type_name(type_name)
            if existing_type:
                return existing_type[0]["type_id"]
        except Exception as e:
            print(f"Error getting type ID: {e}")
        return None

    def get_rating_id(self, rating_name):
        """
        Get rating ID from rating name
        """
        if not rating_name:
            return None
            
        try:
            ratings_repo = RatingsRepository()
            existing_rating = ratings_repo.get_by_rating_name(rating_name)
            if existing_rating:
                return existing_rating[0]["rating_id"]
        except Exception as e:
            print(f"Error getting rating ID: {e}")
        return None

    def create_title_category_relationships_old(self, title_id, listed_in):
        """
        Create relationships between title and categories (OLD NAMING: title_categories)
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
                    print(f"✅ Created OLD title-category relationship: {created}")
                else:
                    print(f"🟡 OLD title-category relationship already exists")
                    
            except Exception as e:
                print(f"❌ Error creating OLD title-category relationship: {e}")

    def create_title_country_relationships_old(self, title_id, country):
        """
        Create relationships between title and countries (OLD NAMING: title_countries)
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
                    print(f"✅ Created OLD title-country relationship: {created}")
                else:
                    print(f"🟡 OLD title-country relationship already exists")
                    
            except Exception as e:
                print(f"❌ Error creating OLD title-country relationship: {e}")

    def create_categories_titles_relationships_new(self, title_id, listed_in):
        """
        Create relationships between categories and titles (NEW NAMING: categories_titles)
        """
        if not listed_in:
            return
            
        categories = [cat.strip() for cat in listed_in.split(",")]
        categories_repo = CategoriesRepository()
        categories_titles_repo = CategoriesTitlesRepository()
        
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
                existing_relationship = categories_titles_repo.get_by_category_and_title(category_id, title_id)
                if not existing_relationship:
                    # Create relationship
                    created = categories_titles_repo.create({
                        "category_id": category_id,
                        "title_id": title_id
                    })
                    print(f"✅ Created NEW categories-titles relationship: {created}")
                else:
                    print(f"🟡 NEW categories-titles relationship already exists")
                    
            except Exception as e:
                print(f"❌ Error creating NEW categories-titles relationship: {e}")

    def create_countries_titles_relationships_new(self, title_id, country):
        """
        Create relationships between countries and titles (NEW NAMING: countries_titles)
        """
        if not country:
            return
            
        countries = [ctry.strip() for ctry in country.split(",")]
        countries_repo = CountriesRepository()
        countries_titles_repo = CountriesTitlesRepository()
        
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
                existing_relationship = countries_titles_repo.get_by_country_and_title(country_id, title_id)
                if not existing_relationship:
                    # Create relationship
                    created = countries_titles_repo.create({
                        "country_id": country_id,
                        "title_id": title_id
                    })
                    print(f"✅ Created NEW countries-titles relationship: {created}")
                else:
                    print(f"🟡 NEW countries-titles relationship already exists")
                    
            except Exception as e:
                print(f"❌ Error creating NEW countries-titles relationship: {e}")

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
            with engine.connect() as connection:
                connection.execute(
                    text("UPDATE public.temp_netflix_titles SET processed = TRUE WHERE show_id = :show_id"),
                    {"show_id": show_id}
                )
                connection.commit()
        except Exception as e:
            print(f"Error marking title as processed: {e}")
