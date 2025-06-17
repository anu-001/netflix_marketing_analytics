from controllers.csv_controller import CSVController
from controllers.temp_netflix_titles_controller import TempNetflixTitlesController
from controllers.people_controller import PeopleController
from controllers.ratings_controller import RatingsController
from controllers.actors_controller import ActorsController
from controllers.directors_controller import DirectorsController
from controllers.actor_titles_controller import ActorTitlesController
from controllers.director_titles_controller import DirectorTitlesController
from controllers.title_types_controller import TitleTypesController
from controllers.categories_controller import CategoriesController
from controllers.countries_controller import CountriesController
from controllers.categories_titles_controller import CategoriesTitlesController
from controllers.countries_titles_controller import CountriesTitlesController
from controllers.titles_controller import TitlesController
from controllers.titles_controller_complete import TitlesControllerComplete
from controllers.base_tracking_controller import BaseTrackingController



def main():
    # Initialize tracking
    tracker = BaseTrackingController()
    
    print("🚀 Starting Netflix Data Processing Pipeline")
    print("   (Complete Implementation with ERD-Compliant Naming)")
    print("=" * 80)
    
    # Show initial dashboard
    print("\n📊 INITIAL STATUS CHECK:")
    tracker.print_processing_dashboard()

    # Define the path to the CSV file
    csv_path = "input/netflix_titles.csv"

    # Save CSV file in the database
    print("Saving CSV to database...")

    # Initialize the CSV handler
    netflix_csv = CSVController(csv_path)
    netflix_csv.save_csv_to_database(
        table_name="temp_netflix_titles",
        schema="public"
    )

    # Set missing directors
    print("Setting missing directors...")
    # Initialize the temporary Netflix titles controller
    temp_netflix_titles_controller = TempNetflixTitlesController()
    temp_netflix_titles_controller.set_missing_directors()

    # Set missing cast
    print("Setting missing cast...")
    temp_netflix_titles_controller.set_missing_actors()

    # Set missing countries
    print("Setting missing countries...")
    temp_netflix_titles_controller.set_missing_countries()

    # STEP 1: PROCESS PEOPLE
    print("\n" + "="*60)
    print("👥 STEP 1: PROCESSING PEOPLE")
    print("="*60)
    
    print("🔄 Creating temp_people table...")
    people_controller = PeopleController()
    people_controller.create_temp_people_table()

    print("🔄 Populating the people table from temp_people using Gemini...")
    people_controller.populate_people_table_from_temp()

    # STEP 2: PROCESS LOOKUP TABLES
    print("\n" + "="*60)
    print("📋 STEP 2: PROCESSING LOOKUP TABLES")
    print("="*60)
    
    # Add ratings processing
    print("🔄 Creating temp_ratings table...")
    ratings_controller = RatingsController()
    ratings_controller.create_temp_ratings_table()
    
    print("🔄 Populating the ratings table from temp_ratings...")
    ratings_controller.populate_ratings_table_from_temp()

    # Add title types processing
    print("🔄 Creating temp_title_types table...")
    title_types_controller = TitleTypesController()
    title_types_controller.create_temp_title_types_table()
    
    print("🔄 Populating the title_types table from temp_title_types...")
    title_types_controller.populate_title_types_table_from_temp()

    # Add categories processing
    print("🔄 Creating temp_categories table...")
    categories_controller = CategoriesController()
    categories_controller.create_temp_categories_table()
    
    print("🔄 Populating the categories table from temp_categories...")
    categories_controller.populate_categories_table_from_temp()

    # Add countries processing
    print("🔄 Creating temp_countries table...")
    countries_controller = CountriesController()
    countries_controller.create_temp_countries_table()
    
    print("🔄 Populating the countries table from temp_countries...")
    countries_controller.populate_countries_table_from_temp()

    # STEP 3: PROCESS MAIN TITLES TABLE
    print("\n" + "="*60)
    print("🎬 STEP 3: PROCESSING MAIN TITLES TABLE")
    print("="*60)
    
    # Use complete titles controller (both old and new junction tables)
    print("🔄 Populating the titles table with CORRECTED junction tables...")
    titles_controller_complete = TitlesControllerComplete()
    titles_controller_complete.populate_titles_table_from_temp_with_corrected_junctions()

    # STEP 4: PROCESS PEOPLE-TITLE JUNCTION TABLES
    print("\n" + "="*60)
    print("🎭 STEP 4: PROCESSING PEOPLE-TITLE JUNCTION TABLES")
    print("="*60)
    
    # Legacy naming
    print("🔄 Creating temp_actors table...")
    actors_controller = ActorsController()
    actors_controller.create_temp_actors_table()
    
    print("🔄 Populating the actors table from temp_actors...")
    actors_controller.populate_actors_table_from_temp()

    print("🔄 Creating temp_directors table...")
    directors_controller = DirectorsController()
    directors_controller.create_temp_directors_table()
    
    print("🔄 Populating the directors table from temp_directors...")
    directors_controller.populate_directors_table_from_temp()

    # ERD compliant naming
    print("🔄 Creating temp_actor_titles table...")
    actor_titles_controller = ActorTitlesController()
    actor_titles_controller.create_temp_actor_titles_table()
    
    print("🔄 Populating the actor_titles table from temp_actor_titles...")
    actor_titles_controller.populate_actor_titles_table_from_temp()

    print("🔄 Creating temp_director_titles table...")
    director_titles_controller = DirectorTitlesController()
    director_titles_controller.create_temp_director_titles_table()
    
    print("🔄 Populating the director_titles table from temp_director_titles...")
    director_titles_controller.populate_director_titles_table_from_temp()

    # STEP 5: PROCESS CATEGORY/COUNTRY-TITLE JUNCTION TABLES
    print("\n" + "="*60)
    print("🌍 STEP 5: PROCESSING CATEGORY/COUNTRY-TITLE JUNCTION TABLES")
    print("="*60)
    
    print("🔄 Creating temp_categories_titles table...")
    categories_titles_controller = CategoriesTitlesController()
    categories_titles_controller.create_temp_categories_titles_table()
    
    print("🔄 Populating the categories_titles table from temp_categories_titles...")
    categories_titles_controller.populate_categories_titles_table_from_temp()

    print("🔄 Creating temp_countries_titles table...")
    countries_titles_controller = CountriesTitlesController()
    countries_titles_controller.create_temp_countries_titles_table()
    
    print("🔄 Populating the countries_titles table from temp_countries_titles...")
    countries_titles_controller.populate_countries_titles_table_from_temp()

    # Final status check
    print("\n" + "=" * 80)
    print("🎉 Netflix Data Processing Pipeline Complete!")
    print("   All tables now follow ERD naming conventions!")
    print("=" * 80)
    
    print("\n📊 FINAL PROCESSING SUMMARY:")
    tracker.print_processing_dashboard()

    print("\n📋 FINAL TABLE SUMMARY:")
    print("   MAIN TABLES (6):")
    print("   ✅ people")
    print("   ✅ ratings") 
    print("   ✅ title_types")
    print("   ✅ categories")
    print("   ✅ countries")
    print("   ✅ titles")
    print("   ")
    print("   JUNCTION TABLES - Legacy Naming (4):")
    print("   ✅ actors (person_id, title_id)")
    print("   ✅ directors (person_id, title_id)")
    print("   ✅ title_categories (title_id, category_id)")
    print("   ✅ title_countries (title_id, country_id)")
    print("   ")
    print("   JUNCTION TABLES - ERD Compliant Naming (4):")
    print("   ✅ actor_titles (person_id, title_id)")
    print("   ✅ director_titles (person_id, title_id)")
    print("   ✅ categories_titles (category_id, title_id)")
    print("   ✅ countries_titles (country_id, title_id)")
    print("   ")
    print("   TRACKING TABLE (1):")
    print("   ✅ processing_status")
    print("\n" + "="*80)
    print("📝 SUMMARY:")
    print("   ✅ Total Main Tables: 6")
    print("   ✅ Total Junction Tables: 8 (4 legacy + 4 ERD compliant)")
    print("   ✅ Total Tracking Tables: 1")
    print("   ✅ GRAND TOTAL: 15 production tables")
    print("   ")
    print("   🔧 ERD Corrections Made:")
    print("   ✅ title_categories → categories_titles")
    print("   ✅ title_countries → countries_titles")
    print("   ✅ actors → actor_titles (also available)")
    print("   ✅ directors → director_titles (also available)")
    print("="*80)


if __name__ == "__main__":
    main()
