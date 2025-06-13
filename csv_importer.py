from controllers.csv_controller import CSVController
from controllers.temp_netflix_titles_controller import TempNetflixTitlesController
from controllers.people_controller import PeopleController



def main():

    # # Define the path to the CSV file
    # csv_path = "input/netflix_titles.csv"

    # # Save CSV file in the database
    # print("Saving CSV to database...")

    # # Initialize the CSV handler
    # netflix_csv = CSVController(csv_path)
    # netflix_csv.save_csv_to_database(
    #     table_name="temp_netflix_titles",
    #     schema="public"
    # )


    # # Set missing directors
    print("Setting missing directors...")
    # # Initialize the temporary Netflix titles controller
    temp_netflix_titles_controller = TempNetflixTitlesController()
    temp_netflix_titles_controller.set_missing_directors()

    # # Set missing cast
    # print("Setting missing cast...")
    # temp_netflix_titles_controller.set_missing_actors()

    # # Set missing countries
    # print("Setting missing countries...")
    # temp_netflix_titles_controller.set_missing_countries()

    # #print("Filling in the people table...")
    # people_controller = PeopleController()
    # people_controller.create_temp_people_table()

    print("🔄 Populating the people table from temp_people using Gemini...")
    people_controller = PeopleController()
    people_controller.populate_people_table_from_temp()


    ## DO NOT WORRY ABOUT THIS SECTION FOR NOW
    # # Populate the people table from cast in the temporary Netflix titles repository
    # print("Populating the people table from cast in the temporary Netflix titles repository...")
    # # Initialize the people controller
    # people_controller = PeopleController()
    # people_controller.populate_people_table_from_cast()


if __name__ == "__main__":
    main()
