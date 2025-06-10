import json

from repositories.temp_netflix_titles_repository import TempNetflixTitlesRepository

from controllers.gemini_controller import GeminiController
import time

class TempNetflixTitlesController:
    """
    A class to represent a temporary storage for Netflix titles.
    """

    def __init__(self):
        pass


    def set_missing_directors(self):
        """
        Set missing directors for records in the temporary Netflix titles repository.
        This method retrieves records with null directors, queries the Gemini model for missing directors,
        and updates the records in the database.
        """

        # Get all records from the temporary Netflix titles repository
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        null_directors = temp_netflix_titles_repo.get_null_directors()

        # Iterate over the records
        for record in null_directors[:1000]:

            time.sleep(1)  # Sleep for 1 second to avoid rate limiting  
            # Print the record
            print("\n", record)

            missing_directors = self.get_missing_directors(
                type=record["type"],
                title=record["title"],
                cast=record["cast"],
                country=record["country"],
                release_year=record["release_year"]
            )
            # Print the missing directors
            print(f"Missing directors: {missing_directors}")
            
            # Update the record in the database
            try:
                temp_netflix_titles_repo.update_director(
                    show_id=record["show_id"],
                    director=missing_directors["directors"],
                )
                print(f"Updated record with show_id {record['show_id']} successfully.")
            except Exception as e:
                print(f"Error updating record with show_id {record['show_id']}: {e}")
                raise e


    def get_missing_directors(self, type: str, title: str, cast: str, country: str, release_year: str) -> str:
        """
        Get missing directors for a given title and cast using the Gemini model.
        Args:
            type (str): The type of the title (e.g., "movie", "tv show").
            title (str): The title of the movie or series.
            cast (str): The cast of the movie or series.
            country (str): The country of the movie or series.
            release_year (str): The release year of the movie or series.
        Returns:
            str: A list of directors.
        """

        # Craft the prompt for Gemini
        prompt = f"""

        Who is the director of this {type} named {title} and the cast is {cast} produced in {country} released in {release_year}?

        Return the result in a structured JSON. If there is more than one director, use a coma to join them using this format:

            - directors: (the director's first, middle and last name, second director's first, middle and last name, ...)

        If you don't know the answer, return directors: unknown.
        """

        # Initialize the Gemini model
        director_generator = GeminiController()

        # Generate content using the Gemini model
        response = director_generator.model.generate_content(prompt)

        # Parse the JSON response
        response_json = json.loads(response.text)

        return response_json


    def set_missing_actors(self):
        """
        Set missing actors for records in the temporary Netflix titles repository.
        This method retrieves records with null cast, queries the Gemini model for missing actors,
        and updates the records in the database.
        """

        # Get all records from the temporary Netflix titles repository
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        null_actors = temp_netflix_titles_repo.get_null_actors()

        # Iterate over the records
        for record in null_actors[:500]:

            # time.sleep(1)  # Sleep for 1 second to avoid rate limiting  
            # Print the record
            print("\n", record)

            missing_actors = self.get_missing_actors(
                type=record["type"],
                title=record["title"],
                director=record["director"],
                release_year=record["release_year"],
                country=record["country"],
            )
            # Print the missing actors
            print(f"Missing actors: {missing_actors}")
            
            # Update the record in the database
            try:
                temp_netflix_titles_repo.update_cast(
                    show_id=record["show_id"],
                    cast=missing_actors["cast"],
                )
                print(f"Updated record with show_id {record['show_id']} successfully.")
            except Exception as e:
                print(f"Error updating record with show_id {record['show_id']}: {e}")
                raise e


    def get_missing_actors(self, type: str, title: str, director: str, release_year: str, country: str) -> str:
        """
        Get missing actors for a given title and director using the Gemini model.
        Args:
            type (str): The type of the title (e.g., "movie", "tv show").
            title (str): The title of the movie or series.
            director (str): The director of the movie or series.
            release_year (str): The release year of the movie or series.
            country (str): The country of the movie or series.
        Returns:
            str: A list of actors.
        """

        # Craft the prompt for Gemini
        prompt = f"""

        Who are the main actors in this {type} named {title} released in {release_year} directed by {director} produced in {country}?

        Return the result in a structured JSON. If there is more than one actor, use a comma to join them using this format:

            - cast: (the actor's first, middle and last name, second actor's first, middle and last name, ...)

        If you don't know the answer, return cast: unknown.
        """

        # Initialize the Gemini model
        actor_generator = GeminiController()

        # Generate content using the Gemini model
        response = actor_generator.model.generate_content(prompt)

        # Parse the JSON response
        response_json = json.loads(response.text)

        return response_json


    def set_missing_countries(self):
        """
        Set missing countries for records in the temporary Netflix titles repository.
        This method retrieves records with null countries, queries the Gemini model for missing countries,
        and updates the records in the database.
        """

        # Get all records from the temporary Netflix titles repository
        temp_netflix_titles_repo = TempNetflixTitlesRepository()
        null_countries = temp_netflix_titles_repo.get_null_countries()

        # Iterate over the records
        for record in null_countries[:500]:

            # time.sleep(1)  # Sleep for 1 second to avoid rate limiting  
            # Print the record
            print("\n", record)

            missing_countries = self.get_missing_countries(
                type=record["type"],
                title=record["title"],
                release_year=record["release_year"],
            )
            # Print the missing countries
            print(f"Missing countries: {missing_countries}")
            
            # Update the record in the database
            try:
                temp_netflix_titles_repo.update_country(
                    show_id=record["show_id"],
                    country=missing_countries["countries"],
                )
                print(f"Updated record with show_id {record['show_id']} successfully.")
            except Exception as e:
                print(f"Error updating record with show_id {record['show_id']}: {e}")
                raise e


    def get_missing_countries(self, type: str, title: str, release_year: str) -> str:
        """
        Get missing countries for a given title using the Gemini model.
        Args:
            type (str): The type of the title (e.g., "movie", "tv show").
            title (str): The title of the movie or series.
            release_year (str): The release year of the movie or series.
        Returns:
            str: A list of countries.
        """

        # Craft the prompt for Gemini
        prompt = f"""

        What country or countries produced this {type} named {title}, release {release_year}?

        Return the result in a structured JSON. If there is more than one country, use a comma to join them using this format:

            - countries: (country1, country2, country3, ...)

        For example: "United States, United Kingdom" or "France, Germany, Spain"

        If you don't know the answer, return countries: unknown.
        """

        # Initialize the Gemini model
        country_generator = GeminiController()

        # Generate content using the Gemini model
        response = country_generator.model.generate_content(prompt)

        # Parse the JSON response
        response_json = json.loads(response.text)

        return response_json
