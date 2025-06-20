import os
import vertexai
from vertexai.generative_models import GenerativeModel
from dotenv import load_dotenv
from config import GEMINI_CONFIG
import json

load_dotenv()
class GeminiController:
    """
    Controller for managing interactions with the Gemini model on Google Cloud Vertex AI.
    """

    def __init__(self, model_name="gemini-2.0-flash"):
        """
        Initializes the GeminiController with a specified model and sets up the Gemini model.

        Args:
            model_name (str): Name of the Gemini model to use. Defaults to "gemini-2.0-flash".
        """
        self.project_id = GEMINI_CONFIG["project_id"]
        self.location = GEMINI_CONFIG["location"]
        self.model_name = model_name

        # Set the path to the service account key file
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials/service-account.json")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        # Initialize Vertex AI with project and location
        vertexai.init(project=self.project_id, location=self.location)

        # Initialize the Gemini model (using Gemini Flash)
        self.model = GenerativeModel(
            model_name=self.model_name,
            generation_config={"response_mime_type": "application/json"},
        )

    def deduce_rating_from_title(self, title, description=None, release_year=None):
        """
        Use Gemini AI to deduce the most appropriate rating for a title based on its information.
        
        Args:
            title (str): The title of the movie/show
            description (str, optional): Description of the content
            release_year (int, optional): Release year of the content
            
        Returns:
            str: The deduced rating (e.g., 'G', 'PG', 'PG-13', 'R', 'TV-MA', etc.)
        """
        try:
            # Create a comprehensive prompt for rating deduction
            prompt = f"""
            You are an expert content analyst. Based on the following information about a Netflix title, 
            deduce the most appropriate content rating. Consider typical Netflix/streaming content ratings.

            Title: {title}
            Description: {description if description else 'Not provided'}
            Release Year: {release_year if release_year else 'Not provided'}

            Please analyze the title and description to determine the most likely content rating.
            Common Netflix ratings include:
            - G (General Audiences)
            - PG (Parental Guidance)
            - PG-13 (Parents Strongly Cautioned)
            - R (Restricted)
            - TV-Y (Children)
            - TV-Y7 (Children 7+)
            - TV-G (General)
            - TV-PG (Parental Guidance)
            - TV-14 (Parents Strongly Cautioned)
            - TV-MA (Mature Audiences)
            - NR (Not Rated)

            Consider:
            1. Content type (movie vs TV show)
            2. Subject matter from title and description
            3. Target audience
            4. Release year context
            5. Typical ratings for similar content

            Return your response as a JSON object with the following structure:
            {{
                "rating": "RATING_CODE",
                "confidence": "high|medium|low",
                "reasoning": "Brief explanation of why this rating was chosen"
            }}
            """

            response = self.model.generate_content(prompt)
            result = json.loads(response.text)
            
            print(f"🤖 Gemini deduced rating for '{title}': {result['rating']} (confidence: {result['confidence']})")
            print(f"   Reasoning: {result['reasoning']}")
            
            return result['rating']
            
        except Exception as e:
            print(f"⚠️ Error using Gemini to deduce rating for '{title}': {e}")
            # Return a default rating if Gemini fails
            return "NR"  # Not Rated as fallback
