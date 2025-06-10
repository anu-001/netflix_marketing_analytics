import os
import vertexai
from vertexai.generative_models import GenerativeModel

from config import GEMINI_CONFIG


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
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            "credentials/service-account.json"
        )

        # Initialize Vertex AI with project and location
        vertexai.init(project=self.project_id, location=self.location)

        # Initialize the Gemini model (using Gemini Flash)
        self.model = GenerativeModel(
            model_name=self.model_name,
            generation_config={"response_mime_type": "application/json"},
        )
