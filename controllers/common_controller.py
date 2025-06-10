import json

from controllers.gemini_controller import GeminiController

class CommonController:
    """
    Common controller for handling common operations.
    """

    def __init__(self):
        pass


    def parse_full_name(self, full_name: str) -> dict:
        """
        Parse the first, middle, and last names from a full name string using the Gemini model.
        Args:
            name (str): The full name to parse.
        Returns:
            dict: A dictionary containing the first, middle, and last names.
        """

        # Craft the prompt for Gemini
        prompt = f"""

        What is the person's first_name, middle_name, and last_name: {full_name}?

        Return the result in a structured JSON. If the first_name, middle_name or last_name are unknown, return "unknown" as the value. For example:
        
            - full_name: Milton Davila
            - first_name: Milton
            - middle_name: unknown
            - last_name: Davila

        """

        # Initialize the Gemini model
        gemini_controller = GeminiController(model_name="gemini-2.0-flash")

        # Generate content using the Gemini model
        response = gemini_controller.model.generate_content(prompt)

        # Parse the JSON response
        response_json = json.loads(response.text)

        return response_json
