# Marketing Analytics for Netflix

## Setup Instructions

1. Copy `.env.example` to `.env` and fill in your actual values:
   ```bash
   cp .env.example .env
   ```

2. Update the `.env` file with your actual credentials:
   - Set your database credentials (DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD, DB_PORT)
   - Set your GCP project id for Gemini (GCP_PROJECT_ID)
   - Set your GCP location if different from us-central1 (GCP_LOCATION)
   - Verify the service account path (GOOGLE_APPLICATION_CREDENTIALS)

3. Save your Service Account in the "credentials" folder with the name "service-account.json"

4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```