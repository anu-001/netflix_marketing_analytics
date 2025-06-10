import pandas as pd
from sqlalchemy import create_engine

from config import DB_CONFIG


class CSVController():
    """
    A class to handle CSV file operations, specifically saving CSV data to a PostgreSQL database.
    """

    def __init__(self, csv_path: str):
        """
        Initialize the CSV handler with the path to the CSV file.
        
        Args:
            csv_path (str): Path to the CSV file.
        """
        self.csv_path = csv_path



    def save_csv_to_database(self, table_name: str, schema: str) -> None:
        """
        Save the CSV file to a PostgreSQL database table.
        Args:
            table_name (str): Name of the table to save the data to.
            schema (str): Schema in which the table resides.
        """
        try:
            # Read the CSV file
            df = pd.read_csv(self.csv_path, encoding="utf-8")

            # Create SQLAlchemy engine directly from config
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
            print(f"Successfully saved data to table '{table_name}' in schema '{schema}'.")
        except Exception as e:
            print(f"Error processing CSV file or saving to database: {e}")
