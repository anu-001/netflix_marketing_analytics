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
        Save the CSV file to a PostgreSQL database table with robust error handling.
        Args:
            table_name (str): Name of the table to save the data to.
            schema (str): Schema in which the table resides.
        """
        try:
            print(f"Reading CSV file: {self.csv_path}")
            
            # Try reading with different options to handle malformed CSV
            df = None
            
            # First attempt: Standard reading
            try:
                df = pd.read_csv(self.csv_path, encoding="utf-8")
                print(f"✅ Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
            except pd.errors.ParserError as e:
                print(f"⚠️ Standard CSV parsing failed: {e}")
                
                # Second attempt: Handle quotes and escaping better
                try:
                    df = pd.read_csv(
                        self.csv_path, 
                        encoding="utf-8",
                        quotechar='"',
                        doublequote=True,
                        escapechar='\\',
                        on_bad_lines='skip'  # Skip problematic lines
                    )
                    print(f"✅ Successfully read CSV with error handling: {len(df)} rows and {len(df.columns)} columns")
                except Exception as e2:
                    print(f"⚠️ Advanced CSV parsing failed: {e2}")
                    
                    # Third attempt: More aggressive error handling
                    try:
                        df = pd.read_csv(
                            self.csv_path, 
                            encoding="utf-8",
                            sep=',',
                            quotechar='"',
                            doublequote=True,
                            skipinitialspace=True,
                            on_bad_lines='warn',  # Warn about bad lines but continue
                            engine='python'  # Use Python engine for better error handling
                        )
                        print(f"✅ Successfully read CSV with Python engine: {len(df)} rows and {len(df.columns)} columns")
                    except Exception as e3:
                        print(f"❌ All CSV parsing attempts failed: {e3}")
                        raise e3
            
            if df is None:
                raise Exception("Failed to read CSV file")
            
            # Clean the data
            print("🧹 Cleaning data...")
            
            # Replace problematic characters in string columns
            for col in df.columns:
                if df[col].dtype == 'object':  # String columns
                    df[col] = df[col].astype(str)
                    # Replace double quotes with single quotes to avoid SQL issues
                    df[col] = df[col].str.replace('""', "'", regex=False)
                    df[col] = df[col].str.replace('"', "'", regex=False)
                    # Clean up any null strings
                    df[col] = df[col].replace('nan', None)
                    df[col] = df[col].replace('NaN', None)
                    df[col] = df[col].replace('', None)
            
            print(f"📊 Final data shape: {df.shape}")
            print(f"📋 Column names: {list(df.columns)}")

            # Create SQLAlchemy engine directly from config
            conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(conn_string)
            
            # Save to database
            print(f"💾 Saving to database table: {schema}.{table_name}")
            df.to_sql(name=table_name, con=engine.connect(), schema=schema, if_exists="replace", index=False)
            print(f"✅ Successfully saved {len(df)} rows to table '{table_name}' in schema '{schema}'.")
            
        except Exception as e:
            print(f"❌ Error processing CSV file or saving to database: {e}")
            print("🔧 Suggestion: Check the CSV file for:")
            print("   - Unescaped quotes or commas in text fields")
            print("   - Embedded newlines in data")
            print("   - Inconsistent number of columns")
            raise
        
    def clean_csv_file(self, output_path: str = None) -> str:
        """
        Clean the CSV file to fix parsing issues and save a corrected version.
        
        Args:
            output_path (str): Path to save the cleaned CSV. If None, overwrites the original.
            
        Returns:
            str: Path to the cleaned CSV file
        """
        if output_path is None:
            output_path = self.csv_path.replace('.csv', '_cleaned.csv')
        
        print(f"🧹 Cleaning CSV file: {self.csv_path}")
        
        try:
            # Read the file line by line to handle problematic rows
            cleaned_lines = []
            expected_columns = None
            
            with open(self.csv_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    # For the header row, determine expected column count
                    if line_num == 1:
                        expected_columns = len(line.split(','))
                        cleaned_lines.append(line)
                        print(f"📋 Header row found with {expected_columns} columns")
                        continue
                    
                    # Check if line has correct number of fields
                    # Simple split might not work due to commas in quoted fields
                    # So we'll use a more sophisticated approach
                    
                    try:
                        # Try to parse the line using CSV reader
                        import csv
                        from io import StringIO
                        
                        reader = csv.reader(StringIO(line))
                        fields = next(reader)
                        
                        if len(fields) == expected_columns:
                            # Line is good, add it
                            cleaned_lines.append(line)
                        else:
                            print(f"⚠️ Line {line_num}: Expected {expected_columns} fields, got {len(fields)}. Attempting to fix...")
                            
                            # Try to fix the line by ensuring proper quoting
                            if len(fields) > expected_columns:
                                # Too many fields - likely unescaped commas
                                # Attempt to merge extra fields into the description field (usually the last field)
                                if len(fields) > expected_columns:
                                    # Merge excess fields into the last field
                                    fixed_fields = fields[:expected_columns-1]
                                    merged_last_field = ','.join(fields[expected_columns-1:])
                                    fixed_fields.append(merged_last_field)
                                    
                                    # Create a properly formatted CSV line
                                    output = StringIO()
                                    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
                                    writer.writerow(fixed_fields)
                                    fixed_line = output.getvalue().strip()
                                    
                                    cleaned_lines.append(fixed_line)
                                    print(f"✅ Fixed line {line_num}")
                                else:
                                    cleaned_lines.append(line)
                            else:
                                # Too few fields - skip this line
                                print(f"❌ Skipping malformed line {line_num}: {line[:100]}...")
                                
                    except Exception as e:
                        print(f"❌ Error processing line {line_num}: {e}")
                        print(f"    Line content: {line[:100]}...")
                        # Skip problematic lines
                        continue
            
            # Write the cleaned CSV
            with open(output_path, 'w', encoding='utf-8', newline='') as output_file:
                for line in cleaned_lines:
                    output_file.write(line + '\n')
            
            print(f"✅ Cleaned CSV saved to: {output_path}")
            print(f"📊 Processed {len(cleaned_lines)} lines (including header)")
            
            return output_path
            
        except Exception as e:
            print(f"❌ Error cleaning CSV file: {e}")
            raise
