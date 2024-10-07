import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from src.data_processing import main as process_data  # Import the main function from data_processing

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Loads data from a CSV file into a pandas DataFrame.
# Data is from https://www.kaggle.com/datasets/flashgordon/usa-airport-dataset
def load_data(filepath: str) -> pd.DataFrame:
    try:
        logging.info(f"Loading data from {filepath}")
        if not os.path.exists(filepath):
            logging.error(f"File not found: {filepath}. Please check the file path.")
            raise FileNotFoundError(f"File not found: {filepath}")
        
        df = pd.read_csv(filepath)
        logging.info(f"Loaded {len(df)} rows from {filepath}")
        return df
    except FileNotFoundError as e:
        logging.error(f"File not found: {filepath}. Please check the file path.")
        raise e
    except pd.errors.EmptyDataError as e:
        logging.error(f"File is empty: {filepath}")
        raise e
    except Exception as e:
        logging.error(f"An error occurred while loading the data: {str(e)}")
        raise e

# Saves the cleaned DataFrame to an SQLite database.
def save_to_sqlite(df: pd.DataFrame, database_path: str, table_name: str = 'cleaned_flight_data') -> create_engine:
    try:
        logging.info(f"Saving DataFrame to SQLite database at {database_path}")
        if not os.path.exists(os.path.dirname(database_path)):
            os.makedirs(os.path.dirname(database_path))
        
        engine = create_engine(f'sqlite:///{database_path}')
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data saved to SQLite database at {database_path}")
        return engine
    except Exception as e:
        logging.error(f"An error occurred while saving to SQLite: {str(e)}")
        raise e

# Queries data from the SQLite database.
def query_from_sqlite(engine: create_engine, table_name: str) -> pd.DataFrame:
    try:
        logging.info(f"Querying data from SQLite table {table_name}")
        df_from_sql = pd.read_sql(table_name, engine)
        logging.info(f"Queried {len(df_from_sql)} rows from {table_name}")
        return df_from_sql
    except Exception as e:
        logging.error(f"An error occurred while querying SQLite: {str(e)}")
        raise e

def main() -> None:
    # Step 1: Load raw data
    df = load_data('data/raw/Airports2.csv')
    
    # Step 2: Clean and process the data by calling the processing pipeline
    df_cleaned = process_data(df)
    
    # Step 3: Save the cleaned DataFrame to SQLite database
    database_path = 'data/processed/cleaned_data.db'
    engine = save_to_sqlite(df_cleaned, database_path)
    
    # Step 4: Query the cleaned data from SQLite and display
    df_from_sql = query_from_sqlite(engine, 'cleaned_flight_data')
    print(df_from_sql.head())

# Correct entry point
if __name__ == "__main__":
    main()
