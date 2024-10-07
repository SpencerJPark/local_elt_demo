# Scripts for cleaning and processing data
import pandas as pd
import numpy as np
import holidays
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cleans the data by dropping rows with missing latitude/longitude values.
# Future versions of this function could include more advanced data cleaning steps 
# like replacing the missing values based on other columns, though best practice would
# be to handle missing values during data ingestion.
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Cleaning data by dropping rows with missing latitude/longitude")
        df_cleaned = df.dropna(subset=['Org_airport_lat', 'Org_airport_long', 'Dest_airport_lat', 'Dest_airport_long'])
        df_cleaned['Fly_date'] = pd.to_datetime(df_cleaned['Fly_date'])
        logging.info(f"Cleaned data has {len(df_cleaned)} rows after dropping NaNs")
        return df_cleaned
    except KeyError as e:
        logging.error(f"Key error: {str(e)}. Ensure the required columns exist in the data.")
        raise e
    except Exception as e:
        logging.error(f"An error occurred while cleaning the data: {str(e)}")
        raise e

# Adds date-related features such as Year, Month, Day of the Week, etc. Useful for machine learning models.
def add_date_features(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Adding date-related features")
        df['Year'] = df['Fly_date'].dt.year
        df['Month'] = df['Fly_date'].dt.month
        df['Day_of_Week'] = df['Fly_date'].dt.dayofweek
        df['Day_of_Month'] = df['Fly_date'].dt.day
        df['Quarter'] = df['Fly_date'].dt.quarter
        reference_date = pd.Timestamp('1990-01-01')
        df['Days_Since_Reference'] = (df['Fly_date'] - reference_date).dt.days
        logging.info("Date features added successfully")
        return df
    except Exception as e:
        logging.error(f"An error occurred while adding date features: {str(e)}")
        raise e

# Adds cyclic features for Month (sine and cosine transformations).
def add_cyclic_features(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Adding cyclic features (sine and cosine transformations for Month)")
        df['Month_Sin'] = np.sin(2 * np.pi * df['Month'] / 12)
        df['Month_Cos'] = np.cos(2 * np.pi * df['Month'] / 12)
        logging.info("Cyclic features added successfully")
        return df
    except Exception as e:
        logging.error(f"An error occurred while adding cyclic features: {str(e)}")
        raise e

# Adds a holiday feature based on the Fly_date column.
def add_holiday_feature(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Adding holiday feature (checking US holidays)")
        us_holidays = holidays.US()
        df['Is_Holiday'] = df['Fly_date'].isin(us_holidays).astype(int)
        logging.info("Holiday feature added successfully")
        return df
    except Exception as e:
        logging.error(f"An error occurred while adding holiday features: {str(e)}")
        raise e

# Adds a season feature based on the Month column.
def add_season_feature(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Adding season feature based on Month")
        def get_season(month: int) -> str:
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8]:
                return 'Summer'
            else:
                return 'Fall'
        
        df['Season'] = df['Month'].apply(get_season)
        logging.info("Season feature added successfully")
        return df
    except Exception as e:
        logging.error(f"An error occurred while adding season features: {str(e)}")
        raise e

# Main function to run the data processing pipeline
def main(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Starting the data processing")

        # Step 1: Clean the data
        df_cleaned = clean_data(df)
        
        # Step 2: Add date-related features
        df_cleaned = add_date_features(df_cleaned)
        
        # Step 3: Add cyclic features for Month
        df_cleaned = add_cyclic_features(df_cleaned)
        
        # Step 4: Add holiday information
        df_cleaned = add_holiday_feature(df_cleaned)
        
        # Step 5: Add season information
        df_cleaned = add_season_feature(df_cleaned)
        
        logging.info("Data processing completed successfully")
        
        # Return the cleaned and processed DataFrame
        return df_cleaned
    
    except Exception as e:
        logging.error(f"An error occurred in the data processing pipeline: {str(e)}")
        raise e

if __name__ == "__main__":
    main()