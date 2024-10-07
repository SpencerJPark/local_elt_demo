import pandas as pd
import sqlite3

# Function to load the SQL query from a file
def load_sql_query(file_path: str) -> str:
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# Execute SQL query on SQLite database
def execute_query(query: str, db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Save DataFrame to CSV file
def save_to_csv(df: pd.DataFrame, output_path: str) -> None:
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")

def main() -> None:
    # Path to SQLite database
    db_path = 'data/processed/cleaned_data.db'
    
    # Path to the SQL file
    sql_file_path = 'models/tableau_query.sql'
    
    # Load the SQL query from the file
    query = load_sql_query(sql_file_path)
    
    # Execute query
    df = execute_query(query, db_path)
    
    # Save result to CSV for Tableau Public
    save_to_csv(df, 'data/external/passengers_by_route.csv')

if __name__ == "__main__":
    main()
