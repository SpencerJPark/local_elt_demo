from src import data_ingestion, tableau_public_data_extraction

# Main function that orchestrates the entire pipeline
def main() -> None:
    # Step 1: Load data from CSV and import it into the SQLite database
    print("Starting data ingestion...")
    data_ingestion.main()

    # Step 2: Extract data for Tableau Public
    print("Extracting data for Tableau Public...")
    tableau_public_data_extraction.main()

    print("Data pipeline completed successfully")

if __name__ == "__main__":
    main()
