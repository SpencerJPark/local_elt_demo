# Air Travel Insights: A Basic ELT Pipeline Demo

## Project Overview
This project demonstrates the development of a local ETL Pipeline. The focus is on leveraging data engineering techniques and advanced dashboards to help travel agencies or related businesses make informed decisions based on travel demand and customer behavior predictions.

## Key Features
- **Data Engineering**: An ELT pipeline for the automated ingestion and transformation of travel data (CSV, JSON, etc.) into a centralized data warehouse.
- **Scalable Workflow**: The current process leverages SQLite and Pandas for simplicity, but it can be refactored to use a data warehouse, distributed computing, and a workflow orchestration tool (I would use BigQuery, PySpark, and Airflow).

## Tools and Technologies
- **Python**: Data manipulation, preprocessing, and ingestion.
- **SQLite**: Data storage and querying for a lightweight database.
- **SQL**: Data queries and storage optimization.
- **Jupyter Notebooks**: Used for exploration and experimentation.

## Data Considerations

### Handling Missing Data
During the data cleaning process, I noticed that some rows had missing latitude and longitude values for the origin and destination airports. These rows were dropped because they represented a small fraction of the dataset. However, for a production-level implementation, it would be ideal to handle these missing values more efficiently by filling in missing values based on other available columns, as the airport locations are consistent and easily verifiable. This would reduce data loss and ensure that any necessary transformations happen early in the ingestion pipeline, avoiding unnecessary processing later.

The dataset used for this project was sourced from [Kaggle](https://www.kaggle.com/datasets/flashgordon/usa-airport-dataset). While the exact cause of the missing data couldn't be determined, further investigation could be done to ensure that the data source is clean in a production setting.

## Machine Learning Potential

### Regression Model for Travel Demand Prediction
Initially, I explored building a regression model to predict travel demand. However, due to limitations in both the dataset and the time frame for this project, the machine-learning model yielded only a partial result. The models passed R² tests but failed to perform well with Mean Squared Error, likely due to inconsistencies in holiday traffic patterns. Addressing the variability would require more complex modeling, including categorical and numerical features.

In the future, missing data could be imputed based on airport locations, and holiday effects could be better modeled to improve accuracy. However, for now, I have shifted focus away from ML for this project to focus on data engineering and visualization.

## Scalability and Future Improvements
While the current project leverages **SQLite** for data storage and **Tableau Public** for visualizations, this process can be scaled up significantly:

### **Data Warehouse Integration**
The SQLite database could be replaced with a more robust data warehouse (such as AWS Redshift, Google BigQuery, or Snowflake) to handle larger datasets and enable more powerful querying. This would make the pipeline more suitable for larger enterprises needing to analyze vast travel data.

### **Distributed Computing with PySpark**
Currently, data manipulation is done using **Pandas**, which works well for small—to medium-sized datasets that can fit in memory. However, for larger datasets that exceed memory limits, **PySpark** can be a scalable alternative to Pandas. PySpark operates on **Spark DataFrames**, which allow for distributed computation across multiple nodes in a cluster, making it possible to process massive datasets efficiently.

Switching to PySpark for distributed computing would allow the data processing pipeline to scale horizontally and handle larger datasets more effectively. PySpark splits large datasets into smaller chunks across multiple nodes, enabling parallel processing, while Pandas operates on a single machine. Additionally, PySpark provides fault tolerance by automatically recovering from node failures and memory management by efficiently handling data that exceeds the memory capacity of a single machine.

### **Automated Data Pipeline**
The pipeline could be automated to ingest new data as it becomes available, enabling real-time insights and reducing manual intervention. Tools like **Apache Airflow** could orchestrate the data pipeline, automating ingestion, processing, and storage in a scalable environment.

These improvements would support a more production-ready pipeline to handle higher data volumes, provide faster querying, and deliver more efficient insights.

### Try it Out
To test this pipeline, first set up and activate a Python virtual environment and then download the requirements listed in requirements.txt. Then, download the dataset from [Kaggle](https://www.kaggle.com/datasets/flashgordon/usa-airport-dataset) and place it in the data/raw folder. Once this setup is complete, run app.py from the root directory. The pipeline will then activate, process the data, save the data to the local SQLite database located in the data/processed folder, and then run the SQL model on the database to extract information into a new CSV, which will be located in the data/external directory.
