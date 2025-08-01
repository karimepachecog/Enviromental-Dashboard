# Import pymongo library for MongoDB operations
import pymongo
# Import MongoClient to establish connection with MongoDB database
from pymongo import MongoClient
# Import datetime to handle timestamps and date operations
from datetime import datetime
# Import logging for error handling and debugging information
import logging

# Function to load raw data into MongoDB
def raw_data_load(ti):
    try:
        # Establish connection to MongoDB running on the host machine via Docker
        client = MongoClient("mongodb://host.docker.internal:27017/")
        # Access the database named 'environmental_data_orchestration'
        db = client["environmental_data_orchestration"]
        # Access the collection named 'raw_data' where extracted data will be stored
        collection = db["raw_data"]
    except Exception as e:
        # Log an error if the connection fails
        logging.error(f"Couldn't connect to MongoDB --> {e}")
        return {"status": "error", "message": "Couldn't connect to MongoDB"}

    try:
        # Pull extracted data from previous Airflow tasks using XCom
        news_data = ti.xcom_pull(task_ids="extract_news_task", key="extracted_news_data")
        weather_data = ti.xcom_pull(task_ids="extract_weather_task", key="extracted_weather_data")
        air_quality_data = ti.xcom_pull(task_ids="extract_air_quality_task", key="extracted_air_quality_data")

        # Log how many items were extracted in each dataset
        logging.info(f"[raw_data_load] News: {len(news_data) if news_data else 0}, Weather: {len(weather_data) if weather_data else 0}, Air: {len(air_quality_data) if air_quality_data else 0}")

        # Proceed only if none of the datasets are None (they can be empty lists)
        if news_data is not None and weather_data is not None and air_quality_data is not None:
            # Create a document to insert into MongoDB with a stable timestamp
            raw_document = {
                "news_data": news_data,
                "weather_data": weather_data,
                "air_quality_data": air_quality_data,
                "loaded_at": ti.execution_date.isoformat()  # Execution time from Airflow
            }
            # Insert the document into the collection
            collection.insert_one(raw_document)
            logging.info("Air pollution data successfully inserted into MongoDB")
            return {"status": "success", "message": "Data loaded successfully"}
        else:
            # Log and return an error if any extraction failed (returned None)
            logging.error("Missing data from XCom - one or more extraction tasks failed (None)")
            return {"status": "error", "message": "Missing data from extraction tasks"}
    except Exception as e:
        # Log and return an error if insertion fails
        logging.error(f"Couldn't insert the data into MongoDB --> {e}")
        return {"status": "error", "message": f"Database insertion failed: {str(e)}"}
    finally:
        try:
            # Always attempt to close the MongoDB connection
            client.close()
        except:
            pass
