# Import MongoDB client for database operations
from pymongo import MongoClient
# Import datetime for timestamp operations
from datetime import datetime
# Import logging for proper error and info logging
import logging

# Function to load the final transformed data into MongoDB
def load(ti):
    """
    Load transformed data (news, weather, air quality) into MongoDB
    Args:
        ti: Task instance for XCom communication
    """
    try:
        # Connect to MongoDB running on Docker host
        # Using host.docker.internal to connect from Airflow container to MongoDB
        client = MongoClient("mongodb://host.docker.internal:27017/")
        # Create/select the database for environmental data orchestration
        db = client["environmental_data_orchestration"]
        # Create/select the collection for pollution monitoring data
        collection = db["pollution_monitoring"]
        logging.info("Successfully connected to MongoDB")
    except Exception as e:
        # Log error if MongoDB connection fails
        logging.error(f"Couldn't connect to MongoDB --> {e}")
        return {"status": "error", "message": "Couldn't connect to MongoDB"}

    try:
        # Retrieve transformed data from XComs using task_ids and keys
        transformed_news = ti.xcom_pull(task_ids='transform_task', key='transformed_news')
        transformed_weather = ti.xcom_pull(task_ids='transform_task', key='transformed_weather')
        transformed_air_quality = ti.xcom_pull(task_ids='transform_task', key='transformed_air_quality')
        
        # Check if all required data is available
        if transformed_news is not None and transformed_weather is not None and transformed_air_quality is not None:
            
            # Count the total records for each data type
            news_count = len(transformed_news) if transformed_news else 0
            weather_count = len(transformed_weather) if transformed_weather else 0
            air_quality_count = len(transformed_air_quality) if transformed_air_quality else 0
            
            # Create comprehensive document combining all data sources
            final_document = {
                "batch_id": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",      # Unique batch identifier
                "data_summary": {
                    "news_articles_count": news_count,                                # Number of news articles
                    "weather_cities_count": weather_count,                           # Number of cities with weather data
                    "air_quality_cities_count": air_quality_count,                   # Number of cities with air quality data
                    "total_records": news_count + weather_count + air_quality_count  # Total records in this batch
                },
                "news_data": {
                    "articles": transformed_news,                                     # All news articles about pollution
                    "extraction_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # When news was extracted
                    "source": "NewsAPI"                                              # Data source identifier
                },
                "weather_data": {
                    "cities": transformed_weather,                                    # Weather data for all cities
                    "extraction_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # When weather was extracted
                    "source": "Open-Meteo Weather API"                              # Data source identifier
                },
                "air_quality_data": {
                    "cities": transformed_air_quality,                               # Air quality data for all cities
                    "extraction_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # When air quality was extracted
                    "source": "Open-Meteo Air Quality API"                          # Data source identifier
                }
            }
            
            # Insert the complete document into MongoDB collection
            result = collection.insert_one(final_document)
            
            # Log successful insertion with document ID
            logging.info(f"Data successfully inserted into MongoDB with ID: {result.inserted_id}")
            logging.info(f"Batch summary: {news_count} news articles, {weather_count} weather records, {air_quality_count} air quality records")
            logging.info(f"Json uploaded: {final_document}")
            
            # Optional: Create indexes for better query performance
            try:
                # Create index on batch_id for efficient batch queries
                collection.create_index("batch_id")
                # Create index on loaded timestamp for time-based queries
                collection.create_index("pipeline_metadata.loaded_at")
                # Create compound index for city-based queries
                collection.create_index([
                    ("weather_data.cities.city", 1),
                    ("air_quality_data.cities.city", 1)
                ])
                logging.info("Database indexes created successfully")
            except Exception as index_error:
                # Log index creation errors (non-critical)
                logging.warning(f"Could not create indexes --> {index_error}")
            
        else:
            # Log warning if some data is missing
            missing_data = []
            if transformed_news is None:
                missing_data.append("news")
            if transformed_weather is None:
                missing_data.append("weather")
            if transformed_air_quality is None:
                missing_data.append("air_quality")
            
            logging.error(f"Missing data from XCom: {', '.join(missing_data)}")
            
            # Insert partial data if at least one data source is available
            if any([transformed_news, transformed_weather, transformed_air_quality]):
                partial_document = {
                    "batch_id": f"partial_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    "status": "partial_data_load",
                    "available_data": {
                        "news": transformed_news if transformed_news else [],
                        "weather": transformed_weather if transformed_weather else [],
                        "air_quality": transformed_air_quality if transformed_air_quality else []
                    },
                    "missing_data_types": missing_data,
                    "loaded_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Insert partial document
                result = collection.insert_one(partial_document)
                logging.info(f"Partial data inserted with ID: {result.inserted_id}")
                logging.info(f"Partial Json uploaded: {partial_document}")
            
    except Exception as e:
        # Log error if data insertion fails
        logging.error(f"Couldn't insert the data into MongoDB --> {e}")
        
    
    finally:
        # Always close the MongoDB connection
        try:
            client.close()
            logging.info("MongoDB connection closed")
        except:
            # Log if connection close fails
            logging.warning("Could not close MongoDB connection properly")

