# Import datetime to work with timestamps
from datetime import datetime
# Import logging for error and status messages
import logging

# Main transform function used in Airflow
def transform(ti):
    try:
        # Retrieve extracted data from previous Airflow tasks using XCom
        news_data = ti.xcom_pull(key="extracted_news_data", task_ids="extract_news_task")
        weather_data = ti.xcom_pull(key="extracted_weather_data", task_ids="extract_weather_task")
        air_quality_data = ti.xcom_pull(key="extracted_air_quality_data", task_ids="extract_air_quality_task")
    except Exception as e:
        # Log error if pulling data fails
        print(f"Couldn't pull the data from xcom --> {e}")
        return

    # ---- Transform news data ----
    try:
        transformed_news = []

        # Process each news article if data exists and no error key is present
        if news_data and not isinstance(news_data, dict) or not news_data.get('error'):
            for article in news_data:
                clean_article = {
                    "title": article.get("title", "").strip(),  # Remove whitespace from title
                    "description": article.get("description", "").strip(),
                    "source": article.get("source", {}).get("name", "Unknown"),
                    "author": article.get("author", "Unknown"),
                    "url": article.get("url", ""),
                    "published_at": article.get("publishedAt", ""),
                    "city": article.get("city", "Unknown"),
                    "content_preview": article.get("content", "")[:200] if article.get("content") else "",
                    "url_to_image": article.get("urlToImage", ""),
                    "word_count": len(article.get("title", "").split()) if article.get("title") else 0,
                    "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Add processing timestamp
                }
                transformed_news.append(clean_article)

        # Push cleaned news data to XCom for later use
        ti.xcom_push(key='transformed_news', value=transformed_news)
        print(f"News data successfully transformed: {len(transformed_news)} articles")
    except Exception as e:
        # If transformation fails, push an empty list to XCom
        print(f"Couldn't transform news data --> {e}")
        ti.xcom_push(key='transformed_news', value=[])

    # ---- Transform weather data ----
    transformed_weather = []

    for entry in weather_data:
        try:
            city = entry.get("city", "Unknown")  # Fallback in case city is missing
            hourly = entry.get("hourly", {})     # Access hourly weather metrics

            # Get time and metrics; skip None values
            time_list = hourly.get("time", [])
            temp_list = [x for x in hourly.get("temperature_2m", []) if x is not None]
            humid_list = [x for x in hourly.get("relative_humidity_2m", []) if x is not None]
            precip_list = [x for x in hourly.get("precipitation", []) if x is not None]

            # Build hourly entries
            hourly_data = []
            for i in range(len(time_list)):
                hourly_data.append({
                    "datetime": time_list[i],
                    "temperature_2m": hourly.get("temperature_2m", [None]*len(time_list))[i],
                    "relative_humidity_2m": hourly.get("relative_humidity_2m", [None]*len(time_list))[i],
                    "precipitation": hourly.get("precipitation", [None]*len(time_list))[i]
                })

            # Calculate averages, guarding against division by zero
            transformed_weather.append({
                "city": city,
                "hourly_data": hourly_data,
                "averages": {
                    "temperature_2m": round(sum(temp_list) / len(temp_list), 2) if temp_list else None,
                    "relative_humidity_2m": round(sum(humid_list) / len(humid_list), 2) if humid_list else None,
                    "precipitation": round(sum(precip_list) / len(precip_list), 2) if precip_list else None
                }
            })
        except Exception as e:
            # Log issues during transformation per city
            logging.warning(f"Error transforming weather data for {entry.get('city', 'Unknown')}: {e}")

    # Push cleaned weather data to XCom
    ti.xcom_push(key="transformed_weather", value=transformed_weather)
    logging.info("Weather data successfully transformed")

    # ---- Transform air quality data ----
    transformed_air_quality = []
    for entry in air_quality_data:
        try:
            city = entry.get("city", "Unknown")
            data = entry.get("data", {})          # Open-Meteo nested data
            hourly = data.get("hourly", {})

            # Extract values from each pollutant
            time_list = hourly.get("time", [])
            pm2_5_list = [x for x in hourly.get("pm2_5", []) if x is not None]
            co_list = [x for x in hourly.get("carbon_monoxide", []) if x is not None]
            pm10_list = [x for x in hourly.get("pm10", []) if x is not None]
            no2_list = [x for x in hourly.get("nitrogen_dioxide", []) if x is not None]

            # Build hourly pollution data entries
            hourly_data = []
            for i in range(len(time_list)):
                hourly_data.append({
                    "datetime": time_list[i],
                    "pm2_5": pm2_5_list[i] if i < len(pm2_5_list) else None,
                    "carbon_monoxide": co_list[i] if i < len(co_list) else None,
                    "pm10": pm10_list[i] if i < len(pm10_list) else None,
                    "nitrogen_dioxide": no2_list[i] if i < len(no2_list) else None
                })

            # Compute average pollutants if data exists
            transformed_air_quality.append({
                "city": city,
                "hourly_data": hourly_data,
                "averages": {
                    "pm2_5": sum(pm2_5_list) / len(pm2_5_list) if pm2_5_list else None,
                    "carbon_monoxide": sum(co_list) / len(co_list) if co_list else None,
                    "pm10": sum(pm10_list) / len(pm10_list) if pm10_list else None,
                    "nitrogen_dioxide": sum(no2_list) / len(no2_list) if no2_list else None,
                }
            })
        except Exception as e:
            # Log transformation error for each city
            logging.warning(f"Error transforming air quality data for {entry.get('city', 'unknown')}: {e}")

    # Push transformed air quality data to XCom
    ti.xcom_push(key="transformed_air_quality", value=transformed_air_quality)
    logging.info("Air quality data successfully transformed")
