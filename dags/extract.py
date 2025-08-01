# Import requests library to make HTTP API calls
import requests
# Import datetime to handle date operations and timestamps
from datetime import datetime, timedelta
import logging

# API key for NewsAPI service
API_KEY = '58ea38158e28449c96832333bdbd4241'

# List of Mexican cities with their coordinates for weather and air quality APIs
CITIES = [
    {"nombre": "Ciudad de México", "lat": 19.4326, "lon": -99.1332},
    {"nombre": "Guadalajara", "lat": 20.6597, "lon": -103.3496},
    {"nombre": "Monterrey", "lat": 25.6866, "lon": -100.3161},
    {"nombre": "Puebla", "lat": 19.0414, "lon": -98.2063},
    {"nombre": "Toluca", "lat": 19.2826, "lon": -99.6557},
    {"nombre": "Tijuana", "lat": 32.5149, "lon": -117.0382},
    {"nombre": "León", "lat": 21.1222, "lon": -101.6826},
    {"nombre": "Juárez", "lat": 31.6904, "lon": -106.4245},
    {"nombre": "Torreón", "lat": 25.5439, "lon": -103.4187},
    {"nombre": "Querétaro", "lat": 20.5888, "lon": -100.3899},
    {"nombre": "San Luis Potosí", "lat": 22.1565, "lon": -100.9855},
    {"nombre": "Mérida", "lat": 20.9674, "lon": -89.5926},
    {"nombre": "Mexicali", "lat": 32.6245, "lon": -115.4523},
    {"nombre": "Aguascalientes", "lat": 21.8853, "lon": -102.2916},
    {"nombre": "Acapulco", "lat": 16.8531, "lon": -99.8237},
    {"nombre": "Hermosillo", "lat": 29.0729, "lon": -110.9559},
    {"nombre": "Chihuahua", "lat": 28.6353, "lon": -106.0889},
    {"nombre": "Saltillo", "lat": 25.4382, "lon": -100.9737},
    {"nombre": "Cancún", "lat": 21.1619, "lon": -86.8515},
    {"nombre": "Morelia", "lat": 19.7060, "lon": -101.1950}
]

# Function to extract air pollution news from NewsAPI
def extraction_news(ti=None):
    """
    Extract air pollution related news for Mexico using NewsAPI
    Args:
        ti: Task instance for XCom communication
    """
    # API endpoint with query parameters for air pollution news in Spanish
    url = (f'https://newsapi.org/v2/everything?q=contaminación%aire%México&language=es&sortBy=publishedAt&pageSize=10&apiKey={API_KEY}')
   
    try:
        # Make GET request to NewsAPI
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
        articles = data.get("articles", [])  # Extract 'articles' list

        if ti:  # If an Airflow TaskInstance is provided
            ti.xcom_push(key="extracted_news_data", value=articles)  # Push articles to XCom
        else:
            return articles  # Otherwise, return articles directly

    except Exception as e:
        print(f"[extract_news_task] Error extracting news: {e}")
        if ti:
            ti.xcom_push(key="extracted_news_data", value=[])  # Push empty list on failure
        else:
            return []

# Function to extract weather forecast data from Open-Meteo API
def extraction_weather(ti):
    """
    Extract weather forecast data for multiple Mexican cities
    Args:
        ti: Task instance for XCom communication
    """
    weather_data = []  # List to store data from all cities

    for city in CITIES:
        name = city['nombre']
        lat = city['lat']
        lon = city['lon']

        # Build API request URL for weather forecast
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}"
            f"&hourly=temperature_2m,relative_humidity_2m,precipitation,cloud_cover"
            f"&timezone=auto"
        )

        try:
            # Make request to weather API
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            data['city'] = name  # Attach city name to the data
            weather_data.append(data)

        except Exception as e:
            logging.error(f"[extract_weather_task] Error extracting weather for {name}: {e}")

    # Log the result and push to Airflow XCom
    logging.info(f"[extract_weather_task] Extracted weather data for {len(weather_data)} cities.")
    ti.xcom_push(key="extracted_weather_data", value=weather_data)

# Function to extract air quality data from Open-Meteo Air Quality API
def extraction_air_quality(ti):
    """
    Extract air quality data for Mexican cities
    Args:
        ti: Task instance for XCom communication
    """
    all_air_data = []  # List to hold air quality data

    for city in CITIES:
        name = city['nombre']
        lat = city['lat']
        lon = city['lon']

        # Construct API request URL for air quality
        url = (
            f"https://air-quality-api.open-meteo.com/v1/air-quality?"
            f"latitude={lat}&longitude={lon}"
            f"&hourly=pm2_5,carbon_monoxide,pm10,nitrogen_dioxide"
            f"&timezone=auto"
        )

        try:
            # Make the request to air quality API
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Store both city name and air quality data
            all_air_data.append({
                "city": name,
                "data": data
            })

        except Exception as e:
            logging.error(f"[extract_air_quality_task] Error extracting air quality for {name}: {e}")

    # Log total and push to XCom
    logging.info(f"[extract_air_quality_task] Extracted air quality for {len(all_air_data)} cities.")
    ti.xcom_push(key="extracted_air_quality_data", value=all_air_data)
