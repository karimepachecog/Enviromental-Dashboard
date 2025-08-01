from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Import the extraction functions
from extract import extraction_news, extraction_weather, extraction_air_quality
from raw_load import raw_data_load
from transform import transform
from aggregated_load import load

# ----------------- DAG DEFINITION ---------------------
dag = DAG(
    'ENVIRONMENTAL_DATA_POLLUTION_MONITOR',
    description='Environmental data pipeline for Mexican cities - monitors air pollution, weather, and news',
    start_date=datetime(2025, 7, 20),
    schedule_interval='@daily',  # Run daily to get fresh environmental data
    catchup=False,
    tags=['environmental', 'pollution', 'mexico', 'monitoring']
)

# ----------------- TASK DEFINITIONS ---------------------

# Extract air pollution news from NewsAPI
extract_news_task = PythonOperator(
    task_id="extract_news_task",
    python_callable=extraction_news,
    provide_context=True,
    dag=dag
)

# Extract weather forecast data from Open-Meteo
extract_weather_task = PythonOperator(
    task_id="extract_weather_task",
    python_callable=extraction_weather,
    provide_context=True,
    dag=dag
)

# Extract air quality data from Open-Meteo Air Quality API
extract_air_quality_task = PythonOperator(
    task_id="extract_air_quality_task",
    python_callable=extraction_air_quality,
    provide_context=True,
    dag=dag
)

# Load raw extracted data into MongoDB
raw_load_task = PythonOperator(
    task_id="raw_load_task",
    python_callable=raw_data_load,
    provide_context=True,
    dag=dag
)

# Transform and clean all extracted data
transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=transform,
    provide_context=True,
    dag=dag
)

# Load final transformed data into MongoDB
aggregated_load_task = PythonOperator(
    task_id="aggregated_load_task",
    python_callable=load,
    provide_context=True,
    dag=dag
)


# ----------------- TASK DEPENDENCIES ---------------------

# Parallel extraction of all data sources
[extract_news_task, extract_weather_task, extract_air_quality_task] >> raw_load_task

# Sequential processing after raw data is loaded
raw_load_task >> transform_task >> aggregated_load_task


