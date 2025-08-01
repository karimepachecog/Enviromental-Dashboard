import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import folium
from streamlit_folium import st_folium
from datetime import datetime, timedelta
import requests
import logging

# Import your extraction functions to avoid code duplication
import sys
import os
# Add your ETL directory to path if needed
# sys.path.append('path/to/your/etl/directory')

# Configure page
st.set_page_config(
    page_title="Environmental Data Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
    .stMetric > label {
        font-size: 0.9rem !important;
    }
    .news-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def connect_to_mongodb():
    """Connect to MongoDB and fetch the latest batch data"""
    try:
        # Update connection string as needed
        client = MongoClient("mongodb://host.docker.internal:27017/")  # Use your connection string
        db = client["environmental_data_orchestration"]
        collection = db["pollution_monitoring"]
        
        # Get the most recent batch
        latest_batch = collection.find().sort("_id", -1).limit(1)
        latest_data = list(latest_batch)
        
        client.close()
        
        if latest_data:
            return latest_data[0]
        else:
            return None
    except Exception as e:
        st.error(f"Error connecting to MongoDB: {e}")
        return None

@st.cache_data(ttl=300)
def get_all_batches():
    """Get last 5 batches for historical analysis"""
    try:
        client = MongoClient("mongodb://host.docker.internal:27017/")
        db = client["environmental_data_orchestration"]
        collection = db["pollution_monitoring"]
        
        # Get last 5 batches
        batches = list(collection.find().sort("_id", -1).limit(5))
        client.close()
        
        return batches
    except Exception as e:
        st.error(f"Error fetching historical batches: {e}")
        return []

def color_pm25(valor):
    """Define colors by pollution level (same as your original function)"""
    if valor is None:
        return 'gray'
    elif valor <= 12:
        return '#00ff00'  # Green - Good
    elif valor <= 35.4:
        return '#ffff00'  # Yellow - Moderate
    elif valor <= 55.4:
        return '#ff8c00'  # Orange - Unhealthy for Sensitive
    elif valor <= 150.4:
        return '#ff0000'  # Red - Unhealthy
    else:
        return '#800080'  # Purple - Very Unhealthy

def get_air_quality_level(valor):
    """Get air quality description"""
    if valor is None:
        return 'No Data'
    elif valor <= 12:
        return 'Good'
    elif valor <= 35.4:
        return 'Moderate'
    elif valor <= 55.4:
        return 'Unhealthy for Sensitive Groups'
    elif valor <= 150.4:
        return 'Unhealthy'
    else:
        return 'Very Unhealthy'

def create_pollution_map_from_mongodb(mongodb_data):
    """Create pollution map using data from your MongoDB structure"""
    # Create map centered on Mexico
    m = folium.Map(location=[23.6345, -102.5528], zoom_start=5)
    
    if not mongodb_data or 'air_quality_data' not in mongodb_data:
        return m
    
    air_quality_cities = mongodb_data['air_quality_data'].get('cities', [])
    
    # Your cities coordinates for mapping
    CITIES_COORDS = {
        "Ciudad de México": {"lat": 19.4326, "lon": -99.1332},
        "Guadalajara": {"lat": 20.6597, "lon": -103.3496},
        "Monterrey": {"lat": 25.6866, "lon": -100.3161},
        "Puebla": {"lat": 19.0414, "lon": -98.2063},
        "Toluca": {"lat": 19.2826, "lon": -99.6557},
        "Tijuana": {"lat": 32.5149, "lon": -117.0382},
        "León": {"lat": 21.1222, "lon": -101.6826},
        "Juárez": {"lat": 31.6904, "lon": -106.4245},
        "Torreón": {"lat": 25.5439, "lon": -103.4187},
        "Querétaro": {"lat": 20.5888, "lon": -100.3899},
        "San Luis Potosí": {"lat": 22.1565, "lon": -100.9855},
        "Mérida": {"lat": 20.9674, "lon": -89.5926},
        "Mexicali": {"lat": 32.6245, "lon": -115.4523},
        "Aguascalientes": {"lat": 21.8853, "lon": -102.2916},
        "Acapulco": {"lat": 16.8531, "lon": -99.8237},
        "Hermosillo": {"lat": 29.0729, "lon": -110.9559},
        "Chihuahua": {"lat": 28.6353, "lon": -106.0889},
        "Saltillo": {"lat": 25.4382, "lon": -100.9737},
        "Cancún": {"lat": 21.1619, "lon": -86.8515},
        "Morelia": {"lat": 19.7060, "lon": -101.1950}
    }
    
    # Add markers for each city using MongoDB data
    for city_data in air_quality_cities:
        city_name = city_data.get('city', 'Unknown')
        avg_pm25 = city_data.get('averages', {}).get('pm2_5')
        
        if city_name in CITIES_COORDS:
            coords = CITIES_COORDS[city_name]
            
            pm25_text = f"{avg_pm25:.1f} μg/m³" if avg_pm25 is not None else "No data"
            level = get_air_quality_level(avg_pm25)
            color = color_pm25(avg_pm25)
            
            # Additional pollutants from your data
            avg_pm10 = city_data.get('averages', {}).get('pm10')
            avg_co = city_data.get('averages', {}).get('carbon_monoxide')
            avg_no2 = city_data.get('averages', {}).get('nitrogen_dioxide')
            
            popup_text = f"""
            <b>{city_name}</b><br>
            <b>Air Quality: {level}</b><br>
            PM2.5: {pm25_text}<br>
            PM10: {f"{avg_pm10:.1f} μg/m³" if avg_pm10 else "N/A"}<br>
            CO: {f"{avg_co:.1f} mg/m³" if avg_co else "N/A"}<br>
            NO₂: {f"{avg_no2:.1f} μg/m³" if avg_no2 else "N/A"}
            """
            
            folium.CircleMarker(
                location=[coords['lat'], coords['lon']],
                radius=12,
                color='black',
                weight=2,
                fill=True,
                fillColor=color,
                fillOpacity=0.8,
                popup=folium.Popup(popup_text, max_width=250),
                tooltip=f"{city_name}: {pm25_text}"
            ).add_to(m)
    
    # Add legend
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 220px; height: 140px; 
                background-color: rgba(255, 255, 255, 0.95); 
                border: 2px solid #333333; 
                border-radius: 8px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.3);
                z-index: 9999; 
                font-size: 13px; 
                font-family: Arial, sans-serif;
                padding: 12px;
                color: #333333;">
    <b style="color: #1f1f1f; font-size: 14px;">PM2.5 Levels (μg/m³)</b><br><br>
    <div style="margin: 3px 0;"><span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background-color: #00ff00; border: 1px solid #333; margin-right: 8px;"></span><b>Good (0-12)</b></div>
    <div style="margin: 3px 0;"><span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background-color: #ffff00; border: 1px solid #333; margin-right: 8px;"></span><b>Moderate (12-35)</b></div>
    <div style="margin: 3px 0;"><span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background-color: #ff8c00; border: 1px solid #333; margin-right: 8px;"></span><b>Unhealthy for Sensitive (35-55)</b></div>
    <div style="margin: 3px 0;"><span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background-color: #ff0000; border: 1px solid #333; margin-right: 8px;"></span><b>Unhealthy (55-150)</b></div>
    <div style="margin: 3px 0;"><span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background-color: #800080; border: 1px solid #333; margin-right: 8px;"></span><b>Very Unhealthy (150+)</b></div>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m

def create_temperature_trends_chart(mongodb_data):
    """Create temperature trends chart using your MongoDB data structure"""
    if not mongodb_data or 'weather_data' not in mongodb_data:
        return go.Figure().add_annotation(text="No weather data available", 
                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
    
    weather_cities = mongodb_data['weather_data'].get('cities', [])
    
    if not weather_cities:
        return go.Figure().add_annotation(text="No weather data available", 
                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
    
    fig = go.Figure()
    
    # Major cities to display
    major_cities = ['Ciudad de México', 'Guadalajara', 'Monterrey', 'Puebla', 'Tijuana']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    for i, city_name in enumerate(major_cities):
        city_data = next((city for city in weather_cities if city.get('city') == city_name), None)
        if city_data and 'hourly_data' in city_data:
            hourly_data = city_data['hourly_data']
            
            # Extract times and temperatures
            times = [entry.get('datetime', '') for entry in hourly_data]
            temperatures = [entry.get('temperature_2m') for entry in hourly_data if entry.get('temperature_2m') is not None]
            
            if temperatures:
                fig.add_trace(go.Scatter(
                    x=times[:len(temperatures)],
                    y=temperatures,
                    mode='lines+markers',
                    name=city_name,
                    line=dict(color=colors[i % len(colors)], width=2),
                    marker=dict(size=4)
                ))
    
    fig.update_layout(
        title="Temperature Trends - Last ETL Run",
        xaxis_title="Time",
        yaxis_title="Temperature (°C)",
        hovermode='x unified',
        height=400,
        showlegend=True,
        template="plotly_white"
    )
    
    return fig

def create_air_quality_comparison_chart(mongodb_data):
    """Create air quality comparison chart across cities"""
    if not mongodb_data or 'air_quality_data' not in mongodb_data:
        return go.Figure().add_annotation(text="No air quality data available", 
                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
    
    air_quality_cities = mongodb_data['air_quality_data'].get('cities', [])
    
    if not air_quality_cities:
        return go.Figure().add_annotation(text="No air quality data available", 
                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
    
    # Prepare data for plotting
    cities = []
    pm25_values = []
    pm10_values = []
    co_values = []
    no2_values = []
    
    for city_data in air_quality_cities:
        city_name = city_data.get('city', 'Unknown')
        averages = city_data.get('averages', {})
        
        cities.append(city_name)
        pm25_values.append(averages.get('pm2_5', 0) or 0)
        pm10_values.append(averages.get('pm10', 0) or 0)
        co_values.append(averages.get('carbon_monoxide', 0) or 0)
        no2_values.append(averages.get('nitrogen_dioxide', 0) or 0)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='PM2.5',
        x=cities,
        y=pm25_values,
        marker_color='red',
        opacity=0.8
    ))
    
    fig.add_trace(go.Bar(
        name='PM10',
        x=cities,
        y=pm10_values,
        marker_color='orange',
        opacity=0.8
    ))
    
    fig.update_layout(
        title="Air Quality Comparison Across Cities (Average Values)",
        xaxis_title="Cities",
        yaxis_title="Concentration (μg/m³)",
        barmode='group',
        height=400,
        template="plotly_white",
        xaxis_tickangle=-45
    )
    
    return fig

def main():
    # Header
    st.markdown('<h1 class="main-header">🌍 Environmental Data Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.header("Dashboard Controls")
    refresh_button = st.sidebar.button("🔄 Refresh Data", type="primary")
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("Auto-refresh every 5 minutes")
    if auto_refresh:
        st.rerun()
    
    # Data loading section
    st.sidebar.markdown("---")
    st.sidebar.subheader("Data Source")
    data_source = st.sidebar.radio(
        "Choose data source:",
        ["MongoDB (ETL Pipeline)", "Live API Data"],
        help="MongoDB shows your processed ETL data, Live API fetches current data"
    )
    
    # Load data based on selection
    if data_source == "MongoDB (ETL Pipeline)":
        mongodb_data = connect_to_mongodb()
        use_mongodb = True
    else:
        mongodb_data = None
        use_mongodb = False
    
    # Main content layout
    if use_mongodb and mongodb_data:
        # MongoDB Data Dashboard
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.header("🗺️ Air Quality Map (ETL Data)")
            pollution_map = create_pollution_map_from_mongodb(mongodb_data)
            st_folium(pollution_map, width=700, height=500)
            
            # Air quality summary from MongoDB
            st.subheader("Air Quality Summary")
            if 'air_quality_data' in mongodb_data:
                air_cities = mongodb_data['air_quality_data'].get('cities', [])
                valid_pm25 = [city.get('averages', {}).get('pm2_5') for city in air_cities 
                             if city.get('averages', {}).get('pm2_5') is not None]
                
                if valid_pm25:
                    col_a, col_b, col_c, col_d = st.columns(4)
                    with col_a:
                        st.metric("Cities Monitored", len(air_cities))
                    with col_b:
                        st.metric("Average PM2.5", f"{sum(valid_pm25)/len(valid_pm25):.1f} μg/m³")
                    with col_c:
                        st.metric("Highest PM2.5", f"{max(valid_pm25):.1f} μg/m³")
                    with col_d:
                        good_air_cities = len([pm25 for pm25 in valid_pm25 if pm25 <= 12])
                        st.metric("Cities with Good Air", good_air_cities)
        
        with col2:
            st.header("📊 ETL Batch Overview")
            batch_info = mongodb_data.get('data_summary', {})
            st.success("✅ Connected to MongoDB")
            
            # ETL metrics
            with st.container():
                st.markdown('<div class="metric-container">', unsafe_allow_html=True)
                st.metric("News Articles", batch_info.get('news_articles_count', 0))
                st.metric("Weather Records", batch_info.get('weather_cities_count', 0))
                st.metric("Air Quality Records", batch_info.get('air_quality_cities_count', 0))
                st.metric("Total Records", batch_info.get('total_records', 0))
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Batch information
            st.text(f"Batch ID: {mongodb_data.get('batch_id', 'N/A')}")
            extraction_time = mongodb_data.get('air_quality_data', {}).get('extraction_timestamp', 'N/A')
            st.text(f"Last Updated: {extraction_time}")
            
            # News headlines
            if 'news_data' in mongodb_data and mongodb_data['news_data'].get('articles'):
                st.subheader("📰 Latest News")
                news_articles = mongodb_data['news_data']['articles'][:3]
                for article in news_articles:
                    with st.expander(f"📄 {article.get('title', 'No title')[:40]}..."):
                        st.write(f"**Source:** {article.get('source', 'Unknown')}")
                        st.write(f"**Published:** {article.get('published_at', 'Unknown')}")
                        if article.get('description'):
                            st.write(f"**Description:** {article['description'][:150]}...")
                        if article.get('url'):
                            st.write(f"[Read more]({article['url']})")
        
        # Charts section
        st.markdown("---")
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.header("🌡️ Temperature Trends")
            temp_chart = create_temperature_trends_chart(mongodb_data)
            st.plotly_chart(temp_chart, use_container_width=True)
        
        with chart_col2:
            st.header("🏭 Air Quality Comparison") 
            air_chart = create_air_quality_comparison_chart(mongodb_data)
            st.plotly_chart(air_chart, use_container_width=True)
            
    else:
        # Fallback to live data or no data message
        if use_mongodb:
            st.error("❌ Could not connect to MongoDB or no data available")
            st.info("Please check your MongoDB connection and ensure your ETL pipeline has run.")
        else:
            st.info("🔄 Live API mode - This would fetch current data from APIs")
            st.info("Switch to 'MongoDB (ETL Pipeline)' in the sidebar to view your processed data.")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "Environmental Data Dashboard | Data from your ETL Pipeline | "
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        "</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()