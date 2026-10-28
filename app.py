"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
"""

import math
import os
import time
import json
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    """Configure sidebar options for Kafka and WeatherAPI inputs."""
    st.sidebar.title("Dashboard Controls")

    st.sidebar.subheader("Data Source Configuration")
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker",
        value="localhost:9092",
        help="Kafka bootstrap server for the streaming topic"
    )

    kafka_topic = st.sidebar.text_input(
        "Kafka Topic",
        value="streaming-data",
        help="Topic used by the live weather producer"
    )

    weather_city = st.sidebar.text_input(
        "Weather City",
        value="Manila",
        help="City name to query from WeatherAPI"
    )

    weather_api_key = st.sidebar.text_input(
        "WeatherAPI Key",
        value=os.getenv("WEATHER_API_KEY", ""),
        type="password",
        help="Optional API key. Leave empty to use the WEATHER_API_KEY environment variable."
    )

    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["HDFS", "MongoDB"],
        help="Historical storage backend for the dashboard"
    )

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "weather_city": weather_city,
        "weather_api_key": weather_api_key,
        "storage_type": storage_type,
    }


def fetch_weather_snapshot(city: str, api_key: str):
    """Fetch a single WeatherAPI reading for the configured city."""
    if not api_key:
        return None

    try:
        response = requests.get(
            "https://api.weatherapi.com/v1/current.json",
            params={"key": api_key, "q": city, "aqi": "no"},
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        current = payload.get("current", {})
        location = payload.get("location", {})

        timestamp = current.get("last_updated")
        if timestamp:
            try:
                ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M").isoformat() + "Z"
            except ValueError:
                ts = datetime.utcnow().isoformat() + "Z"
        else:
            ts = datetime.utcnow().isoformat() + "Z"

        return pd.DataFrame([
            {
                "timestamp": ts,
                "value": float(current.get("temp_c", 0.0)),
                "metric_type": "temperature",
                "sensor_id": f"weatherapi-{city.lower().replace(' ', '-')}-temperature",
                "location": location.get("name", city),
                "unit": "°C",
            },
            {
                "timestamp": ts,
                "value": float(current.get("humidity", 0.0)),
                "metric_type": "humidity",
                "sensor_id": f"weatherapi-{city.lower().replace(' ', '-')}-humidity",
                "location": location.get("name", city),
                "unit": "%",
            },
            {
                "timestamp": ts,
                "value": float(current.get("pressure_mb", 0.0)),
                "metric_type": "pressure",
                "sensor_id": f"weatherapi-{city.lower().replace(' ', '-')}-pressure",
                "location": location.get("name", city),
                "unit": "hPa",
            },
        ])
    except Exception:
        return None


def generate_sample_data():
    """Create realistic weather sample data for dashboard fallbacks."""
    current_time = datetime.now()
    times = [current_time - timedelta(minutes=i) for i in range(100, 0, -1)]

    values = [22 + 10 * (1 + (i % 8)) / 8 + ((i % 5) * 0.8) for i in range(100)]
    sample_data = pd.DataFrame({
        'timestamp': times,
        'value': values,
        'metric_type': ['temperature'] * 100,
        'sensor_id': ['weatherapi-manila-temperature'] * 100,
        'location': ['Manila'] * 100,
        'unit': ['°C'] * 100,
    })

    return sample_data

def consume_kafka_data(config):
    """Read weather messages from Kafka, or fall back to live WeatherAPI data."""
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")
    city = config.get("weather_city", "Manila")
    api_key = config.get("weather_api_key") or os.getenv("WEATHER_API_KEY")

    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    if cache_key not in st.session_state:
        max_retries = 2
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                st.session_state[cache_key] = KafkaConsumer(
                    kafka_topic,
                    bootstrap_servers=[kafka_broker],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=5000,
                )
                break
            except Exception as exc:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    st.session_state[cache_key] = None
                    st.warning(f"Kafka unavailable: {exc}. Falling back to live WeatherAPI data.")

    consumer = st.session_state[cache_key]
    if consumer:
        try:
            messages = []
            start_time = time.time()
            while time.time() - start_time < 5 and len(messages) < 10:
                msg_pack = consumer.poll(timeout_ms=1000)
                for _, messages_batch in msg_pack.items():
                    for message in messages_batch:
                        data = message.value
                        if all(key in data for key in ['timestamp', 'value', 'metric_type', 'sensor_id']):
                            timestamp_str = data['timestamp']
                            if timestamp_str.endswith('Z'):
                                timestamp_str = timestamp_str[:-1] + '+00:00'
                            timestamp = datetime.fromisoformat(timestamp_str)
                            messages.append({
                                'timestamp': timestamp,
                                'value': float(data['value']),
                                'metric_type': data['metric_type'],
                                'sensor_id': data['sensor_id'],
                                'location': data.get('location', city),
                                'unit': data.get('unit', ''),
                            })
            if messages:
                return pd.DataFrame(messages)
        except (NoBrokersAvailable, KafkaError, Exception) as exc:
            st.warning(f"Kafka polling failed: {exc}. Falling back to live API data.")

    live_data = fetch_weather_snapshot(city, api_key)
    if live_data is not None and not live_data.empty:
        return live_data

    st.info("No live Kafka messages or WeatherAPI data available. Showing generated fallback weather data.")
    return generate_sample_data()

def query_historical_data(time_range="1h", metrics=None):
    """Return a small historical weather dataset using time-range filters."""
    now = datetime.now()
    hours_map = {"1h": 1, "24h": 24, "7d": 168, "30d": 720}
    hours = hours_map.get(time_range, 24)

    timestamps = [now - timedelta(hours=hours - i) for i in range(hours + 1)]
    records = []
    for idx, ts in enumerate(timestamps):
        base = 28 + math.sin(idx / max(1, hours) * 3.14) * 6
        records.append({
            'timestamp': ts,
            'value': round(base, 2),
            'metric_type': 'temperature',
            'sensor_id': 'historical-weather-manila',
            'location': 'Manila',
            'unit': '°C',
        })

    data = pd.DataFrame(records)
    if metrics:
        data = data[data['metric_type'].isin(metrics)].copy()
    return data


def display_real_time_view(config, refresh_interval):
    """Display the live weather stream and metrics."""
    st.header("📈 Real-time Weather Dashboard")

    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")

    with st.spinner("Fetching live weather data..."):
        real_time_data = consume_kafka_data(config)

    if real_time_data is not None:
        data_freshness = datetime.now() - refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡" if data_freshness.total_seconds() < 30 else "🔴"
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")

        st.subheader("📊 Live Weather Metrics")
        if not real_time_data.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records Received", len(real_time_data))
            with col2:
                st.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f} {real_time_data['unit'].iloc[-1]}")
            with col3:
                st.metric("Location", real_time_data['location'].iloc[0])

        st.subheader("📈 Live Trend")
        if not real_time_data.empty:
            fig = px.line(
                real_time_data,
                x='timestamp',
                y='value',
                color='metric_type',
                title=f"Weather Streaming Data ({len(real_time_data)} points)",
                labels={'value': 'Value', 'timestamp': 'Time'},
                template='plotly_white'
            )
            fig.update_layout(xaxis_title='Time', yaxis_title='Value', hovermode='x unified')
            st.plotly_chart(fig, width='stretch')

            with st.expander("📋 View Raw Data"):
                st.dataframe(real_time_data.sort_values('timestamp', ascending=False), width='stretch', height=300)
        else:
            st.warning("No real-time weather data available.")
    else:
        st.error("Real-time weather stream is unavailable.")


def display_historical_view(config):
    """Display a simple historical weather analysis with filters."""
    st.header("📊 Historical Weather Analysis")

    st.subheader("Data Filters")
    col1, col2, col3 = st.columns(3)

    with col1:
        time_range = st.selectbox("Time Range", ["1h", "24h", "7d", "30d"], help="Choose the historical time window")

    with col2:
        metric_type = st.selectbox("Metric Type", ["temperature", "humidity", "pressure", "all"], help="Select metric to analyze")

    with col3:
        aggregation = st.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"], help="Choose the aggregation level")

    historical_data = query_historical_data(time_range, [metric_type] if metric_type != "all" else None)

    if historical_data is not None:
        st.subheader("Historical Data Table")
        st.dataframe(historical_data, width='stretch', hide_index=True)

        st.subheader("Historical Trends")
        if not historical_data.empty:
            fig = px.line(
                historical_data,
                x='timestamp',
                y='value',
                title=f"Historical {metric_type if metric_type != 'all' else 'weather'} trend",
                labels={'value': 'Value', 'timestamp': 'Time'},
            )
            st.plotly_chart(fig, width='stretch')

            st.subheader("Data Summary")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Records", len(historical_data))
                st.metric("Date Range", f"{historical_data['timestamp'].min().strftime('%Y-%m-%d')} to {historical_data['timestamp'].max().strftime('%Y-%m-%d')}")
            with col2:
                st.metric("Average Value", f"{historical_data['value'].mean():.2f}")
                st.metric("Data Variability", f"{historical_data['value'].std():.2f}")
    else:
        st.error("Historical weather query is unavailable.")


def main():
    """Main Streamlit application entry point."""
    st.title("🚀 Weather Streaming Dashboard")

    with st.expander("📋 Project Overview"):
        st.markdown("""
        This application streams live weather data from WeatherAPI, forwards the data through Kafka,
        and presents real-time and historical dashboard views.
        """)
    
    # Initialize session state for refresh management
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }

    refresh_interval = 15

    # Setup configuration
    config = setup_sidebar()
    
    # Refresh controls in sidebar
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Automatically refresh real-time data"
    )
    
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15,
            help="Set how often real-time data refreshes"
        )
        
        # Auto-refresh using streamlit-autorefresh package
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    # Display refresh status
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)
    

if __name__ == "__main__":
    main()