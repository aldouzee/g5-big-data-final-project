"""
Kafka Producer Template for Streaming Data Dashboard
STUDENT PROJECT: Big Data Streaming Data Producer

This is a template for students to build a Kafka producer that generates and sends
streaming data to Kafka for consumption by the dashboard.

DO NOT MODIFY THE TEMPLATE STRUCTURE - IMPLEMENT THE TODO SECTIONS
"""

import argparse
import json
import time
import random
import math
import socket
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import os
import requests


class StreamingDataProducer:
    """Kafka producer that streams live weather observations from WeatherAPI."""

    def __init__(self, bootstrap_servers: str, topic: str, api_key: str = None, city: str = "Manila"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key or os.getenv("WEATHER_API_KEY")
        self.city = city
        self.weather_url = "https://api.weatherapi.com/v1/current.json"
        self.producer = None

        self.producer_config = {
            'bootstrap_servers': self._get_bootstrap_list(bootstrap_servers),
            'acks': 'all',
            'retries': 3,
            'linger_ms': 10,
        }

        self._connect_to_broker()

    def _get_bootstrap_list(self, bootstrap_servers: str):
        """Normalize a comma-separated broker list into Kafka client format."""
        if not bootstrap_servers:
            return ['localhost:9092']
        return [server.strip() for server in bootstrap_servers.split(',') if server.strip()]

    def _check_broker_reachable(self, host: str, port: int) -> bool:
        """Return True when the target host/port accepts connections."""
        try:
            with socket.create_connection((host, port), timeout=3):
                return True
        except (socket.timeout, OSError, ConnectionError):
            return False

    def _connect_to_broker(self):
        """Create the Kafka producer only when the broker is available."""
        brokers = self.producer_config['bootstrap_servers']
        reachable = []

        for broker in brokers:
            try:
                host, port = broker.split(':', 1)
                port = int(port)
            except ValueError:
                host, port = broker, 9092
            if self._check_broker_reachable(host, port):
                reachable.append(broker)

        if not reachable:
            print(f"ERROR: No Kafka brokers available at {', '.join(brokers)}")
            print("Start Kafka first, then rerun this producer.")
            print("Example: .\\bin\\windows\\kafka-server-start.bat .\\config\\server.properties")
            self.producer = None
            return

        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {', '.join(reachable)} on topic {self.topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {', '.join(reachable)}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def fetch_current_weather(self, metric_type: str = "temperature") -> Dict[str, Any]:
        """Fetch and normalize a single weather reading from WeatherAPI."""
        if not self.api_key:
            raise ValueError("WeatherAPI key is missing. Set WEATHER_API_KEY or pass --api-key.")

        params = {
            "key": self.api_key,
            "q": self.city,
            "aqi": "no",
        }
        response = requests.get(self.weather_url, params=params, timeout=15)
        response.raise_for_status()
        payload = response.json()

        location = payload.get("location", {})
        current = payload.get("current", {})

        metric_map = {
            "temperature": (current.get("temp_c"), "°C", "temperature"),
            "humidity": (current.get("humidity"), "%", "humidity"),
            "pressure": (current.get("pressure_mb"), "hPa", "pressure"),
            "wind_speed": (current.get("wind_kph"), "km/h", "wind_speed"),
        }

        value, unit, default_metric = metric_map.get(metric_type, metric_map["temperature"])
        if value is None:
            raise ValueError(f"Weather metric '{metric_type}' not available for {self.city}")

        timestamp = current.get("last_updated")
        if timestamp:
            try:
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
                ts = dt.isoformat() + "Z"
            except ValueError:
                ts = datetime.utcnow().isoformat() + "Z"
        else:
            ts = datetime.utcnow().isoformat() + "Z"

        data = {
            "timestamp": ts,
            "value": float(value),
            "metric_type": default_metric,
            "sensor_id": f"weatherapi-{location.get('name', self.city).lower().replace(' ', '-')}-{default_metric}",
            "location": location.get("name", self.city),
            "region": location.get("region", ""),
            "country": location.get("country", ""),
            "unit": unit,
            "condition": current.get("condition", {}).get("text", "unknown"),
        }
        return data

    def generate_sample_data(self) -> Dict[str, Any]:
        """Return the latest live weather observation or a safe fallback when the API is unavailable."""
        try:
            return self.fetch_current_weather()
        except Exception as exc:
            print(f"Weather API fetch failed, using fallback sample data: {exc}")
            current_time = datetime.utcnow()
            return {
                "timestamp": current_time.isoformat() + "Z",
                "value": 28.5,
                "metric_type": "temperature",
                "sensor_id": "weatherapi-manila-temperature",
                "location": self.city,
                "region": "",
                "country": "",
                "unit": "°C",
                "condition": "fallback",
            }

    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """Serialize the weather data payload as JSON for Kafka."""
        try:
            return json.dumps(data).encode('utf-8')
        except Exception as exc:
            print(f"Serialization error: {exc}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """Send a weather record to the Kafka topic."""
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False

        serialized_data = self.serialize_data(data)
        if not serialized_data:
            print("ERROR: Serialization failed")
            return False

        try:
            future = self.producer.send(self.topic, value=serialized_data)
            future.get(timeout=10)
            print(f"Message sent successfully - Topic: {self.topic}, Data: {data}")
            return True
        except KafkaError as exc:
            print(f"Kafka send error: {exc}")
            return False
        except Exception as exc:
            print(f"Unexpected error during send: {exc}")
            return False

    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):
        """Continuously pull live weather data and publish it to Kafka."""
        print(f"Starting weather producer: {messages_per_second} msg/sec, duration: {duration or 'infinite'}")

        start_time = time.time()
        message_count = 0

        try:
            while True:
                if duration and (time.time() - start_time) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break

                data = self.generate_sample_data()
                if self.send_message(data):
                    message_count += 1
                    if message_count % 10 == 0:
                        print(f"Sent {message_count} weather messages...")

                sleep_time = 1.0 / messages_per_second if messages_per_second > 0 else 1.0
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as exc:
            print(f"Streaming error: {exc}")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")

    def close(self):
        """Flush and close Kafka producer resources."""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as exc:
                print(f"Error closing Kafka producer: {exc}")


def parse_arguments():
    """Parse command-line arguments for the weather streaming producer."""
    parser = argparse.ArgumentParser(description='Weather data Kafka producer using WeatherAPI')

    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', help='Kafka bootstrap servers (comma-separated list allowed)')
    parser.add_argument('--topic', type=str, default='streaming-data', help='Kafka topic to produce to')
    parser.add_argument('--rate', type=float, default=0.1, help='Messages per second (default: 0.1 for 10-second intervals)')
    parser.add_argument('--duration', type=int, default=None, help='Run duration in seconds (default: infinite)')
    parser.add_argument('--api-key', type=str, default=None, help='WeatherAPI key. If omitted, WEATHER_API_KEY env value is used.')
    parser.add_argument('--city', type=str, default='Manila', help='City or location name to query from WeatherAPI')
    parser.add_argument('--metric', choices=['temperature', 'humidity', 'pressure', 'wind_speed'], default='temperature', help='Weather metric to stream')

    return parser.parse_args()


def main():
    """Run the weather producer with CLI configuration."""
    print("=" * 60)
    print("WEATHERAPI STREAMING PRODUCER")
    print("Streaming live weather data to Kafka")
    print("=" * 60)

    args = parse_arguments()
    print(f"Kafka broker target: {args.bootstrap_servers}")
    print(f"Kafka topic: {args.topic}")

    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        api_key=args.api_key,
        city=args.city,
    )

    if producer.producer is None:
        print("Kafka connection failed. Start Kafka broker before running the producer.")
        return

    try:
        producer.produce_stream(
            messages_per_second=args.rate,
            duration=args.duration,
        )
    except Exception as exc:
        print(f"Main execution error: {exc}")
    finally:
        print("Producer execution completed")


if __name__ == "__main__":
    main()