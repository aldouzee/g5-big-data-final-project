# 🚀 Streaming Data Dashboard: Project Guide

This project requires building a big data streaming dashboard using **Kafka** and **Streamlit**, featuring separate real-time and historical data views.

## 🎯 Architecture & Components

A **dual-pipeline** architecture separates live streaming from long-term storage and analysis.

| Pipeline | Flow | Output |
| :--- | :--- | :--- |
| **Real-time** | Kafka $\rightarrow$ Streamlit (Live Consumer) | `📈 Real-time Streaming View` |
| **Historical** | Kafka $\rightarrow$ **HDFS OR MongoDB** $\rightarrow$ Streamlit (Query) | `📊 Historical Data View` |

### Mandatory Components
* Kafka Producer/Consumer.
* **HDFS or MongoDB** integration.
* Two-page Streamlit dashboard with charts.
* Robust error handling.

---

## 💻 Technical Implementation Tasks

### 1. Data Producer (`producer.py`)
Create a Kafka Producer that fetches real data from an **existing Application Programming Interface (API)** (e.g., a public weather API, stock market API, etc.).

**Required Data Schema Fields:**
* `timestamp` (ISO format)
* `value` (Numeric)
* `metric_type` (String)
* `sensor_id` (String)

### 2. Dashboard (`app.py`)
Implement the Streamlit logic:
* `consume_kafka_data()`: Real-time processing.
* `query_historical_data()`: Data retrieval from storage.
* Create interactive widgets (filters, time-range selector) for the Historical View.

### 3. Storage Integration
Implement data writing and querying for **ONE** of the following: **HDFS** or **MongoDB**.

---

## 🏃‍♂️ Setup & Execution

### Prerequisites
Python 3.8+, Docker Desktop with Docker Compose, and a WeatherAPI key.

### Setup
1. **Setup environment**
    - Download miniconda
    - Create your python environment
    ```bash
    conda create -n bigdata python=3.10.13
    ```
2.  **Clone Repo & Install:**
    ```bash
    git clone [REPO_URL]
    conda activate bigdata
    pip install -r requirements.txt
    ```
3. **Configure the WeatherAPI key:**
    - Get an API key from [WeatherAPI](https://www.weatherapi.com/).
    - Either set it in PowerShell:
    ```powershell
    $env:WEATHER_API_KEY = "your-weatherapi-key"
    ```
    - Or pass it directly to the producer with `--api-key`.

### Execution
Run each command from the project directory:

1. **Start Kafka with Docker:**
    ```powershell
    docker compose up -d
    ```
    The included [docker-compose.yml](docker-compose.yml) starts a single Kafka broker in KRaft mode and exposes it at `localhost:9092`.

2. **Verify Kafka is running:**
    ```powershell
    docker compose ps
    Test-NetConnection localhost -Port 9092
    ```
    `TcpTestSucceeded` must be `True` before starting the producer. You can also verify the topic with:
    ```powershell
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
    ```

3. **Start the producer in a new PowerShell window:**
    ```bash
    python producer.py
    ```

    The producer uses `localhost:9092` and the `streaming-data` topic by default. To provide the API key directly:
    ```powershell
    python producer.py --api-key "your-weatherapi-key"
    ```

4. **Launch the dashboard in another PowerShell window:**
    ```powershell
    streamlit run app.py
    ```

5. **Stop Kafka when finished:**
    ```powershell
    docker compose down
    ```

    To remove Kafka's stored Docker volume and reset its data, use `docker compose down -v`.

### Troubleshooting

* If the producer reports `No Kafka brokers available at localhost:9092`, run `docker compose up -d` and verify the port with `Test-NetConnection localhost -Port 9092`.
* If the producer reports repeated `KafkaConnectionError: socket disconnected` errors, ensure the producer does not force an old Kafka API version. The current producer lets `kafka-python` negotiate the broker version automatically.

---

## 📦 Deliverables
Submit the following files:
* `app.py`
* `producer.py`
* `requirements.txt`
* `README.md`