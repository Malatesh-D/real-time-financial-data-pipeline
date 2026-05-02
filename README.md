# Real-Time Financial Data Streaming Pipeline 📈

An end-to-end Data Engineering pipeline that streams, processes, and visualizes live stock market data in real-time.

## 🏗️ Architecture
* **Data Source:** Python Producer fetching live market data.
* **Message Queue:** Apache Kafka (Zookeeper).
* **Stream Processing:** Apache Spark (PySpark) calculating moving averages.
* **Database:** PostgreSQL.
* **Visualization & Alerting:** Grafana.
* **Infrastructure:** Docker & Docker Compose.

