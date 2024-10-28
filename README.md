![](./src/assets/header.png)

# HR Pro Data Management System

Welcome to the **HR Pro Data Management System** repository! This project, developed by the G7 Data Engineering, is a comprehensive data engineering solution for **HR Pro**, a prominent human resources company. The system is designed to organize and analyze vast amounts of HR data efficiently, integrating various data sources such as job applications, payroll records, and employee surveys.


---

## Project Overview
The goal of this project is to implement a robust and scalable data management system tailored to the needs of HR Pro. Our solution facilitates the efficient collection, transformation, and storage of data, leveraging advanced data engineering tools and techniques. The key objectives include:

- **Designing an ETL process** to unify and process diverse data sources.
- **Organizing and storing data** in both MongoDB and SQL databases.
- **Deploying the solution** within a Dockerized environment to ensure scalability, ease of maintenance, and portability.
- **User Interface for Data Exploration**: Integrating a Streamlit application to provide an interactive, visual dashboard for HR Pro to analyze key metrics.
- **API for Data Access**: Implementing a FastAPI application to expose key data endpoints, allowing for smooth integration with external systems and applications.

## Key Features

- **Data Integration**: Seamlessly integrates multiple data sources, including structured and unstructured data.
- **ETL Pipeline**: Extracts, transforms, and loads data into MongoDB and SQL databases.
- **Scalability**: Uses Docker and Docker Compose for a fully containerized deployment.
- **Real-Time Processing**: Utilizes Apache Kafka for efficient real-time data ingestion and streaming.
- **Data Storage**: Stores data in MongoDB for unstructured records and in a SQL data warehouse for structured information.
- **Documentation and Usability**: A thoroughly documented and user-friendly system, ensuring HR Pro can manage and analyze their HR data effortlessly.
- **Interactive Dashboard**: Uses Streamlit to provide a dashboard for data visualization and HR insights.
- **API Access**: Provides a FastAPI-based API for secure and efficient access to data, enabling integration with other HR systems.

## Technologies Used

- **Apache Kafka**: For real-time data ingestion and streaming.
- **Docker & Docker Compose**: For containerization and orchestration of services.
- **MongoDB**: For flexible data storage of unstructured data.
- **SQL Database**: For structured data storage and analytics.
- **Streamlit**: To create an interactive, web-based dashboard for data exploration and visualization.
- **FastAPI**: For building a fast, efficient REST API to serve data endpoints.
- **GitHub**: Version control and collaboration.
- **Project Management**: Kanban board in Jira for task tracking and project management.

## Project Structure

```
├── config/
│   ├── db.py
│   ├── db.sql
├── docker/
│   ├── docker-compose.yml         # Defines the containerized services, including exporters
│   ├── Dockerfile
├── docs/
│   ├── api_documentation.md
│   ├── architecture_diagram.png
│   ├── data_model.md
├── monitoring/
│   ├── kafka/
│   │   ├── jmx_prometheus_javaagent.jar  # JMX Exporter agent for Kafka (needed for JVM applications)
│   │   └── kafka_jmx_config.yaml         # JMX Exporter configuration file for Kafka
├── src/
│   ├── extraction/
│   │   └── kafkaconsumer.py
│   ├── loading/
│   │   ├── mongodbloader.py
│   │   ├── redisloader.py
│   │   └── sqldbloader.py
│   ├── transformation/
│   │   ├── transformations.py
│   │   └── logger.py
│   └── main.py
├── tests/
│   └── test_cases.py
└── prometheus.yml                  # Prometheus configuration file for scraping the exporters
                # Prometheus configuration file for scraping the exporters

```

This structure allows HR Pro to not only process and store data efficiently but also to access and visualize it in meaningful ways through Streamlit and FastAPI.
### Description of Key Folders and Files:

- **docker/**: Contains Docker configurations.
  - `docker-compose.yml`: Defines services and configurations for Docker containers.
  - `dockerfile`: Instructions for building the main project image.

- **docs/**: Documentation for the project (currently empty).

- **src/**: Main source code folder.
  - **extraction/**: Holds code for data extraction.
    - `kafkaconsumer.py`: Likely handles data consumption from Kafka.
  - **loading/**: Contains code for loading data into storage.
    - `mongodbloader.py`: Manages data loading into MongoDB.
    - `sqldbloader.py`: Manages data loading into SQL databases.
  - **transformation/**: Contains code for data transformation.
    - `main.py`: Likely the main script for transforming data.

- **tests/**: Contains unit tests for each ETL stage.
  - `test_extraction.py`: Tests for the extraction process.
  - `test_loading.py`: Tests for the loading process.
  - `test_transformation.py`: Tests for the transformation process.

- **Environment and Metadata Files:**
  - `.env`: Stores environment variables.
  - `.gitignore`: Specifies files and directories Git should ignore.
  - `README.md`: Project description and setup instructions.
  - `requirements.txt`: Lists project dependencies.
  - `src.zip`: A zipped version of the source code. 

This structure organizes your ETL pipeline efficiently by separating extraction, transformation, and loading stages while including Docker and test files for containerization and testing.

Here’s the updated **Getting Started** section with steps for setting up Streamlit and FastAPI applications:

---

## Getting Started

To get a copy of this project up and running on your local machine, follow these instructions.

### Prerequisites

Ensure you have Docker and Docker Compose installed on your machine.

1. **Clone this repository:**

   ```bash
   git clone https://github.com/DataTechSolutions/HRPro-Data-Management-System.git
   cd HRPro-Data-Management-System
   ```

2. **Create and configure the environment variables** by copying the `.env.example` file:

   ```bash
   cp .env.example .env
   ```

3. **Update `.env`** with your own configuration details, such as database credentials and API keys.

### Installation

1. **Start Docker Containers**: Run the following command to start all services, including the ETL pipeline, Streamlit, and FastAPI applications, using Docker Compose.

   ```bash
   docker-compose up -d
   ```

2. **Initialize Databases**: Once services are up, initialize MongoDB and SQL database schema by running the setup scripts.

   ```bash
   # Commands to initialize databases, if applicable
   ```

3. **Run the ETL Pipeline**: Start the ETL pipeline to begin processing and ingesting data.

   ```bash
   # Commands to initiate ETL process
   ```

4. **Access the Streamlit Dashboard**: After starting the containers, access the interactive Streamlit dashboard by opening [http://localhost:8501](http://localhost:8501) in your browser.

5. **Access the FastAPI Application**: The FastAPI application, providing API endpoints for data access, will be accessible at [http://localhost:8000](http://localhost:8000). Visit [http://localhost:8000/docs](http://localhost:8000/docs) to view and interact with the API documentation.

### Usage

- **Data Ingestion**: Data is ingested in real-time via Kafka, processed in the ETL pipeline, and stored in MongoDB and SQL databases.
- **Querying Data**: Use MongoDB queries for unstructured data and SQL queries for structured data analysis.
- **Interactive Dashboard**: Use the Streamlit dashboard to visualize and explore HR metrics and insights.
- **Data API Access**: Use FastAPI endpoints to retrieve data programmatically, allowing for integration with other HR systems.
- **Scaling**: The system is designed to be easily scalable, utilizing Docker containers for flexible deployments.

### Examples

For example usage, refer to the `docs/examples` directory, which includes sample queries, API requests, and instructions for using the Streamlit dashboard.



For integrating **Prometheus exporters** into your project to monitor Kafka, Redis, and MongoDB:

### 1. **Selecting Files from Each Exporter Repository**

   - **Kafka Exporter (prometheus/jmx_exporter)**:
     - Download the latest **`jmx_prometheus_javaagent.jar`** file from the [Releases](https://github.com/prometheus/jmx_exporter/releases) section. This is the core agent that enables Prometheus to collect JMX metrics from Kafka.
     - Create or adapt a **JMX exporter configuration YAML** file. The repository usually includes example configurations you can use as a starting point.
   
   - **Redis Exporter (oliver006/redis_exporter)**:
     - Download the **binary executable** for the Redis Exporter or set it up as a Docker container. The binary can be found in the [Releases](https://github.com/oliver006/redis_exporter/releases) section.
     - No additional configuration files are necessary unless you want to specify custom Redis metrics. 

   - **MongoDB Exporter (percona/mongodb_exporter)**:
     - Use the **binary executable** or Docker image available in the [Releases](https://github.com/percona/mongodb_exporter/releases). This provides essential MongoDB metrics out of the box.
     - MongoDB Exporter supports some optional configurations, but for basic monitoring, the default setup should suffice.



### 3. **Updating the `docker-compose.yml` for Exporters**

   - **Add services for each exporter** in your `docker-compose.yml` file to ensure Prometheus can access them:
   
   ```yaml
   version: '3'
   services:
     kafka:
       image: confluentinc/cp-kafka
       environment:
         KAFKA_JMX_OPTS: "-javaagent:/monitoring/kafka/jmx_prometheus_javaagent.jar=7071:/monitoring/kafka/kafka_jmx_config.yaml"
       volumes:
         - ./monitoring/kafka:/monitoring/kafka

     redis:
       image: redis:latest
     redis_exporter:
       image: oliver006/redis_exporter
       ports:
         - "9121:9121"
       depends_on:
         - redis

     mongodb:
       image: mongo:latest
     mongodb_exporter:
       image: percona/mongodb_exporter
       ports:
         - "9216:9216"
       environment:
         MONGODB_URI: "mongodb://mongodb:27017"
       depends_on:
         - mongodb

     prometheus:
       image: prom/prometheus
       volumes:
         - ./prometheus.yml:/etc/prometheus/prometheus.yml
       ports:
         - "9090:9090"
   ```

   - **Configure Prometheus to scrape metrics** by adding targets for each exporter in `prometheus.yml`:

   ```yaml
   scrape_configs:
     - job_name: 'kafka'
       static_configs:
         - targets: ['kafka:7071']

     - job_name: 'redis'
       static_configs:
         - targets: ['redis_exporter:9121']

     - job_name: 'mongodb'
       static_configs:
         - targets: ['mongodb_exporter:9216']
   ```

### 4. **How It Will Work Together**

- When the containers are up and running, **Prometheus** will scrape metrics from each exporter:
- **Kafka JMX Exporter** will expose Kafka metrics on port 7071.
- **Redis Exporter** will expose Redis metrics on port 9121.
- **MongoDB Exporter** will expose MongoDB metrics on port 9216.
- **Prometheus** will aggregate these metrics, allowing you to monitor Kafka, Redis, and MongoDB through a unified dashboard.
- Optionally, you can use **Grafana** to visualize these metrics by connecting it to Prometheus for more flexible dashboards and alerts.

This setup provides you with a scalable monitoring structure where each exporter runs as a containerized service, accessible and configurable through Prometheus. Let me know if you'd like further help on any configuration specifics!

## Contributing

We welcome contributions to this project! If you'd like to contribute:

1. **Fork the repository.**
2. **Create a new branch** (`git checkout -b feature-branch`).
3. **Commit your changes** (`git commit -m 'Add feature'`).
4. **Push to the branch** (`git push origin feature-branch`).
5. **Create a pull request.**

--- 

This setup ensures all components, including the ETL, Streamlit dashboard, and FastAPI, are fully operational for development and exploration.
