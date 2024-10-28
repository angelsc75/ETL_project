# HR Pro Data Management System

Welcome to the **HR Pro Data Management System** repository! This project, developed by the G7 Data Engineering, is a comprehensive data engineering solution for **HR Pro**, a prominent human resources company. The system is designed to organize and analyze vast amounts of HR data efficiently, integrating various data sources such as job applications, payroll records, and employee surveys.

Here's the expanded text including **Streamlit** and **FastAPI** components:

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
- **Project Management**: Kanban board in Trello (or similar) for task tracking and project management.

## Project Structure

```
├── data-extraction/               # Scripts and modules for data extraction from various sources
├── data-transformation/           # ETL logic for data transformation
├── docker-compose.yml             # Docker Compose file for orchestrating services
├── src/                           # Source code for the ETL pipeline
│   ├── kafka/                     # Kafka configuration and setup
│   ├── mongodb/                   # MongoDB database configuration
│   ├── sql-db/                    # SQL database configuration and schema
│   ├── etl/                       # Core ETL processes and data ingestion scripts
│   ├── streamlit_app/             # Streamlit application for interactive data visualization
│   └── fastapi_app/               # FastAPI application for providing data access endpoints
├── docs/                          # Documentation and guides for setup, usage, and contribution
└── README.md                      # Project overview and usage guide
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

## Contributing

We welcome contributions to this project! If you'd like to contribute:

1. **Fork the repository.**
2. **Create a new branch** (`git checkout -b feature-branch`).
3. **Commit your changes** (`git commit -m 'Add feature'`).
4. **Push to the branch** (`git push origin feature-branch`).
5. **Create a pull request.**

--- 

This setup ensures all components, including the ETL, Streamlit dashboard, and FastAPI, are fully operational for development and exploration.
