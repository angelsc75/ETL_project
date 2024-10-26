# HR Pro Data Management System

Welcome to the **HR Pro Data Management System** repository! This project, developed by the G7 Data Engineering, is a comprehensive data engineering solution for **HR Pro**, a prominent human resources company. The system is designed to organize and analyze vast amounts of HR data efficiently, integrating various data sources such as job applications, payroll records, and employee surveys.

## Project Overview

The goal of this project is to implement a robust and scalable data management system tailored to the needs of HR Pro. Our solution facilitates the efficient collection, transformation, and storage of data, leveraging advanced data engineering tools and techniques. The key objectives include:

- Designing an ETL process to unify and process diverse data sources.
- Organizing and storing data in both MongoDB and SQL databases.
- Deploying the solution within a Dockerized environment to ensure scalability, ease of maintenance, and portability.

## Key Features

- **Data Integration**: Seamlessly integrates multiple data sources, including structured and unstructured data.
- **ETL Pipeline**: Extracts, transforms, and loads data into MongoDB and SQL databases.
- **Scalability**: Uses Docker and Docker Compose for a fully containerized deployment.
- **Real-Time Processing**: Utilizes Apache Kafka for efficient real-time data ingestion and streaming.
- **Data Storage**: Stores data in MongoDB for unstructured records and in a SQL data warehouse for structured information.
- **Documentation and Usability**: A thoroughly documented and user-friendly system, ensuring HR Pro can manage and analyze their HR data effortlessly.

## Technologies Used

- **Apache Kafka**: For real-time data ingestion and streaming.
- **Redis**: As an in-memory cache layer, holding frequently accessed or real-time data to reduce load on primary databases.
- **Docker & Docker Compose**: For containerization and orchestration of services.
- **MongoDB**: For flexible data storage of unstructured data.
- **SQL Database**: For structured data storage and analytics.
- **GitHub**: Version control and collaboration.
- **Project Management**: Kanban board in Jira for task tracking and project management.



```
DATA-ENGINEERING-EDUCATIONAL-PROJECT/
│
├── controllers/
│   └── __pycache__/
│
├── docker/
│   ├── docker-compose.yml
│   └── dockerfile
│
├── docs/
│
├── src/
│   ├── extraction/
│   │   ├── __init__.py
│   │   └── kafkaconsumer.py
│   │
│   ├── loading/
│   │   ├── __init__.py
│   │   ├── mongodbloader.py
│   │   └── sqldbloader.py
│   │
│   ├── transformation/
│   │   ├── __init__.py
│   │   └── main.py
│
│   ├── tests/
│   │   ├── __init__.py
│   │   ├── test_extraction.py
│   │   ├── test_loading.py
│   │   └── test_transformation.py
│
├── .env
├── .gitignore
├── README.md
├── requirements.txt
└── src.zip
```

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
