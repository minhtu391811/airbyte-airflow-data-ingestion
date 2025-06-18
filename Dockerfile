# Use the official Airflow image as the base
FROM apache/airflow:2.8.1

# Switch to airflow User
USER airflow

# Install the Docker, HTTP, and Airbyte providers for Airflow
RUN pip install apache-airflow-providers-docker \
  apache-airflow-providers-http \
  apache-airflow-providers-airbyte \
  apache-airflow-providers-postgres

# Switch back to root user
USER root