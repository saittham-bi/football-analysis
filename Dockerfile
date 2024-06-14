FROM apache/airflow:2.9.0

USER root

# Compulsory to switch parameter
# ENV PIP_USER=false

WORKDIR /app

COPY requirements.txt app/requirements.txt

# RUN apt-get install -y gosu
RUN pip3 install --upgrade pip 
RUN pip3 install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r app/requirements.txt
# Add a tag to the image
LABEL version="1.0"

# Ensure the directory exists
RUN mkdir -p data/docker-entrypoint-initdb.d

# Copy the SQL script from the data directory to the initialization directory
COPY data/init-db.sh data/docker-entrypoint-initdb.d/init-db.sh

# Make the script executable
RUN chmod +x data/docker-entrypoint-initdb.d/init-db.sh

# Set user for db migration
USER ${_AIRFLOW_WWW_USER_USERNAME}

# Initialize the Airflow database
RUN airflow db migrate

# Set the custom command to start Airflow
CMD ["airflow", "webserver"]