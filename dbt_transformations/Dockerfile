FROM python:3.8-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir dbt requests pandas python-dotenv sqlalchemy snowflake-sqlalchemy

# Run DBT command
CMD ["dbt", "run"]