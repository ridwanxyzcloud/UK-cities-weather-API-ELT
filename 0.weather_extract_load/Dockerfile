# python image
FROM python:3.8-slim

WORKDIR /app

# this copies the current directory contents into the container at /app
COPY . /app

# installing all requirment specifies and needed packages 
RUN pip install --no-cache-dir -r requirements

#environment variable
ENV PYTHONUNBUFFERED=1

# this runs the script 'app.py' when the docker container launches
CMD ["python", "app.py"]