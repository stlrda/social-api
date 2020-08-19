# Python FastAPI for Serving Social (Demographic, Employment, Health) Data
FROM python:3.8-slim-buster

LABEL maintainer='Branson Fox <bransonfox@umsl.edu>'

# Environmental Variables

## For Connecting to DB
ENV DB_HOST='localhost'
ENV DB_PORT=5432
ENV DB_USER='postgres'
ENV DB_PASS='postgres'
ENV DB_NAME='uw211dashboard'

# Install Python Dependencies
RUN mkdir -p /social/api
WORKDIR /social/api

COPY requirements.txt /social/api/requirements.txt
RUN pip install -r requirements.txt

# Copy Python (API) Files
COPY *.py /social/api/

EXPOSE 3002

# Start Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3002"]