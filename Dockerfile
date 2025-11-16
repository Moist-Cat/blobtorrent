FROM python:3.12-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    iproute2 \
    net-tools \
    iputils-ping \
    dnsutils \
    vim \
    curl

RUN apt-get update && apt-get install python3-dev -y
RUN apt-get install gcc -y

# Create app directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt


# Adapter
COPY adapter/requirements.txt requirements.adapter.txt
RUN pip install -r requirements.adapter.txt

RUN mkdir /log /out

COPY entrypoint.sh /
