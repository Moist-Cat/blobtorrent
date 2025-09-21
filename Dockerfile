FROM python:3.12-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    iproute2 \
    net-tools \
    iputils-ping \
    dnsutils \
    vim \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for data and logs
RUN mkdir /out/ /log/

# Copy original data
COPY ./out/ /out/

# Entrypoint script
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["--help"]
