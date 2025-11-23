FROM python:3.9-alpine

WORKDIR /app

# Install system dependencies first to leverage caching
RUN apk add --no-cache build-base python3-dev linux-headers

# Copy requirements.txt to the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

CMD ["python3", "-m", "WebStreamer"]
