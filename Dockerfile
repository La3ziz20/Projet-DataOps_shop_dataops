FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project into the container
COPY . .

# Default command runs the Prefect daily flow once against Day 1 sample orders
CMD ["python", "orchestration/flow.py", "--date", "2026-10-05", "--orders", "data/raw/orders_day1.csv"]
