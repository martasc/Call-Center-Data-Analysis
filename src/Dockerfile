# Use an official Python image (Windows-compatible)
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy only necessary files (exclude __pycache__)
COPY requirements.txt .
COPY main.py .
COPY main_paradela.py .
COPY utils.py .
COPY config.py .
# Add other necessary files (e.g., data_filtering.py, metricas.py)

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the default command to run main.py
CMD ["python", "main.py"]