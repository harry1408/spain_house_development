# Use lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Prevent Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (optional but recommended)
RUN apt-get update && apt-get install -y build-essential

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy project files
COPY . .

# Expose port (Flask default 5000 / FastAPI 8000)
EXPOSE 8000

# Run app (adjust as per your framework)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]