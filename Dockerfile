# Use a python base image with Playwright support
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install browsers for playwright
RUN playwright install chromium

# Copy application code
COPY . .

# Expose the dashboard port
EXPOSE 8080

# Use supervisord to run multiple processes
CMD ["supervisord", "-c", "supervisord.conf"]
