# Use Python 3.11 official image as base
# This provides a complete Python 3.11 environment on Debian Linux
FROM python:3.11-slim

# Set metadata labels for the image
LABEL maintainer="learning-docker"
LABEL description="Python 3.11 environment for learning Docker"

# Set working directory inside the container
# All subsequent commands will run from this directory
WORKDIR /app

# Copy requirements.txt first (before copying source code)
# This allows Docker to cache the pip install step when only source code changes
COPY requirements.txt .

# Upgrade pip and install Python dependencies
# --no-cache-dir prevents caching packages, keeping image smaller
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the source code into the container
# The . means "current directory" (which is /app due to WORKDIR)
COPY . .

# Make the Python script executable
RUN chmod +x hello.py

# Create a non-root user for security best practices
# This prevents running the container as root
RUN groupadd -g 1001 appuser && \
    useradd -r -u 1001 -g appuser appuser && \
    chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

# Set default command to keep container running
# tail -f /dev/null is a common trick to keep container alive
# You can override this in docker-compose.yml or when running
CMD ["tail", "-f", "/dev/null"]
