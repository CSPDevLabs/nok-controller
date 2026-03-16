# Dockerfile
FROM python:3.12-slim-bookworm

WORKDIR /app

# Copy pyproject.toml and install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# Copy the controller script
COPY controller.py .

# Expose the port for the HTTP service
EXPOSE 8080

# Command to run the controller
CMD ["python", "controller.py"]