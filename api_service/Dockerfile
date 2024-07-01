FROM python:3.8-slim

# Set environment variables
# ENV variables specific to FastAPI can be set here if needed

WORKDIR /app

# Copy the wait-for-it script
COPY wait-for-it.sh /usr/wait-for-it.sh
RUN chmod +x /usr/wait-for-it.sh

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the data directory
COPY data /app/data

# Copy the rest of the application
COPY . .

# Expose the port Uvicorn runs on, default is 8000
EXPOSE 5000

# Run the FastAPI application using Uvicorn
# Adjust the command below if your app file or variable names differ
CMD ["./usr/wait-for-it.sh", "db:7687", "--", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000", "--reload"]