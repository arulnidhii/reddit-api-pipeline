# Base image
FROM python:3.8

# Working directory
WORKDIR /app

# Install required libraries
RUN pip install praw azure-storage-blob  

# Copy your Python script
COPY newscript.py .

# Set the main script for execution
CMD ["python", "newscript.py"]
