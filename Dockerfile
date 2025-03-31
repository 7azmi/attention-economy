# Use a Python base image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the project dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install playwright

RUN playwright install --with-deps firefox # Copy the rest of the project files into the container
COPY . .

# Set the command to run when the container starts
CMD ["python", "main.py"]