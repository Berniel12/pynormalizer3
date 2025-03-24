# For more information, see https://docs.apify.com/actors/development/base-docker-images
FROM apify/actor-python:3.10

# Copy all files from the directory where Dockerfile is located to the Docker image
COPY . ./

# Install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Run the Actor
CMD ["python3", "main.py"]
