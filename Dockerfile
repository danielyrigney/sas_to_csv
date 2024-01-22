# In the main directory, run "docker build -t sas-to-csv ." to build the Docker image 
# Then, run "docker run -it -v /path/to/folder:/app/data sas-to-csv" with /

# Use an official Python runtime as a parent image
FROM python:3

# Set the working directory to /app
WORKDIR /

# Copy the script into the container at /
COPY convert_sas7bdat_to_csv.py /

# Install any needed packages specified in requirements.txt
RUN pip install pandas pathlib

# Run the Python script when the container launches
CMD ["python", "/convert_sas7bdat_to_csv.py"]