# set base image (host OS)
FROM python:3.8

# set the working directory in the container
WORKDIR /usr/src/app

# install dependencies
RUN pip install cassandra-driver

# copy the content of the local src directory to the working directory
COPY src/ .
COPY csv/ ./csv

# command to run on container start
CMD [ "python", "./data_loader.py" ]
