# set base image (host OS)
FROM python:3.8 as base
FROM base as builder

ENV workflow=${workflow}
ENV client_name=${client_name}

# set the working directory in the container
WORKDIR /usr/src/app

# copy the content of the local src directory to the working directory
COPY src/ .
COPY requirements.txt . 

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# command to run on container start
CMD [ "python", "./DataLoader.py" ]
