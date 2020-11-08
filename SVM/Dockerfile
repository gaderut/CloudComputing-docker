FROM python:3.7-slim as base
FROM base as builder
RUN mkdir /install
WORKDIR /install
COPY requirement.txt /requirement.txt
RUN pip install --prefix=/install -r /requirement.txt

# COPY requirement.txt /
# RUN pip install --no-cache-dir -r requirement.txt
FROM base
COPY --from=builder /install /usr/local
COPY . /app
WORKDIR /app
# COPY employeedata.csv /app
# RUN apt-get update -y
# RUN apt-get install -y python-pip python-dev build-essential
