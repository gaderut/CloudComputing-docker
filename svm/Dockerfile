FROM python:3
COPY . /app
WORKDIR /app
# COPY employeedata.csv /app
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential
RUN pip install --no-cache-dir -r requirement.txt
EXPOSE 8080
CMD  ["python","model.py"]