FROM python:3.7-slim as base
FROM base as builder
RUN mkdir /install
WORKDIR /install
COPY requirement.txt /requirement.txt
RUN pip install --prefix=/install -r /requirement.txt

FROM base
COPY --from=builder /install /usr/local
COPY . /app
WORKDIR /app
ENV workflow=${workflow}
ENV client_name=${client_name}

CMD [ "python3", "model.py" ]

