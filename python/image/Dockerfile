FROM python:3.7-slim

WORKDIR /
COPY requirements.txt /
COPY zk.py /
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        python-dev \
        libsasl2-dev \
	cyrus-sasl \
        make \
        gcc \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get remove -y --purge make gcc build-essential python-dev libsasl2-dev  \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
