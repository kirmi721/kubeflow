FROM python:3.7-slim
WORKDIR /
COPY libsasl2.so.3 /lib64
RUN cd /lib64
RUN ln -s libsasl2.so.3 libsasl2.so.2
RUN cd /
COPY requirements.txt /
COPY pyhiveclient.py /usr/local/lib/python3.7
COPY zk.py /usr/local/lib/python3.7
COPY zkhiveconnection.txt /usr/local/lib/python3.7

RUN apt-get update && \
    apt-get install -y \
        build-essential \
        python-dev \
        libsasl2-dev \
        make \
        gcc \
    && pip install --no-cache-dir -r requirements.txt
