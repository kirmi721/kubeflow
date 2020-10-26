# FROM amazonlinux:latest
# RUN yum update -y && \
#     yum install -y python3 python3-devel
# COPY requirements.txt ./
# RUN pip3 install --no-cache-dir -r requirements.txt

FROM python:3.7-slim
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
ADD ./zk.py /app
ADD ./zkhiveconnection.txt /app

