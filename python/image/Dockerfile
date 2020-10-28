FROM centos:7
WORKDIR /

RUN yum -y install wget zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel libffi-devel
RUN wget https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tgz
RUN tar -zxvf Python-3.7.0.tgz
RUN mkdir /usr/local/python3
RUN cd Python-3.7.0
RUN ./configure --prefix=/usr/local/python3 && make && make install
RUN ln -s /usr/local/python3/bin/python3.7 /usr/bin/python3
RUN ln -s /usr/local/python3/bin/pip3.7 /usr/bin/pip3

COPY requirements.txt /
COPY pyhiveclient.py /usr/local/lib/python3.7
COPY zk.py /usr/local/lib/python3.7
COPY zkhiveconnection.txt /usr/local/lib/python3.7
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
