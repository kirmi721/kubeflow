FROM centos:7
WORKDIR /
RUN yum -y install gcc automake autoconf libtool make wget zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel libffi-devel gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64 cyrus-sasl-devel cyrus-sasl-gssapi cyrus-sasl-md5 cyrus-sasl-plain
RUN wget https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tgz
RUN tar -zxvf Python-3.7.0.tgz
RUN ./Python-3.7.0/configure --prefix=/usr/local/python3 
RUN cd Python-3.7.0
RUN make 
RUN make install
RUN cd /
RUN ln -s /usr/local/python3/bin/python3.7 /usr/bin/python3
RUN ln -s /usr/local/python3/bin/pip3.7 /usr/bin/pip3
RUN pip3 install --upgrade pip
COPY requirements.txt /
COPY pyhiveclient.py /usr/local/python3/lib/python3.7
COPY zk.py /usr/local/python3/lib/python3.7
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm -rf Python-3.7.0 
RUN rm -rf Python-3.7.0.tgz