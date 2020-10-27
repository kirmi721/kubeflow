FROM registry.cn-shenzhen.aliyuncs.com/kirmi/kubeflow:python-1.0.0
WORKDIR /
COPY requirements.txt /
COPY pyhiveclient.py /usr/local/lib/python3.7
COPY zk.py /usr/local/lib/python3.7
COPY zkhiveconnection.txt /usr/local/lib/python3.7
RUN  pip install --no-cache-dir -r requirements.txt 
