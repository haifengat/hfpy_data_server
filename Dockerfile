# 基础库中带配置用的csv文件
FROM python:3.6.12-slim

ENV PROJECT=hfpy_data_server
ENV DOWNLOAD_URL "https://github.com/haifengat/${PROJECT}/archive/master.zip"

WORKDIR /
RUN set -ex; \
    apt-get update && apt-get install -y --no-install-recommends wget unzip; \
    wget -O master.zip "${DOWNLOAD_URL}"; \
    unzip master.zip; \
    rm master.zip -rf;

WORKDIR /${PROJECT}-master
RUN wget http://data.haifengat.com/tradingtime.csv; \
    wget http://data.haifengat.com/calendar.csv; \
    wget http://data.haifengat.com/instrument.csv; \
    pip install --no-cache-dir -r ./requirements.txt;

ENTRYPOINT ["python", "server.py"]
