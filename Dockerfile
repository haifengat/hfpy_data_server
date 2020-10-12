# 基础库中带配置用的csv文件
FROM python:3.6.12-slim

COPY server.py .
COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends wget

RUN wget http://data.haifengat.com/tradingtime.csv; \
    wget http://data.haifengat.com/calendar.csv; \
    wget http://data.haifengat.com/instrument.csv; \
    pip install --no-cache-dir -r ./requirements.txt;

ENTRYPOINT ["python", "server.py"]
