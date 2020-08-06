# 基础库中带配置用的csv文件
FROM haifengat/ctp_realmd
# 合约信息
COPY instrument.csv /home/
COPY *.py /home/
COPY requirements.txt /home/
RUN pip install -r /home/requirements.txt
ENV pg_config postgresql://postgres:123456@pg_min:5432/postgres
ENV redis_addr redis_tick:6379
ENV min_csv_gz_path /home/min_csv_gz
ENV server_port 5055

ENTRYPOINT ["python", "/home/server.py"]

