# 基础库中带配置用的csv文件
FROM haifengat/ctp_real_md:slim

ENV PROJECT hfpy_data_server
ENV DOWNLOAD_URL "https://github.com/haifengat/${PROJECT}/archive/master.zip"

WORKDIR /${PROJECT}
ADD "${DOWNLOAD_URL}" .
RUN unzip master.zip; \
    rm master.zip -rf; \
    pip install --no-cache-dir -r ./${PROJECT}-master/requirements.txt
ENTRYPOINT ["python", "./${PROJECT}-master/server.py"]

