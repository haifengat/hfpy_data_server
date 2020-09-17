# 基础库中带配置用的csv文件
FROM haifengat/ctp_real_md

ENV PROJECT=hfpy_data_server
ENV DOWNLOAD_URL "https://github.com/haifengat/${PROJECT}/archive/master.zip"

WORKDIR /${PROJECT}
ADD "${DOWNLOAD_URL}" .
RUN unzip master.zip; \
    rm master.zip -rf; \
    pip install --no-cache-dir -r ./${PROJECT}-master/requirements.txt

RUN echo "#!/bin/sh \npython ${PROJECT}-master/server.py" > run.sh
ENTRYPOINT ["/bin/bash", "run.sh"]
