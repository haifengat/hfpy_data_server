# 基础库中带配置用的csv文件
FROM haifengat/ctp_real_md
# 合约信息
COPY instrument.json /home/
RUN pip install -r ./requirements.txt

