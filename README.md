# data_service

### 项目介绍
为at平台提供数据服务，基于docker  haifengat/ctp_real_md。

### 数据
|说明|文件名|来源|
|-|-|-|
|交易日历|calendar.csv|haifengat/ctp_real_md:/home|
|品种时间|tradingtime.csv|haifengat/ctp_real_md:/home|
|合约信息|instrument.csv|本项目|

### 数据更新
> 上面提到的3个数据文件更新方式
> * 自行更新
> * 及时push最新的 docker

### 环境变量
* server_port
  * 服务端口
* redis_addr
  * 实时行情使用的redis连接地址
* pg_config
  * 分钟 postgres 数据库
* min_csv_gz_path[可选]
  * 分钟csv文件路径,每日数据导入用.

### Dockerfile
```dockerfile
# 基础库中带配置用的csv文件
FROM haifengat/ctp_real_md
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
```

### build
```bash
# XXX通过github git push触发 hub.docker自动build耗时太久(因修改pip指向国内)
docker build -t haifengat/hfpy_data_server . && docker push haifengat/hfpy_data_server && 
docker tag haifengat/hfpy_data_server haifengat/hfpy_data_server:`date +%Y%m%d` && docker push haifengat/hfpy_data_server:`date +%Y%m%d`
```

### 启动
```bash
docker-compose --compatibility up -d
```

### docker-compose.yml
```yml
version: "3.7"
services:
    # 启动: docker-compose --compatibility up -d
    hfpy_data_server:
        image: haifengat/hfpy_data_server
        container_name: hfpy_data_server
        restart: always
        ports:
            - 15555:5055
        environment:
            - TZ=Asia/Shanghai
            # 数据服务端口
            - server_port=5055
            # redis 实时行情
            - redis_addr=redis_real:6379
            # postgres 历史K线数据
            - pg_config=postgresql://postgres:123456@pg_min:5432/postgres
            # 分钟数据路径
            - min_csv_gz_path=/home/min_csv_gz
        volumes: 
            - /mnt/future_min_csv_gz:/home/min_csv_gz
        depends_on:
            - pg_min
            - redis_real
        deploy:
            resources:
                limits:
                    cpus: '1'
                    memory: 2G
                reservations:
                    memory: 200M

    # 遇到the database system is starting up错误, 配置数据文件下的postgres.conf,hot_standby=on
    pg_min:
        image: postgres:12
        container_name: pg_min
        restart: always
        environment:
            TZ: "Asia/Shanghai"
            POSTGRES_PASSWORD: "123456"
        ports:
            - "25432:5432"
        volumes:
            - /mnt/pg_future/pgdata:/var/lib/postgresql/data

    real_md:
        image: haifengat/ctp_real_md
        container_name: real_md
        restart: always
        environment:
            - "TZ=Asia/Shanghai"
            - "redis_addr=redis_tick:6379"
            - "front_trade=tcp://180.168.146.187:10101"
            - "front_quote=tcp://180.168.146.187:10111"
            - "login_info=008105/1/9999/simnow_client_test/0000000000000000"
        deploy:
            resources:
                limits:
                    cpus: '1'
                    memory: 2G
                reservations:
                    memory: 200M
        depends_on:
            - redis_real

    redis_real:
        image: redis:6.0.5
        container_name: redis_real
        restart: always
        environment:
            - TZ=Asia/Shanghai
      
```