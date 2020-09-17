#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = '数据服务'
__author__ = 'HaiFeng'
__mtime__ = '20180911'

import zmq, redis
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import os, json, time, gzip, threading
from color_log import Logger


class Server(object):

    def __init__(self, port):
        self.log = Logger()
        
        self.df_time = pd.read_csv('./tradingtime.csv', converters={'OpenDate':str})
        g = self.df_time.groupby(by=['GroupId'])
        df_tmp = g['OpenDate'].max()
        self.df_time = self.df_time[self.df_time.apply(lambda x: x['OpenDate']==df_tmp[x['GroupId']], axis=1)]

        self.df_canlendar = pd.read_csv('./calendar.csv', converters={'day':str})
        self.df_canlendar = self.df_canlendar[self.df_canlendar['tra']][['day']]
        self.df_canlendar.rename(columns={'day':'_id'}, inplace=True)

        df_instrument = pd.read_csv('./instrument.csv', converters={'OPENDATE': str, 'EXPIREDATE': str})        
        # 品种信息
        # g = df_instrument[df_instrument['EXPIREDATE'] > time.strftime('%Y%m%d', time.localtime())].groupby(by='PRODUCTID')
        g = df_instrument.groupby(by='PRODUCTID')
        df = g.first()
        # productid归到列里
        self.df_productinfo = df.reset_index()
        self.df_productinfo.rename(columns={'PRODUCTID':'_id', 'PRICETICK': 'PriceTick', 'VOLUMEMULTIPLE': 'VolumeTuple', 'PRODUCTCLASS': 'ProductType', 'MAXLIMITORDERVOLUME': 'MAXLIMITORDERVOLUME', 'EXCHANGEID': 'ExchangeID'}, inplace=True)

        # 合约对应的品种
        # df = df_instrument[df_instrument['EXPIREDATE'] > time.strftime('%Y%m%d', time.localtime())][['INSTRUMENTID', 'PRODUCTID']]
        df = df_instrument[['INSTRUMENTID', 'PRODUCTID']]
        self.df_inst_proc = df.rename(columns={'INSTRUMENTID':'_id', 'PRODUCTID': 'ProductID'})

        self.pg: Engine = None
        pg_config = 'postgresql://postgres:123456@127.0.0.1:25432/postgres'
        if 'pg_config' in os.environ:
            pg_config = os.environ['pg_config']
        self.pg = create_engine(pg_config)
        self.log.info(f'connecting pg: {pg_config}')

        redis_addr, rds_port = '127.0.0.1', 16379
        if 'redis_addr' in os.environ:
            redis_addr = os.environ['redis_addr']
            if ':' in redis_addr:
                redis_addr, rds_port =  redis_addr.split(':')
        self.log.info(f'connecting redis: {redis_addr}:{rds_port}')
        pool = redis.ConnectionPool(host=redis_addr, port=rds_port, db=0, decode_responses=True)
        self.rds = redis.StrictRedis(connection_pool=pool)

        self.min_csv_gz_path = ''
        if 'min_csv_gz_path' in os.environ:
            self.min_csv_gz_path = os.environ['min_csv_gz_path']

        context = zmq.Context(1)
        self.server = context.socket(zmq.REP)
        self.server.bind('tcp://*:{}'.format(port))

    def min_csv_pg(self):
        """分钟数据从csv.gz到postgres
        """
        if len(self.min_csv_gz_path) == 0:
            return
        ret = self.pg.execute('select max("TradingDay" ) from future.future_min')
        max_pg_min = ret.fetchone()[0]
        self.log.info(f'current tradingday in pg: {max_pg_min}')
        min_files = [m.split('.')[0] for m in os.listdir(self.min_csv_gz_path)]
        if len(min_files) > 0:
            # 存在的数据入库
            exists_days = [d for d in min_files if d > max_pg_min]
            if len(exists_days) > 0:
                for day in exists_days:
                    self.min_2_pg(day)
                max_pg_min = max(exists_days)
        trading_days = list(self.df_canlendar['_id'])
        next_day = min([d for d in trading_days if d > max_pg_min])
        self.log.info(f'{next_day} waiting...')
        while True:
            if os.path.exists(os.path.join(self.min_csv_gz_path, f'{next_day}.csv.gz')):
                # 分钟数据入库
                self.min_2_pg(next_day)
                next_day = min([d for d in trading_days if d > next_day])
                self.log.info(f'{next_day} waiting...')
                continue
            time.sleep(60 * 10)

    def min_2_pg(self, day: str):
        """分钟csv.gz数据入库

        Args:
            day (str): 交易日
        """
        self.log.info(f'{day} starting...')
        with gzip.open(os.path.join(self.min_csv_gz_path, f'{day}.csv.gz')) as f_min:
            df:DataFrame = pd.read_csv(f_min, sep='\t', header=0)
            df.loc[:, 'TradingDay'] = day
            df.to_sql('future_min', schema='future', con=self.pg, index=False, if_exists='append')
            self.log.info(f'{day} finish.')

    def run(self):
        self.log.war('listen to port: {}'.format(self.server.LAST_ENDPOINT.decode()))
        while True:
            request = self.server.recv_json()  # .recv_string()
            self.log.info(request)
            # Min, Day, Real, Time, Product, TradeDate, InstrumentInfo, Instrumet888, Rate000
            rsp = self.read_data(request)
            if rsp == '':
                self.log.error('{} 未取得数据'.format(request))
            rsp = gzip.compress(rsp.encode(), 9)
            self.server.send(rsp)
            self.log.info('sent to client.')

    def read_data(self, req) -> str:
        """
            Min, Day, Real, Time, Product, TradeDate, InstrumentInfo, Instrumet888, Rate000
        """
        df: DataFrame = None
        sql:str = ''
        if req['Type'] in [0, 1]:  # Min, Day # 注意tradingday的大小写,为兼容旧版本要改为Tradingday
            sql = f"""select to_char("DateTime", 'YYYY-MM-DD HH24:MI:SS') as "_id", '{req['Instrument']}' as "Instrument", "TradingDay" as "Tradingday", "High", "Low", "Open", "Close", "Volume"::int, "OpenInterest" from future.future_min where "Instrument" = '{req['Instrument']}' and "TradingDay" between '{req['Begin']}' and '{req['End']}'"""
            df = pd.read_sql_query(sql, self.pg)
        elif req['Type'] == 2:  # Real
            if not self.rds.exists(req['Instrument']):
                return ''
            json_mins = self.rds.lrange(req['Instrument'], 0, -1)
            df = pd.read_json(f"[{','.join(json_mins)}]", orient='records')
            df.rename(columns={'TradingDay': 'Tradingday'}, inplace=True)
            df.loc[:, 'Instrument'] = req['Instrument']
        elif req['Type'] == 3:  # Time
            df = self.df_time
        elif req['Type'] == 4:  # ProductInfo
            df = self.df_productinfo
        elif req['Type'] == 5:  # TradeDate
            df = self.df_canlendar
        elif req['Type'] == 6:  # InstrumentInfo
            df = self.df_inst_proc
        # elif req['Type'] == 7:  # Instrumet888
        #     sql = '''select product as "_id", instrument as "value" from (select instrument, product, rate, row_number() over (partition by product order by rate desc) as rk from future_config.rate_000) a where a.rk = 1'''
        # elif req['Type'] == 8:  # rate000
        #     sql = 'select instrument, rate from future_config.rate_000'
        else:
            return ''
        # K线
        if req['Type'] <= 2:
            # 20181010 采用yyyy-mm-dd格式,无需转换.df['_id'] = df['_id'].apply(lambda x: ''.format(x[0:4] + x[5:7] + x[8:]))  # ==> yyyyMMdd HH:mm:ss
            if req['Type'] == 1:  # 日线
                df_tmp: DataFrame = DataFrame()
                g = df.groupby(by=df['Tradingday'])
                # df_tmp.index = g.indices
                df_tmp['_id'] = g.indices
                df_tmp['Open'] = g['Open'].first()
                df_tmp['High'] = g['High'].max()
                df_tmp['Low'] = g['Low'].min()
                df_tmp['Close'] = g['Close'].last()
                df_tmp['Volume'] = g['Volume'].sum()
                df_tmp['OpenInterest'] = g['OpenInterest'].last()
                df_tmp = df_tmp.reindex(df_tmp['_id'])
                df = df_tmp

            keys = ["_id", "Instrument", "Tradingday", "High", "Low", "Open", "Close", "Volume", "OpenInterest"]
            df = df.loc[:, keys]
        rtn = ''
        if req['Type'] == 5:
            rtn = [r for r in df['_id']]
            rtn = json.dumps(rtn)
        elif req['Type'] in [3, 7]:
            rtn = df.to_json(orient='records')
            # rtn = rtn.replace('"[', '[').replace("]\"", ']')
        elif req['Type'] in [0, 1, 2]:
            rtn = df.to_json(orient='records')
        elif req['Type'] == 8:  # => dict
            rtn = df.to_json(orient='values')
            rtn = '{{{}}}'.format(rtn.replace('[', '').replace(']', '').replace('",', '":'))
        else:  # 4,6
            rtn = df.to_json(orient='records')
        return rtn


if __name__ == '__main__':
    port = 5055
    if 'port' in os.environ:
        port = os.environ['port']
    s = Server(port)
    threading.Thread(target=s.min_csv_pg).start()
    s.run()
