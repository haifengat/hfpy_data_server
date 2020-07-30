#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = '数据服务'
__author__ = 'HaiFeng'
__mtime__ = '20180911'

import zmq, redis
import gzip
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import os
import json
from color_log import Logger


class Server(object):

    def __init__(self, port):
        self.log = Logger()
        
        self.df_time = pd.read_csv('/home/tradingtime.csv', converters={'OpenDate':str})
        g = self.df_time.groupby(by=['GroupId'])
        df_tmp = g['OpenDate'].max()
        self.df_time = self.df_time[self.df_time.apply(lambda x: x['OpenDate']==df_tmp[x['GroupId']], axis=1)]

        self.df_canlendar = pd.read_csv('/home/calendar.csv', converters={'day':str})
        self.df_canlendar = self.df_canlendar[self.df_canlendar['tra']][['day']]
        self.df_canlendar.rename(columns={'day':'_id'}, inplace=True)

        df_instrument = pd.read_csv('/home/instrument.csv', converters={'OPENDATE': str, 'EXPIREDATE': str})        
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

        redis_host, rds_port = '127.0.0.1', 16379
        if 'redis_addr' in os.environ:
            redis_host = os.environ['redis_addr']
        if ':' in redis_host:
            redis_host, rds_port =  redis_host.split(':')
        self.log.info(f'connecting redis: {redis_host}:{rds_port}')
        pool = redis.ConnectionPool(host=redis_host, port=rds_port, db=0, decode_responses=True)
        self.rds = redis.StrictRedis(connection_pool=pool)

        context = zmq.Context(1)
        self.server = context.socket(zmq.REP)
        self.server.bind('tcp://*:{}'.format(port))

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
        elif req['Type'] == 7:  # Instrumet888
            sql = '''select product as "_id", instrument as "value" from (select instrument, product, rate, row_number() over (partition by product order by rate desc) as rk from future_config.rate_000) a where a.rk = 1'''
        elif req['Type'] == 8:  # rate000
            sql = 'select instrument, rate from future_config.rate_000'
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
    s = Server(5055)
    s.run()
