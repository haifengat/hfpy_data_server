#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = '数据服务'
__author__ = 'HaiFeng'
__mtime__ = '20180911'

import zmq
import gzip
from pandas import read_sql_query
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import time
import json
import sys


class Server(object):

    def __init__(self, port):
        self.pg_conn_str = ''
        cfg = json.load(open('./config.json', 'r', encoding='utf-8'))
        if 'postgres_config' in cfg:
            cfg_pg = cfg['postgres_config']
            self.pg_conn_str = 'postgresql://{}:{}@{}:{}/{}'.format(cfg_pg['user'], cfg_pg['pwd'], cfg_pg['host'], cfg_pg['port'], cfg_pg['db'])
        self.pg: Engine = create_engine(self.pg_conn_str)

        context = zmq.Context(1)
        self.server = context.socket(zmq.REP)
        self.server.bind('tcp://*:{}'.format(port))

    def run(self):
        print('listen to port: {}'.format(self.server.LAST_ENDPOINT.decode()))
        while True:
            request = self.server.recv_json()  # .recv_string()
            print(request)
            # Min, Day, Real, Time, Product, TradeDate, InstrumentInfo, Instrumet888, Rate000
            rsp = self.read_data(request)
            rsp = gzip.compress(rsp.encode(), 9)
            self.server.send(rsp)

    def read_data(self, req) -> str:
        """
            Min, Day, Real, Time, Product, TradeDate, InstrumentInfo, Instrumet888, Rate000
        """
        if req['Type'] in [0, 1]:  # Min, Day
            sql = 'select "DateTime" as "_id", \'{0}\' as "Instrument", "Tradingday", "High", "Low", "Open", "Close", "Volume"::int, "OpenInterest" from future_min."{0}" where "Tradingday" between \'{1}\' and \'{2}\''.format(req['Instrument'], req['Begin'], req['End'])
        elif req['Type'] == 2:  # Real
            sql = 'select "DateTime" as "_id", "Instrument", "Tradingday", "High", "Low", "Open", "Close", "Volume"::int, "OpenInterest" from future_min.future_real where "Instrument" = \'{}\''.format(req['Instrument'])
        elif req['Type'] == 3:  # Time
            sql = '''select "GroupId", "WorkingTimes" from (select "GroupId", "WorkingTimes"::json , row_number() over (partition by "GroupId" order by "OpenDate" desc) as rk from future_config.tradingtime) as t where t.rk = 1'''
        elif req['Type'] == 4:  # Product
            sql = '''select * from (select productid as "_id", pricetick as "PriceTick", volumemultiple::int as "VolumeTuple", exchangeid as "ExchangeID", productclass as "ProductType", row_number() over (partition by productid order by opendate desc) as rk from future_config.instrument where expiredate > '20180914') as t where t.rk = 1'''
        elif req['Type'] == 5:  # TradeDate
            sql = 'select _id from future_config.trade_date where trading = 1'
        elif req['Type'] == 6:  # InstrumentInfo
            sql = 'select instrumentid as "_id", productid as "ProductID" from future_config.instrument where expiredate > \'{0}\' union select productid || \'_000\' as "_id", productid as "ProductID" from future_config.instrument where expiredate > \'{0}\''.format(time.strftime('%Y%m%d', time.localtime()))
        elif req['Type'] == 7:  # Instrumet888
            sql = '''select product as "_id", instrument as "value" from (select instrument, product, rate, row_number() over (partition by product order by rate desc) as rk from future_config.rate_000) a where a.rk = 1'''
        elif req['Type'] == 8:  # rate000
            sql = 'select instrument, rate from future_config.rate_000'
        else:
            sys.exit(-1)
        # 调用前需重复调用Create engine 否则报错
        self.pg = create_engine(self.pg_conn_str)
        with self.pg.connect() as connection:
            df: DataFrame = read_sql_query(sql, connection)
            connection.close()
        # K线
        if req['Type'] <= 2:
            # 20181010 采用yyyy-mm-dd格式,无需转换.df['_id'] = df['_id'].apply(lambda x: ''.format(x[0:4] + x[5:7] + x[8:]))  # ==> yyyyMMdd HH:mm:ss
            if req['Type'] == 1:  # 日线
                df_tmp: DataFrame = DataFrame()
                g = df.groupby(by=df['Tradingday'])
                df_tmp.index = g.indices
                df_tmp['_id'] = g.indices
                df_tmp['Open'] = g['Open'].first()
                df_tmp['High'] = g['High'].max()
                df_tmp['Low'] = g['Low'].min()
                df_tmp['Close'] = g['Close'].last()
                df_tmp['Volume'] = g['Volume'].sum()
                df_tmp['OpenInterest'] = g['OpenInterest'].last()
                df = df_tmp

            keys = ["_id", "Instrument", "Tradingday", "High", "Low", "Open", "Close", "Volume", "OpenInterest"]
            df = df.ix[:, keys]
        rtn = ''
        if req['Type'] == 5:
            rtn = [r for r in df['_id']]
            rtn = json.dumps(rtn)
        elif req['Type'] in [3, 7]:
            rtn = df.to_json(orient='records')
            rtn = rtn.replace('"[', '[').replace("']", ']')
        elif req['Type'] in [0, 1, 2]:
            rtn = df.to_json(orient='records')
        elif req['Type'] == 8:  # => dict
            rtn = df.to_json(orient='values')
            rtn = '{{{}}}'.format(rtn.replace('[', '').replace(']', '').replace('",', '":'))
        else:
            rtn = df.to_json(orient='records')
        return rtn


def main():
    s = Server(5055)
    s.run()


if __name__ == '__main__':
    main()
    input()
