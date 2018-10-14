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
import yaml
from color_log import Logger


class Server(object):

    def __init__(self, port):
        self.log = Logger()
        cfg = yaml.load(open('./config.yml', 'r', encoding='utf-8'))
        self.pg: Engine = None
        if 'pg_config' in cfg:
            self.pg = create_engine(cfg['pg_config'])
        self.ora: Engine = None
        if 'ora_config' in cfg:
            self.ora = create_engine(cfg['ora_config'])

        context = zmq.Context(1)
        self.server = context.socket(zmq.REP)
        self.server.bind('tcp://*:{}'.format(port))

    def run(self):
        self.log.war('listen to port: {}'.format(self.server.LAST_ENDPOINT.decode()))
        while True:
            try:
                request = self.server.recv_json()  # .recv_string()
                self.log.info(request)
                # Min, Day, Real, Time, Product, TradeDate, InstrumentInfo, Instrumet888, Rate000
                rsp = self.read_data(request)
                if rsp == '':
                    self.log.error('{} 未取得数据'.format(request))
                rsp = gzip.compress(rsp.encode(), 9)
                self.server.send(rsp)
            except Exception as err:
                self.log.error(str(err))
                self.log.error('=== 错误导致程序退出 ===')
                break

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
        elif req['Type'] == 4:  # Product==>instrument用了外联oracle所以查询 > 10s
            # sql = '''select * from (select productid as "_id", pricetick as "PriceTick", volumemultiple::int as "VolumeTuple", exchangeid as "ExchangeID", productclass as "ProductType", row_number() over (partition by productid order by opendate desc) as rk from future_config.instrument where expiredate > '{}') as t where t.rk = 1'''.format(time.strftime('%Y%m%d', time.localtime()))
            sql = '''select * from (select trim(productid) as "_id", pricetick as "PriceTick", volumemultiple as "VolumeTuple", trim(exchangeid) as "ExchangeID", productclass as "ProductType", row_number() over (partition by productid order by opendate desc) as rk from SOURCETMP.T_INSTRUMENT where expiredate > '{}')  t where t.rk = 1'''.format(time.strftime('%Y%m%d', time.localtime()))
        elif req['Type'] == 5:  # TradeDate
            sql = 'select _id from future_config.trade_date where trading = 1'
        elif req['Type'] == 6:  # InstrumentInfo
            # sql = '''select instrumentid as "_id", productid as "ProductID" from future_config.instrument where expiredate > '{0}'  union select productid || '_000' as "_id", productid as "ProductID" from future_config.instrument where expiredate > '{0}'  '''.format(time.strftime('%Y%m%d', time.localtime()))
            sql = '''select trim(instrumentid) as "_id", trim(productid) as "ProductID" from SOURCETMP.T_INSTRUMENT where expiredate > '{0}' union select trim(productid) || '_000' as "_id", trim(productid) as "ProductID" from SOURCETMP.T_INSTRUMENT where expiredate > '{0}' '''.format(time.strftime('%Y%m%d', time.localtime()))
        elif req['Type'] == 7:  # Instrumet888
            sql = '''select product as "_id", instrument as "value" from (select instrument, product, rate, row_number() over (partition by product order by rate desc) as rk from future_config.rate_000) a where a.rk = 1'''
        elif req['Type'] == 8:  # rate000
            sql = 'select instrument, rate from future_config.rate_000'
        else:
            return ''
        # 调用前需重复调用Create engine 否则报错: Operation cannot be accomplished in current state
        if self.pg is not None:
            self.pg = create_engine(self.pg.url)
        # if self.ora is not None:
        #     self.ora = create_engine(self.ora.url)
        # with (self.ora.raw_connection() if req['Type'] in [4, 6] else self.pg.raw_connection()) as connection:
        # connection = self.ora.raw_connection() if req['Type'] in [4, 6] else self.pg.raw_connection()
        en = self.ora if req['Type'] in [4, 6] else self.pg
        df: DataFrame = None
        try:
            df = read_sql_query(sql, en)
        except Exception as err:
            # self.log.error(str(err))
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
            rtn = rtn.replace('"[', '[').replace("']", ']')
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
