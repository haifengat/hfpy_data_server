#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = '配置文件'
__author__ = 'HaiFeng'
__mtime__ = '20180912'


from psycopg2 import create_engine
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import json


class Config(object):

    def __init__(self, cfg_file):

        cfg = json.load(open(cfg_file, 'r', encoding='utf-8'))
        self.engine_postgres: Engine = None
        if 'postgres_config' in cfg:
            cfg_pg = cfg['postgres_config']
            self.engine_postgres = create_engine('postgresql://{}:{}@{}:{}/{}'.format(cfg_pg['user'], cfg_pg['pwd'], cfg_pg['host'], cfg_pg['port'], cfg_pg['db']))
