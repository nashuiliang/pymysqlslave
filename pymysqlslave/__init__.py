#!/usr/bin/env python
# coding=utf-8

__version__ = '1.0.1'

import functools
import logging
import traceback

from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError

from .dbutils import MySQLSelector, CONST_MASTER_KEY, CONST_SLAVE_KEY, CONST_ALL_KEY


__all__ = [
    "MySQLOperationalError", "MySQLDBSlave"
]


class MySQLOperationalError(Exception):
    pass


class MySQLDBSlave(object):

    def __init__(self, masters, slaves=None, retry_nums=1, is_retry=True):

        all_masters = list()
        for item in masters:
            all_masters.append(create_engine(item))

        if not slaves:
            slaves = list()
        all_slaves = list()
        for item in slaves:
            all_slaves.append(create_engine(item))

        #: selector
        self._selector = MySQLSelector(all_masters, all_slaves)

        #: init engine
        self._init_mysql_engine()

        self.is_retry = is_retry
        self.retry_nums = retry_nums

        if self.is_retry and self.retry_nums <= 0:
            logging.error("retry nums > 0")
            raise MySQLOperationalError("please modify retry nums")

    @property
    def table(self):
        return self._engine

    def _init_mysql_engine(self):
        meta_data = MetaData()
        meta_data.reflect(self._selector.get_master_engine())
        self._engine = _MySQLEngine(meta_data)

    def with_master(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.client_type = CONST_MASTER_KEY
            self._engine.client = self._selector.get_master_engine()

            if not self.is_retry:
                return method(*args, **kwargs)
            return self.with_reconnect(self.retry_nums)(method)(*args, **kwargs)
        return _wrap

    def with_slave(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.client_type = CONST_SLAVE_KEY
            self._engine.client = self._selector.get_slave_engine()

            if not self.is_retry:
                return method(*args, **kwargs)
            return self.with_reconnect(self.retry_nums)(method)(*args, **kwargs)
        return _wrap

    def with_random_engine(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.client_type = CONST_ALL_KEY
            self._engine.client = self._selector.get_random_engine()

            if not self.is_retry:
                return method(*args, **kwargs)
            return self.with_reconnect(self.retry_nums)(method)(self, *args, **kwargs)
        return _wrap

    def with_reconnect(self, retry=1):

        def _reconnect(method):
            @functools.wraps(method)
            def _wrap(self, *args, **kwargs):
                _f = lambda: method(*args, **kwargs)

                for i in xrange(retry + 1):
                    try:
                        return _f()
                    except OperationalError as e:

                        f_name = method.__name__
                        f_module = self.__class__.__module__
                        f_class = self.__class__.__name__
                        f_val = "{}:{}:{}".format(f_module, f_class, f_name)
                        logging.info(("Retry:{} mysqldb_reconnect:{}".format(i + 1, f_val), u"mysqldb reconnect", e))

                        # reconnect mysqldb
                        engine = self._engine.client
                        engine.connect()
                        self._selector.update(self._engine.client_type, engine)
                        continue

                logging.error(traceback.format_exc())

                raise MySQLOperationalError(
                    "mysqldb_reconnect:{} *retry:{}*. But MySQL server has gone away".format(f_val, retry))
            return _wrap
        return _reconnect

    def execute(self, *multiparams, **params):
        return self._engine.execute(*multiparams, **params)


class _MySQLEngine(object):
    def __init__(self, meta_data):
        self._meta_data = meta_data

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, val):
        self._client = val

    @property
    def client_type(self):
        return self._client_type

    @client_type.setter
    def client_type(self, val):
        self._client_type = val

    def execute(self, *multiparams, **params):
        return self._client.execute(*multiparams, **params)

    def __getattr__(self, name):
        try:
            return self._meta_data.tables[name]
        except KeyError:
            raise exc.NoSuchTableError(name)
