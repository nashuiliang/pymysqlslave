#!/usr/bin/env python
# coding=utf-8

__version__ = '1.0.6'

import functools
import logging
import traceback

from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError

#: logging handler
_logger = logging.getLogger("pymysqlslave")

from .dbutils import MySQLSelector, CONST_MASTER_KEY, CONST_SLAVE_KEY, CONST_ALL_KEY, MASTER_HANDLERS


__all__ = [
    "MySQLOperationalError", "MySQLDBSlave"
]


class MySQLOperationalError(Exception):
    pass


class MySQLDBSlave(object):

    def __init__(self, masters, slaves=None, is_auto_allocation=False, reconnect_retry_nums=3, is_reconnect=True):
        """ init mysqldb slave
        :param list masters:
                mysql masters `sqlalchemy.create_engine(*args, **kwargs)` list,
                create a new :class:`.Engine` instance.
        :param list slaves:
                mysql slaves `sqlalchemy.create_engine(*args, **kwargs)` list,
                create a new :class:`.Engine` instance.
        :param bool is_reconnect:
                when the Mysql client is connecting timeout, the client is trying to reconnect
        :param int reconnect_retry_nums:
                when the client is connecting timeout and opens the `is_reconnect`,
                the client retries to connecting times
        :param bool is_auto_allocation: open `auto_allocation`, the program will allocate masters or slaves
        """

        all_masters = list()
        for item in masters:
            name = item["name"]
            item.pop("name", None)
            all_masters.append(create_engine(name, **item))

        if not slaves:
            slaves = list()
        all_slaves = list()
        for item in slaves:
            name = item["name"]
            item.pop("name", None)
            all_slaves.append(create_engine(name, **item))

        if all([all_masters, all_slaves]):
            _logger.debug("mysqldb clients contain masters and slaves")
        elif all_masters:
            _logger.debug("mysqldb clients include only masters")
        elif all_slaves:
            _logger.debug("mysqldb clients include only slaves")
        else:
            raise MySQLOperationalError("`MySQLDBSlave` instantiation"
                                        "requires parameters masters or slaves values are not empty")

        if is_auto_allocation and not all_masters:
            raise MySQLOperationalError("`is_auto_allocation` should contain master clients")

        #: selector
        self._selector = MySQLSelector(all_masters, all_slaves)

        #: init engine
        self._init_mysql_engine()

        self.is_reconnect = is_reconnect
        self.reconnect_retry_nums = reconnect_retry_nums
        self.is_auto_allocation = is_auto_allocation

        if self.is_reconnect and self.reconnect_retry_nums <= 0:
            _logger.error("reconnect retry nums > 0")
            raise MySQLOperationalError("please modify `reconnect_retry_nums`")

    @property
    def table(self):
        return self._engine

    def _init_mysql_engine(self):
        meta_data = MetaData()
        meta_data.reflect(self._selector.get_random_engine())
        self._engine = _MySQLEngine(meta_data)

    def _reset_engine(self):
        self._engine.mysql_client = None
        self._engine.mysql_client_type = None
        self._engine.mysql_reconnect_open_retry = False

    def with_master(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.mysql_client = self._selector.get_master_engine()
            self._engine.mysql_client_type = CONST_MASTER_KEY

            if not self.is_reconnect or self._engine.mysql_reconnect_open_retry:
                return method(*args, **kwargs)
            return self.with_reconnect(self.reconnect_retry_nums)(method)(*args, **kwargs)
        return _wrap

    def with_slave(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.mysql_client = self._selector.get_slave_engine()
            self._engine.mysql_client_type = CONST_SLAVE_KEY

            if not self.is_reconnect or self._engine.mysql_reconnect_open_retry:
                return method(*args, **kwargs)
            return self.with_reconnect(self.reconnect_retry_nums)(method)(*args, **kwargs)
        return _wrap

    def with_random_engine(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.mysql_client = self._selector.get_random_engine()
            self._engine.mysql_client_type = CONST_ALL_KEY

            if not self.is_reconnect or self._engine.mysql_reconnect_open_retry:
                return method(*args, **kwargs)
            return self.with_reconnect(self.reconnect_retry_nums)(method)(*args, **kwargs)
        return _wrap

    def with_reconnect(self, retry=3):

        def _reconnect(method):
            @functools.wraps(method)
            def _wrap(*args, **kwargs):

                #: open retry
                self._engine.mysql_reconnect_open_retry = True

                _f = lambda: method(*args, **kwargs)

                for i in xrange(retry + 1):
                    try:
                        return _f()
                    except OperationalError as e:

                        f_name = method.__name__
                        f_module = self.__class__.__module__
                        f_class = self.__class__.__name__
                        f_val = "{}:{}:{}".format(f_module, f_class, f_name)
                        _logger.debug(("Retry:{} with_reconnect:{}".format(i + 1, f_val), u"mysqldb reconnect", e))

                        # reconnect mysqldb
                        engine = self._engine.mysql_client
                        engine.connect()
                        self._selector.update(self._engine.mysql_client_type, engine)
                        continue

                _logger.error(traceback.format_exc())
                raise MySQLOperationalError(
                    "mysqldb_reconnect:{} *retry:{}*. But MySQL server has gone away".format(f_val, retry))
            return _wrap
        return _reconnect

    def execute(self, statement, *multiparams, **params):
        #: is open auth_allocation and not set engine client(with_master, with_slave, with_random_engine)
        if self.is_auto_allocation and not self._engine.mysql_client:
            self._allocate_engine_by_statement(statement)

        #: not open retry
        if self.is_reconnect and not self._engine.mysql_reconnect_open_retry:
            return self.with_reconnect(self.reconnect_retry_nums)(self._execute)(statement, *multiparams, **params)

        #: open retry
        return self._execute(statement, *multiparams, **params)

    def _allocate_engine_by_statement(self, statement):
        raw_statement = str(statement)
        if not raw_statement:
            raise MySQLOperationalError("statement not empty")

        statement_handler = raw_statement[:6].split(" ")[0]

        #: insert, create, update
        if statement_handler in MASTER_HANDLERS:
            self._engine.mysql_client = self._selector.get_master_engine()
            self._engine.mysql_client_type = CONST_MASTER_KEY
        else:
            self._engine.mysql_client = self._selector.get_random_engine()
            self._engine.mysql_client_type = CONST_ALL_KEY

        _logger.debug("Statement Handler TYPE: {}".format(self._engine.mysql_client_type))

    def _execute(self, statement, *multiparams, **params):
        #: execute
        assert self._engine.mysql_client
        result = self._engine.execute(statement, *multiparams, **params)

        #: reset engine
        self._reset_engine()
        return result


class _MySQLEngine(object):
    def __init__(self, meta_data):
        self._meta_data = meta_data
        self._client = None
        self._client_type = None
        self._reconnect_open_retry = False

    @property
    def mysql_client(self):
        return self._client

    @mysql_client.setter
    def mysql_client(self, val):
        self._client = val

    @property
    def mysql_client_type(self):
        return self._client_type

    @mysql_client_type.setter
    def mysql_client_type(self, val):
        self._client_type = val

    @property
    def mysql_reconnect_open_retry(self):
        return self._reconnect_open_retry

    @mysql_reconnect_open_retry.setter
    def mysql_reconnect_open_retry(self, val):
        self._reconnect_open_retry = val

    def execute(self, statement, *multiparams, **params):
        return self._client.execute(statement, *multiparams, **params)

    def __getattr__(self, name):
        try:
            return self._meta_data.tables[name]
        except KeyError:
            raise exc.NoSuchTableError(name)
