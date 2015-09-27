#!/usr/bin/env python
# coding=utf-8

__version__ = '1.0.4'

import functools
import logging
import traceback

from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError

#: logging handler
_logger = logging.getLogger("pymysqlslave")
_log_handler = logging.StreamHandler()
_log_formatter = logging.Formatter("[%(levelname)s][%(asctime)s]: %(message)s")
_log_handler.setFormatter(_log_formatter)
_logger.addHandler(_log_handler)

from .dbutils import MySQLSelector, CONST_MASTER_KEY, CONST_SLAVE_KEY, CONST_ALL_KEY, MASTER_HANDLERS


__all__ = [
    "MySQLOperationalError", "MySQLDBSlave"
]


class MySQLOperationalError(Exception):
    pass


class MySQLDBSlave(object):

    def __init__(self, masters, slaves=None, is_auto_allocation=False, reconnect_retry_nums=1, is_reconnect=True):
        """ init mysqldb slave
        :param list masters:
                mysql masters `sqlalchemy.create_engine(*args, **kwargs)` list,
                create a new :class:`.Engine` instance.
        :param list slave:
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
            all_slaves.append(create_engine(**item))

        if all([all_masters, all_slaves]):
            _logger.debug("mysqldb clients contain masters and slaves")
        elif all_masters:
            _logger.debug("mysqldb clients include only masters")
        elif all_slaves:
            _logger.debug("mysqldb clients include only slaves")
        else:
            raise MySQLOperationalError("`MySQLDBSlave` instantiation"
                                        "requires parameters masters or slaves value is not empty")

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
        self._engine.client_type = None
        self._engine.client = None

    def with_master(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.client_type = CONST_MASTER_KEY
            self._engine.client = self._selector.get_master_engine()

            if not self.is_reconnect:
                return method(*args, **kwargs)
            return self.with_reconnect(self.retry_nums)(method)(*args, **kwargs)
        return _wrap

    def with_slave(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.client_type = CONST_SLAVE_KEY
            self._engine.client = self._selector.get_slave_engine()

            if not self.is_reconnect:
                return method(*args, **kwargs)
            return self.with_reconnect(self.retry_nums)(method)(*args, **kwargs)
        return _wrap

    def with_random_engine(self, method):
        @functools.wraps(method)
        def _wrap(*args, **kwargs):
            self._engine.client_type = CONST_ALL_KEY
            self._engine.client = self._selector.get_random_engine()

            if not self.is_reconnect:
                return method(*args, **kwargs)
            return self.with_reconnect(self.retry_nums)(method)(*args, **kwargs)
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
                        _logger.debug(("Retry:{} with_reconnect:{}".format(i + 1, f_val), u"mysqldb reconnect", e))

                        # reconnect mysqldb
                        engine = self._engine.client
                        engine.connect()
                        self._selector.update(self._engine.client_type, engine)
                        continue

                _logger.error(traceback.format_exc())
                raise MySQLOperationalError(
                    "mysqldb_reconnect:{} *retry:{}*. But MySQL server has gone away".format(f_val, retry))
            return _wrap
        return _reconnect

    def execute(self, statement, *multiparams, **params):
        #: is open auth_allocation and not set engine client(with_master, with_slave, with_random_engine)
        if self.is_auto_allocation and not self._engine.client:
            raw_statement = str(statement)
            if not raw_statement:
                raise MySQLOperationalError("statement not empty")

            statement_handler = raw_statement[:6].split(" ")[0]

            #: insert, create, update
            if statement_handler in MASTER_HANDLERS:
                self._engine.client_type = CONST_MASTER_KEY
                self._engine.client = self._selector.get_master_engine()
            else:
                self._engine.client_type = CONST_ALL_KEY
                self._engine.client = self._selector.get_random_engine()

            _logger.debug("Statement Handler TYPE: {}".format(self._engine.client_type))

        #: execute
        assert self._engine.client
        result = self._engine.execute(statement, *multiparams, **params)
        self._reset_engine()
        return result


class _MySQLEngine(object):
    def __init__(self, meta_data):
        self._meta_data = meta_data
        self._client = None
        self._client_type = None

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

    def execute(self, statement, *multiparams, **params):
        return self._client.execute(statement, *multiparams, **params)

    def __getattr__(self, name):
        try:
            return self._meta_data.tables[name]
        except KeyError:
            raise exc.NoSuchTableError(name)
