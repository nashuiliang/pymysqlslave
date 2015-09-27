#!/usr/bin/env python
# coding=utf-8

import random

from sqlalchemy.engine import url
from sqlalchemy.engine.base import Engine


CONST_MASTER_KEY = "m"
CONST_SLAVE_KEY = "s"
CONST_ALL_KEY = "a"

SLAVE_HANDLERS = [
    "SELECT"
]

MASTER_HANDLERS = [
    "INSERT",
    "DELETE",
    "UPDATE",
    "CREATE",
    "DROP",
    "ALTER",
]


class MySQLSelector(object):

    def __init__(self, masters, slaves):
        self._all_engines = {
            CONST_MASTER_KEY: {},
            CONST_SLAVE_KEY: {}
        }

        for item in masters:
            self._all_engines[CONST_MASTER_KEY][self.get_engine_key(item.url)] = item

        for item in slaves:
            self._all_engines[CONST_SLAVE_KEY][self.get_engine_key(item.url)] = item

    @property
    def all_masters(self):
        return self._all_engines[CONST_MASTER_KEY]

    @property
    def all_slaves(self):
        return self._all_engines[CONST_SLAVE_KEY]

    def get_engine_key(self, resource):
        """ get engine key "{host}:{port}"

        :params type resource: (str/url.URL)
        :return: "{host}:{port}"
        """

        url_resource = None
        if isinstance(resource, str):
            url_resource = url.make_url(resource)
        if isinstance(resource, url.URL):
            url_resource = resource

        assert url_resource is not None
        return "{}:{}".format(url_resource.host, url_resource.port)

    def get_engine(self, engine_type, engine_key):
        """ get engine

        :params str engine_type: master/slave
        :params str engine_key: "{host}:{port}"
        :return: engine
        """

        return self._all_engines[engine_type][engine_key]

    def add_engine(self, engine_type, engine_key, engine):
        """ add engine

        :params str engine_type: master/slave
        :params str engine_key: "{host}:{port}"
        :params Engine engine: engine
        """

        assert isinstance(engine, Engine)
        self._all_engines[engine_type][engine_key] = engine

    def update(self, engine_type, engine):
        """ update engine

        :params Engine engine: engine
        """

        key = self.get_engine_key(engine.url)

        if engine_type == CONST_ALL_KEY:
            if key in self.all_masters:
                self._all_engines[CONST_MASTER_KEY][key] = engine

            if key in self.all_slaves:
                self._all_engines[CONST_SLAVE_KEY][key] = engine

        else:
            self._all_engines[engine_type][key] = engine

    def remove(self, engine_type, engine):
        """ remove resource

        :params type resource: (str/url.URL)
        """

        key = self.get_engine_key(engine.url)
        self._all_engines[engine_type].pop(key, None)

    def get_master_engine(self):
        """ get master engine from self.raw_masters

        :return: master engine
        """

        assert self.all_masters
        return random.choice(self.all_masters.values())

    def get_slave_engine(self):
        """ get slave engine from self.raw_slaves or self.raw_masters

        :return: slave engine
        """

        if not self.all_slaves:
            return self.get_master_engine()

        return random.choice(self.all_slaves.values())

    def get_random_engine(self):
        """ get engine from self.raw_slaves and self.raw_masters

        :return: engine
        """

        return random.choice(self.all_masters.values() + self.all_slaves.values())
