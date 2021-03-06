#!/usr/bin/env python
# coding=utf-8

import logging
logging.basicConfig(level=logging.DEBUG)
from sqlalchemy import select

from pymysqlslave import MySQLDBSlave

jianv1 = MySQLDBSlave(
    masters=[
        {
            "name": "mysql+mysqldb://jianxun:jianxun@jianxun.dev:3306/jianxunv2?charset=utf8",
            "echo": False,
            "pool_size": 5,
            "pool_recycle": 1,
        }

    ],
    slaves=[
    ],
    is_auto_allocation=True)


def get_info_by_email(email):
    _t = jianv1.table.customer_member_t
    sql = select([_t.c.member_id]).where(_t.c.email == email)
    return jianv1.execute(sql).fetchone()


@jianv1.with_random_engine
def get_info_by_email_1(email):
    _t = jianv1.table.customer_member_t
    sql = select([_t.c.member_id]).where(_t.c.email == email)
    return jianv1.execute(sql).fetchone()


@jianv1.with_random_engine
@jianv1.with_reconnect(retry=3)
def get_info_by_email_2(email):
    _t = jianv1.table.customer_member_t
    sql = select([_t.c.member_id]).where(_t.c.email == email)
    return jianv1.execute(sql).fetchone()


def update_info_by_email(email):
    _t = jianv1.table.customer_member_t
    sql = _t.update().where(_t.c.email == email).values(name=u"穿完")
    jianv1.execute(sql)


if __name__ == "__main__":
    result = get_info_by_email("592030542@qq.com")
    logging.warn(result)
    result = get_info_by_email_1("592030542@qq.com")
    logging.warn(result)
    result = get_info_by_email_2("592030542@qq.com")
    logging.warn(result)
