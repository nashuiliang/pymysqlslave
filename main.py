#!/usr/bin/env python
# coding=utf-8

import logging
logging.basicConfig(level=logging.DEBUG)
from sqlalchemy import select

from sqlalchemyMySQLDB import MySQLDBSlave

jianv1 = MySQLDBSlave(
    masters=[
        "mysql+mysqldb://jianxun:jianxun@jianxun.dev:3306/jianxunv2?charset=utf8",
    ],
    slaves=[
    ])


@jianv1.with_random_engine
def get_info_by_email(email, **kwargs):
    _t = jianv1.table.customer_member_t
    sql = select([_t], _t.c.email == email)
    return jianv1.execute(sql, **kwargs).fetchone()


if __name__ == "__main__":
    result = get_info_by_email("592030542@qq.com")
    logging.warn(result)
