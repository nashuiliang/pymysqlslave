# SQLAlchemy Simple Master Slave Load Balancing(***beta***)


> DEMO
> ``` python
> #!/usr/bin/env python
> # coding=utf-8
> 
> import logging
> logging.basicConfig(level=logging.DEBUG)
> from sqlalchemy import select
> 
> from sqlalchemyMySQLDB import MySQLDBSlave
> 
> jianv1 = MySQLDBSlave(
>     masters=[
>         "mysql+mysqldb://jianv1:jianv1passwd@10.10.10.3:3306/jianv1?charset=utf8",
>     ],
>     slaves=[
>     ])
> 
> 
> @jianv1.with_random_engine
> def get_info_by_email(email):
>     _t = jianv1.table.customer_member_t
>     sql = select([_t], _t.c.email == email)
>     return jianv1.execute(sql).fetchone()
> 
> 
> if __name__ == "__main__":
>     result = get_info_by_email("592030542@qq.com")
>     logging.warn(result)
> 
> ```
> 
> 
