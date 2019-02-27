#sql
import pymysql
try:
    # 开启连接
    conn = pymysql.connect(host='localhost',user='root',passwd='root',db='pythontest', port=3306, charset='utf8')
    # 打开游标
    cur = conn.cursor()
    # 执行游标
    sql = "select * from student"
    cur.execute(sql)
    conn.commit()
    version = cur.fetchall()
    print(version)
    cur.close()
    conn.close()
except Exception:
    print("异常")
# '''
#   执行事务管理,手动提交事务，回滚事务。
# '''
# conn = pymysql.connect(host='localhost', user='root', passwd='root', db='python', port=3306, charset='utf8')
# try:
#     #关闭自动提交
#     conn.autocommit(False)
#     #开始事务
#     conn.begin()
#     #打开游标
#     cur = conn.cursor();
#     sql = "delete from t1 where id > 130000"
#     cur.execute(sql)
#     conn.commit()
#     cur.close
#
# except Exception:
#     conn.rollback()
# finally:
#     conn.close()

#
#
# '''
#     聚合查询，统计
# '''
# conn = pymysql.connect(host='localhost', user='root', passwd='root', db='python', port=3306, charset='utf8')
# try:
#     # 关闭自动提交
#     conn.autocommit(False)
#     # 开始事务
#     conn.begin()
#     # 打开游标
#     cur = conn.cursor();
#     sql = "select count(*) from t1 where age < 20"
#     cur.execute(sql)
#     res = cur.fetchone()
#     print(res[0])
#     conn.commit()
#     cur.close
#
# except Exception:
#     conn.rollback()
# finally:
#     conn.close()








