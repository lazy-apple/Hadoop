from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
# 导入自已编译生成的hbase python模块
from mythrift.hbase import THBaseService
from mythrift.hbase.ttypes import *
from mythrift.hbase.ttypes import TResult
import base64
import os

# 创建Socket连接，到s201:9090
transport = TSocket.TSocket('s201', 9090)
transport = TTransport.TBufferedTransport(transport)#
protocol = TBinaryProtocol.TBinaryProtocol(transport)#
client = THBaseService.Client(protocol)

#定义函数保存网页
def savePage(url,page):
    transport.open()
    urlBase64Bytes = base64.encodebytes(url.encode("utf-8"))

    #插入
    table = b'ns1:pages'
    rowkey = urlBase64Bytes
    v1 = TColumnValue(b'f1',b'page',page)
    vals=[v1]
    put = TPut(rowkey,vals)
    client.put(table,put)
    transport.close()

#判断网页是否存在
def exists(url):
    transport.open()
    urlBase64Bytes = base64.encodebytes(url.encode("utf-8"))
    table = b'ns1:pages'
    rowkey = urlBase64Bytes
    col_page = TColumn(b'f1',b"page")
    cols = [col_page]
    get = TGet(rowkey,cols)
    res = client.get(table,get)
    transport.close()
    return res.row is not None









