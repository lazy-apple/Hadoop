import urllib.request
#打开url地址上的资源
resp = urllib.request.urlopen("http://www.python.org")
#读取内容，作为bytes读取
mybytes = resp.read()
#解码：bytes->String
mystr = mybytes.decode("utf8")
#关闭资源
resp.close()
# #保存
# f = open("", "wb")
# f.write(mybytes)
# f.close()
#输出
print(mystr)