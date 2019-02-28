# -*- encoding=utf-8 -*-
import urllib.request
#下载
resp = urllib.request.urlopen("https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1551328109928&di=70084bd22888e8dde3725f859e8cbf64&imgtype=0&src=http%3A%2F%2Fimg32.photophoto.cn%2F20140717%2F0029013773135456_s.jpg")
data = resp.read()
resp.close()
#保存
f = open("E:/IDEA_workspace/Hadoop/PythonDemo/file/urlDemo2/1.jpg","wb")
f.write(data)
f.close()
