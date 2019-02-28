import urllib.request
import re
import os


# 下载网页方法
def download(url):
    #处理路径
    path = url
    path = path.replace(":", "_")
    path = path.replace("？", "_")
    path = path.replace("/","$")
    path = "E:/IDEA_workspace/Hadoop/PythonDemo/file/urlDemo4/"+path

    if not os.path.exists(path):#处理相同路径
        resp = urllib.request.urlopen(url)
        pageBytes = resp.read()
        resp.close()
        # 保存
        f = open(path, "wb")
        f.write(pageBytes)
        f.close()
    try:
        #解析网页
        pageStr = pageBytes.decode("utf-8")
        # 解析href
        pattern=u'<a\s*href="([\u0000-\uffff&&^"]*?)"'
        res = re.finditer(pattern,pageStr)
        for r in res :
            addr = r.group(1)
            if addr.startswith("http://"):
                download(addr)
    except Exception:
        print(url+":   !!!wrong@!!!")
        return ;
download("http://www.tianya.cn/")
