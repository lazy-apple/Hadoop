import urllib.request
import re
from hbase import PageDao


#下载网页方法
def download(url):
   resp = urllib.request.urlopen(url)
   pageBytes = resp.read()
   resp.close

   if not PageDao.exists(url):
    PageDao.savePage(url, pageBytes)

    try:
        #解析网页内容
        pageStr = pageBytes.decode("utf-8")
        #j解析href

        pattern = u'<a[\u0000-\uffff&&^[href]]*href="([\u0000-\uffff&&^"]*?)"'
        res = re.finditer(pattern,pageStr)
        for i in res:
            addr = i.group(1);
            print(addr)
            if addr.startswith("//"):
                addr = addr.replace("//","http://")
            #判断网页是否包含自己网址
            if addr.startswith("http://") and url != addr and (not PageDao.exists(addr)):
                download(addr)
    except Exception as e:
        print(e)
        print(pageBytes.decode("gbk",errors='ignore'))
        return ;

download("http://jd.com")



