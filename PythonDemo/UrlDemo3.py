import urllib.request
import re #正则表达式模块 regexp

resp = urllib.request.urlopen("")
cont = resp.read()
resp.close()
pageHtml = cont.decode("utf8")
#\s:空白符； &&：与； ^?：不是？；*？：非贪婪模式
pattern=u'<a\s*href="([\u0000-\uffff&&^"]*?)"'
res = re.finditer(pattern,pageHtml)
for r in res :
    print(r.group(1))