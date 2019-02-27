import threading,_thread,time
#定义函数
def hello(str):
    tname = threading.current_thread().getName()
    print(tname+" "+str)

_thread.start_new_thread(hello,("helloworld",))
_thread.start_new_thread(hello,("helloworld",))

time.sleep(5)














