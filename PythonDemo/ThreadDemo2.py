# -*- encoding utf-8 -*-
import threading

tickets = 100 ;

loc = threading.Lock()

def getTicket():
    global loc ;
    loc.acquire()
    global tickets
    tmp = 0 ;
    if tickets > 0 :
        tmp = tickets ;
        tickets -= 1 ;
    else:
        tmp = 0 ;
    loc.release()
    return tmp ;


class Saler(threading.Thread):
    def run(self):
        while True:
            tick = getTicket();
            if tick != 0:
                print(self.getName() + " : " + str(tick))
            else:
                return ;


s1 = Saler()
s1.setName("s1")

s2 = Saler()
s2.setName("s2")

s3 = Saler()
s3.setName("s3")

s1.start()
s2.start()
s3.start()
