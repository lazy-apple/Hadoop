#-*- encoding=utf-8-*-
# 1.=============================打印：单双引号通用，#：注释

print("hello world!")

# 2.无大括号，缩进要对齐
if True:
    print("true")
else:
    print('false')
# 3.三引号，自带格式
str0 = '''hello
        world 
        
        china'''
print(str0)
# 4.============================标准类型：Numbers （int,long,float,complex(复数)）
#            String
#            List
#            Tuple
#            Dictionary

# 5.String 操作
str0 = "hello"
print(str0[2:4])
print(str0[:4])
print(str0[2:-1])
print(str0 * 2)
#删除变量 del
del str0
# r+字符串：原样输出
# u+字符串
print(r"hello\n")
# 6.List 操作
list = [1,2,3,"hello"];
print(list.__len__())
print(list * 5)
# 7.Tuple 操作
t = (1,2,3,4)# 不能二次赋值
print(t[0:4])
# 8.Dictionary 操作
dict = {100:"tom","code":"5555",2:"value"}# 类似k-v
print(dict[100])
print(dict)
print(dict.values())
# 9类型转换
print(str(dict))
print(int('100')+1)
print(eval("1+1*(2+2)"))#eval:求值
seq=1,2,3,4; print(tuple(t));#tuple:序列（1,2），（3,4），（1,3）
#====================================10.运算符 :
# **：幂运算
print(2 ** 3)
# /:浮点除 .//:java除
print(10 / 3)
# in , not in
print(1 in t)
# is 身份运算，等价于java 的equals
a = b = 100
print(a is not b)
# ==========================================11.流程控制
age = 10
if age<18:
    print("未成年")
elif age>18&age<60:
    print("青年")
else:
    print("老年")
# for循环
rows = [1,2,3,4,5,6,7,8,9] #列表
for i in rows:
    print(i)
# #io
f = open(r"E:\IDEA_workspace\Hadoop\PythonDemo\file\1.txt",'a')#a:追加
lines = f.readlines()
for l in lines:
    print(l,end="")
f.write("how are you")
f.closed







