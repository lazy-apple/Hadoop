class Dog:
    name:"默认"#相当于静态变量
    #构造函数
    def __in(self,age):
        self.age = age#相当于创建实例变量

    def add(self,a,b):
        return a+b
    def _count(self,a,b):#私有方法
        return a+b
#创建对象
d1 = Dog()
print(d1.add(1, 3))

#=============================================继承：hashiqi继承Dog
class hashiqi(Dog):
    def run(self):
        print("run")
h = hashiqi()
h.run()