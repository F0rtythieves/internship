#studentExample 例子 练习
def map_func(x):
    s = x.split()
    return (s[0], [int(s[1]),int(s[2]),int(s[3])]) #返回为（key,vaklue）格式，其中key:x[0],value:x[1]且为有三个元素的列表    
    #return (s[0],[int(s[1],s[2],s[3])])   #注意此用法不合法

def has100(x):
    for y in x:
        if(y == 100):  #把x、y理解为 x轴、y轴 
            return True
        return False

def allis0(x):
    if(type(x)==list and sum(x) == 0): #类型为list且总分为0 者为true；其中type(x)==list :判断类型是否相同
        return True
    return False

def subMax(x,y):
    m = [x[1][i] if(x[1][i] > y[1][i]) else y[1][i] for i in range(3)]
    return('Maximum subject score', m)

def sumSub(x,y):
    n = [x[1][i]+y[1][i] for i in range(3)]
    #或者 n = ([x[1][0]+y[1][0],x[1][1]+y[1][0],x[1][2]+y[1][2]]) 
    return('Total subject score', n)

def sumPer(x):
    return (x[0],sum(x[1]))


#停止之前的SparkContext，不然重新运行或者创建工作会失败；另外，只有 sc.stop()也可以，但是首次运行会有误    
try:
    sc.stop()  
except:
    pass 

from pyspark import SparkContext   #导入模块
sc=SparkContext(appName='Student')  #命名
lines=sc.textFile("student.txt").map(lambda x:map_func(x)).cache() #导入数据且保持在内存中，其中cache()：数据保持在内存中
count=lines.count()  #对RDD中的数据个数进行计数；其中，RDD一行为一个数据集


#RDD'转换'运算 （筛选 关键字filter）
whohas100 = lines.filter(lambda x: has100(x[1])).collect() #注意：处理的是value列表，也就是x[1]
whois0 = lines.filter(lambda x: allis0(x[1])).collect()

#‘动作’运算
maxScore = max(sumScore,key=lambda x: x[1]) #总分最高者
minScore = min(sumScore,key=lambda x: x[1]) #总分最低者
avgScore = [x/count for x in sumSubScore[1]]#单科成绩平均值

#RDD key-value‘转换’运算
subM = lines.reduce(lambda x,y: subMax(x,y))
sumSubScore = lines.reduce(lambda x,y: sumSub(x,y))
redByK = lines.reduceByKey(lambda x,y: [x[i]+y[i] for i in range(3)]).collect() #合并key相同的value值x[0]+y[0],x[1]+y[1],x[2]+y[2]

#RDD'转换'运算
sumPerSore = lines.map(lambda x: sumPer(x)).collect() #每个人的总分 #sumSore = lines.map(lambda x: (x[0],sum(x[1]))).collect()
sorted = lines.sortBy(lambda x: sum(x[1])) #总成绩低到高的学生成绩排序
sortedWithRank = sorted.zipWithIndex().collect()#按总分排序
first3 = sorted.takeOrdered(3,key=lambda x:-sum(x[1])) #总分前三者


#限定以空格的形式输出到文件中
first3RDD = sc.parallelize(first3)\
.map(lambda x:str(x[0])+' '+str(x[1][0])+' '+str(x[1][1])+' '+str(x[1][2]))
.saveAsTextFile("result")
