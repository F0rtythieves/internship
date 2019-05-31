from pyspark import SparkContext
from pyspark import SparkConf
import argparse

#初始传递命令
parser = argparse.ArgumentParser(add_help = False, usage = '\npython3 get_fastq_cg.py -i [fastq] -o [txt]\n用于提取 fastq 中的信息')
required = parser.add_argument_group()
optional = parser.add_argument_group()
required.add_argument('-i', '--input', metavar = '[fastq]', help = '输入文件，fastq 文件', required = True)
required.add_argument('-o', '--output', metavar = '[txt]', help = '输出文件，txt文件', required = True)
optional.add_argument('-h', '--help', action = 'help', help = '帮助信息')
args = parser.parse_args()
#取出fastq文件中的序列
def getseq(b):
    import re
    pattern=re.compile(r'[^ATCG]')
    a=pattern.findall(b)
    if a:
        return False
    else:
        return True

#计算每条序列中的CG含量
def count(a):
    sum=0
    sum+=a.count('C')+a.count('G')
    return float(sum)/len(a)

sc=SparkContext("local","Simple App")
#readfile
textFile = sc.textFile(args.input)
#获得每个单位的名称的RDD
raw_index=textFile.filter(lambda x: x[0]=='@' )
index=raw_index.map(lambda a:a[0:24])#提取DPxxxxxxxxxxxxLxCxxxRxxx
#获得每个单位的CG含量的RDD
seq=textFile.filter(getseq)
cg=seq.map(count)
#将二者合成一个RDD
result=index.zip(cg)
#按键值计算平均值
avg_by_key = result \
    .mapValues(lambda v: (v, 1)) \
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
    .mapValues(lambda v: v[0]/v[1]) \
    .collectAsMap()

with open(args.output,'w') as f:
    f.write("unit"+ '\t'+ "GC" + '\n')
    for i in avg_by_key:
        f.write(i+ "\t" + str(avg_by_key[i]) +'\n' )
#print(avg_by_key)
#result.saveAsTextFile('/home/wangjie/wj')