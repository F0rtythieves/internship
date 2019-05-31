from pyspark import SparkContext
from pyspark import SparkConf
#Judge whether it is transition or transversion
def titv(x,y):
    if((x=='A' or x=='G') and (y=='C' or y=='T'))or((x=='C' or x=='T') and (y=='A' or y=='G')):
        return 1
    if(x=='A' and y=='G')or(x=='C' and y=='T')or(x=='G' and y=='A')or(x=='T' and y=='C'):
        return 0

sc=SparkContext("local","Simple App")
#readfile
textFile = sc.textFile("/home/wangjie/HG00308.hc.vcf")
#total variation
all=textFile.filter(lambda x: '#' not in x)
all_num=all.count()
#clear tab
snpcl=all.map(lambda line: line.split('\t'))
#snp
snp=snpcl.filter(lambda g : len(g[3])==len(g[4]))
snp_num=snp.count()
#insertion
Insertion=snpcl.filter(lambda g : len(g[3])<len(g[4]))
Insertion_num=Insertion.count()
#deletion
Deletion=snpcl.filter(lambda g : len(g[3])>len(g[4]))
Deletion_num=Deletion.count()
#10 column and split
test=snpcl.map(lambda line : line[9].split(':'))
#het in GT
het0=test.filter(lambda line : line[0]=="0/1")
het0_num=het0.count()
het1=test.filter(lambda line : line[0]=="1/2")
het1_num=het1.count()
#hom in GT
hom=test.filter(lambda line : line[0]=="1/1")
hom_num=hom.count()
#total odd
odd=float(het1_num+het0_num)/float(hom_num)
#titv in snp
ti=snp.filter(lambda g : titv(g[3],g[4])==0)
ti_num=ti.count()
tv=snp.filter(lambda g : titv(g[3],g[4])==1)
tv_num=tv.count()
titvodd=float(ti_num)/float(tv_num)
#snp het/hom
snp10=snp.map(lambda line : line[9].split(':'))
snphet0=snp10.filter(lambda line : line[0]=="0/1")
snphet0_num=snphet0.count()
snphet1=snp10.filter(lambda line : line[0]=="1/2")
snphet1_num=snphet1.count()
snphet_num=snphet0_num+snphet1_num
snphom=snp10.filter(lambda line : line[0]=="1/1")
snphom_num=snphom.count()
snpodd=float(snphet_num)/float(snphom_num)
#Insertion Het/Hom ratio
in10=Insertion.map(lambda line : line[9].split(':'))
inhet0=in10.filter(lambda line : line[0]=="0/1")
inhet0_num=inhet0.count()
inhet1=in10.filter(lambda line : line[0]=="1/2")
inhet1_num=inhet1.count()
inhet_num=inhet0_num+inhet1_num
inhom=in10.filter(lambda line : line[0]=="1/1")
inhom_num=inhom.count()
inodd=float(inhet_num)/float(inhom_num)
#Deletion Het/Hom ratio
del10=Deletion.map(lambda line : line[9].split(':'))
delhet0=del10.filter(lambda line : line[0]=="0/1")
delhet0_num=delhet0.count()
delhet1=del10.filter(lambda line : line[0]=="1/2")
delhet1_num=delhet1.count()
delhet_num=delhet0_num+delhet1_num
delhom=del10.filter(lambda line : line[0]=="1/1")
delhom_num=delhom.count()
delodd=float(delhet_num)/float(delhom_num)


with open('test.txt', 'w') as f:
    print>>f,'Total\t:%d\nSNPs\t:%d\nInsertions\t:%d\nDeletions\t:%d\nSNP Transitions/Transversions\t:%f\n(%d,%d)\nTotal Het/Hom ratio\t:%f\n(%d,%d)\nSNP Het/Hom ratio\t:%f\n(%d,%d)\nInsertion Het/Hom ratio\t:%f\n(%d,%d)\nDeletion Het/Hom ratio\t:%f\n(%d,%d)\n' %(all_num,snp_num,Insertion_num,Deletion_num,titvodd,ti_num,tv_num,odd,(het0_num+het1_num),hom_num,snpodd,snphet_num,snphom_num,inodd,inhet_num,inhom_num,delodd,delhet_num,delhom_num)