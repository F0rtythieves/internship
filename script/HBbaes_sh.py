import re
file=open("HG00308.hc.vcf", "r")
i=1
with open("result",'w') as file1:
    file1.write('create \'HG00308\',\'data\'\n')
    while True:
        line=file.readline()
        if line:
            line=line.strip('\n')
            if line[0]!="#":
                m=re.split("\t",line)
                file1.write('put \'HG00308\',\'%d\',\'data:CHROM\',\'%s\'\n' % (i,m[0]))
                file1.write('put \'HG00308\',\'%d\',\'data:POS\',\'%s\'\n' % (i,m[1]))
                file1.write('put \'HG00308\',\'%d\',\'data:ID\',\'%s\'\n' % (i,m[2]))
                file1.write('put \'HG00308\',\'%d\',\'data:REF\',\'%s\'\n' % (i,m[3]))
                file1.write('put \'HG00308\',\'%d\',\'data:ALT\',\'%s\'\n' % (i,m[4]))
                file1.write('put \'HG00308\',\'%d\',\'data:QUAL\',\'%s\'\n' % (i,m[5]))
                file1.write('put \'HG00308\',\'%d\',\'data:FILTER\',\'%s\'\n' % (i,m[6]))
                file1.write('put \'HG00308\',\'%d\',\'data:INFO\',\'%s\'\n' % (i,m[7]))
                file1.write('put \'HG00308\',\'%d\',\'data:FORMAT\',\'%s\'\n' % (i,m[8]))
                file1.write('put \'HG00308\',\'%d\',\'data:HG00308\',\'%s\'\n' % (i,m[9]))
                i=i+1
        else:
            break
