#!/ifs4/ISDC_BD/software/python/Python-2.7.12/bin/python
# Copyright (C) 2013 Ion Torrent Systems, Inc. All Rights Reserved

import sys
import os
import subprocess
import gzip
import time
from optparse import OptionParser

def printtime(message, *args):
    if args:
        message = message % args
    print "[ " + time.strftime('%X') + " ] " + message
    sys.stdout.flush()
    sys.stderr.flush()

def RunCommand(command,description):
    printtime(' ')
    printtime('Task    : ' + description)
    printtime('Command : ' + command)
    printtime(' ')
    stat = subprocess.call(command,shell=True)
    if stat != 0:
        printtime('ERROR: command failed with status %d' % stat)
        sys.exit(1)

def main():
    
    parser = OptionParser()
    parser.add_option('-i', '--input-vcf',      help='Input vcf file to be sorted', dest='input') 
    parser.add_option('-o', '--output-vcf',     help='Filename of output sorted vcf', dest='output')
    parser.add_option('-t', '--tmp_dir',     help='When input vcf file bigger than 3G, please set it', dest='tmp_dir')
    parser.add_option('-r', '--index-fai',      help='Reference genome index for chromosome order', dest='index') 
    parser.add_option('-g', '--bgzip-compress',  action="store_true",   help='Generate compressed vcf', dest='bgzip')
    (options, args) = parser.parse_args()

    if options.input is None:
        parser.print_help()
        sys.stderr.write('[sort_vcf.py] Error: --input-vcf not specified\n')
        return 1
    if options.output is None:
        sys.stderr.write('[sort_vcf.py] Error: --output-vcf not specified\n')
        return 1
    if options.index is None:
        sys.stderr.write('[sort_vcf.py] Error: --index-fai not specified\n')
        return 1

    vcfsize = os.path.getsize(options.input)
    if vcfsize/1024/1024/1024 > 10:
            if options.tmp_dir is None:
                options.tmp_dir = "./vcfsort_tmp" 

    # Step 1: Read index to establish chromosome order
    if options.tmp_dir is not None:
        if not os.path.exists(options.tmp_dir):
            os.makedirs(options.tmp_dir)
    
    chr_order = []
    chr_vcf_entries = {}
    chr_vcf_tmp = {}
    chr_vcf_fh = {}

    index_file = open(options.index,'r')
    for line in index_file:
        if not line:
            continue
        fields = line.split()
        if options.tmp_dir is not None:
            chr_vcf_tmp[fields[0]] = os.path.join(options.tmp_dir, fields[0] + '.vcf.tmp')
            chr_vcf_fh[fields[0]] = open(chr_vcf_tmp[fields[0]],'w')

        chr_order.append(fields[0])
        chr_vcf_entries[fields[0]] = []
    index_file.close()
    print 'Chromosome order: ' + ' '.join(chr_order)

    input_vcf = options.input
    output_vcf = options.output

    if os.path.splitext(input_vcf)[1] == '.gz':	
        input_file = gzip.open(input_vcf,'rt')		
    else:
        input_file = open(input_vcf,'r')

    output_file = open(output_vcf,'w')
    
    for line in input_file:
        if not line:
            continue
        if line[0] == '#':
            output_file.write(line)
            continue
        fields = line.split()
        if options.tmp_dir is not None:
            chr_vcf_fh[fields[0]].writelines(line)
        else:
            chr_vcf_entries[fields[0]].append((int(fields[1]),line))
    
    input_file.close()
    if options.tmp_dir is not None:
        for chr in chr_order:
            chr_vcf_fh[chr].close()
            tmp = open(chr_vcf_tmp[chr],'r')
            chr_list = []
            for line in tmp:
                fields = line.split()
                chr_list.append((int(fields[1]),line))
            chr_list.sort()
            output_file.writelines([line for idx,line in chr_list])
            tmp.close()
    else:
        for chr in chr_order:
            chr_vcf_entries[chr].sort()
            output_file.writelines([line for idx,line in chr_vcf_entries[chr]])    
    
    output_file.close()

    if options.bgzip :
        RunCommand('bgzip   -c "%s"   > "%s.gz"' % (output_vcf,output_vcf), 'Generate compressed vcf')
        RunCommand('rm "%s"' % output_vcf, 'Remove uncompressed vcf')
    if options.tmp_dir is not None:
        RunCommand('rm -rf "%s"' % options.tmp_dir, 'Remove tmp dir')

if __name__ == '__main__':
    sys.exit(main())
