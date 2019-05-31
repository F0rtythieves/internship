#!/ifs5/ST_TRANS_CARDIO/PUB/bin/python

import sys
import os
import subprocess
import time
import gzip
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


def SplitVcf(input_vcf,output_snp_vcf,output_indel_vcf):
    
    if os.path.splitext(input_vcf)[1] == '.gz':	
         input = gzip.open(input_vcf,'rt')		
    else:
         input = open(input_vcf,'r')
    output_snp = open(output_snp_vcf,'w')
    output_indel = open(output_indel_vcf,'w')
    
    for line in input:
        if not line or line[0]=='#':
            output_snp.write(line)
            output_indel.write(line)
            continue
        
        fields = line.split('\t')
        ref = fields[3]
        alt = fields[4].split(',')[0]
        
        if alt == '.':			
            continue
        elif len(alt) == len(ref):
            output_snp.write(line)
        else:
            output_indel.write(line)
            
    input.close()
    output_snp.close()
    output_indel.close()

def LimitVcf(input_vcf,output_vcf,bedfile):
        bedfilter_command  =    'vcftools'
        bedfilter_command +=    '   --vcf %s' % input_vcf
        bedfilter_command +=    '   --bed %s' % bedfile
        bedfilter_command +=    '   --out %s' % output_vcf
        bedfilter_command +=    '   --recode  --keep-INFO-all'
        #bedfilter_command +=    ' > /dev/null'
        RunCommand(bedfilter_command, 'Filter merged VCF using region BED')

def main():
    
    parser = OptionParser()
    parser.add_option('-i', '--vcf',       help='Input VCF file (optional)', dest='vcffile')
    parser.add_option('-b', '--region-bed',       help='Limit variant calling to regions in this BED file (optional)', dest='bedfile')    
    parser.add_option('-f', '--output-limit',     help='Output limit variant calling to regions in bedfile(default: variants.vcf)', dest='indel_out', default='variants.vcf')
    parser.add_option('-o', '--output-dir',       help='Output directory (default: current)', dest='outdir', default='.')
    parser.add_option('-s', '--output-snp',       help='Output SNP vcf(default: SNP_variants.vcf)', dest='snp_out', default='SNP_variants.vcf')
    parser.add_option('-d', '--output-indel',     help='Output InDel vcf(default: indel_variants.vcf)', dest='indel_out', default='indel_variants.vcf')
    parser.add_option('-g', '--bgzip-compress',  action="store_true",   help='Generate compressed vcf', dest='bgzip')
    
    (options, args) = parser.parse_args()
	
    if not options.vcffile :
        parser.print_help()
        exit(1)

    if not os.path.exists(options.outdir):
        os.makedirs(options.outdir)
   
    snp_out = '%s/%s' % (options.outdir,options.snp_out)
    indel_out = '%s/%s' % (options.outdir,options.indel_out)
	
    SplitVcf(options.vcffile , snp_out ,indel_out)

    if options.bgzip :
        RunCommand('bgzip   -c "%s"   > "%s.gz"' % (snp_out,snp_out), 'Generate compressed vcf')
        RunCommand('bgzip   -c "%s"   > "%s.gz"' % (indel_out,indel_out), 'Generate compressed vcf')
        RunCommand('rm "%s" "%s"' % (snp_out,indel_out), 'Remove uncompressed vcf')
         #   RunCommand('tabix   -p vcf   "%s.gz"' % (vcfout), 'Generate index for compressed vcf')

if __name__ == '__main__':
    main()
