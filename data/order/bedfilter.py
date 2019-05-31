#!/ifs5/ST_TRANS_CARDIO/PUB/bin/python

import sys
import os
import subprocess
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

def BedFilter(input_vcf,output_vcf,bedfile):
        bedfilter_command  =    'vcftools'
        if os.path.splitext(input_vcf) == '.gz':		
            bedfilter_command +=    '   --gzvcf %s' % input_vcf
        else:
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
    parser.add_option('-o', '--output-vcf',     help='Output limit variant calling to regions in bedfile(default: variants.vcf)', dest='output', default='variants.vcf')
    parser.add_option('-g', '--bgzip-compress',  action="store_true",   help='Generate compressed vcf', dest='bgzip')
    
    (options, args) = parser.parse_args()
	
    if not options.vcffile :
        parser.print_help()
        exit(1)

    BedFilter(vcffile,options.output,bedfile)
   
    if options.bgzip :
        RunCommand('bgzip   -c "%s"   > "%s.gz"' % (options.output,options.output), 'Generate compressed vcf')
        RunCommand('rm "%s"' % (options.output), 'Remove uncompressed vcf')
         #   RunCommand('tabix   -p vcf   "%s.gz"' % (vcfout), 'Generate index for compressed vcf')

if __name__ == '__main__':
    main()
