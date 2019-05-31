#!/usr/bin/env python

import sys,os,re
import subprocess
import time
import gzip
from optparse import OptionParser


def _makedirs(loc):                                                                     
    try:
        os.makedirs(loc)
    except OSError, e:
        if not os.path.isdir(loc):
            raise e

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

def check_permission(path):
    permission = False
    testfile = os.path.join(path,"test.txt")

    try:
        txt = open(testfile,"w")
        txt.write("test")
        txt.close()
        permission = True
    except:
        permission = False
    finally:
        if os.path.exists(testfile):
            os.remove(testfile)

    return permission

def check_vcf_type(vcf):
    p = subprocess.Popen("htsfile %s | cut -f2" % vcf, shell=True,stdout=subprocess.PIPE)
    vcf_dir = os.path.dirname(vcf)
    vcf_out = vcf

    (out,err) = p.communicate()
    if re.search(r'variant calling text',out):
        vcf_out = "%s.gz" % vcf
        RunCommand('bgzip   -c "%s"   > "%s"' % (vcf,vcf_out), 'Generate compressed vcf')
        #RunCommand('rm "%s"' % vcf, 'Remove uncompressed vcf')
    elif re.search(r'gzip-compressed variant calling data',out):
        RunCommand('zcat "%s"   > "%s"' % (vcf,os.path.join(vcf_dir,'tmpfile.vcf')), 'zcat gzip-compressed vcf')
        RunCommand('mv -f "%s" "%s"' % (os.path.join(vcf_dir,'tmpfile.vcf'),vcf), 'mv BGZF-compressed tmpfile to vcf')
    elif not re.search(r'BGZF-compressed variant calling data',out):
        printtime('"Invalid test VCF file: %s'% vcf )
        sys.exit(1)
    if not os.path.exists(vcf_out + '.tbi'):
        RunCommand('tabix   -p vcf   "%s"' % (vcf_out), 'Generate index for compressed vcf')

    return vcf_out

def chech_refvcf(vcf):
    p = subprocess.Popen("htsfile %s | cut -f2" % vcf, shell=True)
    out = p.communicate()
    if re.search(r'BGZF-compressed variant calling data',out):
        return True
    else:
        return False

def get_default_tempdir(outdir):
    abs_outdir = os.path.abspath(outdir)
    outdir_prefix = os.path.dirname(abs_outdir)
    outdir_name = os.path.basename(abs_outdir)
    tempdir = os.path.join(outdir_prefix,'TEMP_%s' % outdir_name)
    return tempdir

def main():
    
    parser = OptionParser()
    parser.add_option('-i', '--test-vcf',   help='Input test VCF file.', dest='test_vcf',metavar="FILE")
    parser.add_option('-r', '--ref-vcf',    help='Input benchmark VCF file,must be compressed(BGZF-compressed) and indexed with tabix(.tbi)', dest='ref_vcf',metavar="FILE")
    parser.add_option('-t', '--template',   help='SDF of the reference genome the variants are called against', dest='sdf',metavar="SDF")
    parser.add_option('-o', '--output-dir', help='Output directory (default: ./out)', dest='outdir', default='./out')
    parser.add_option('--test-bed',         help='An optional BED file whose coordinates will be used to constraint the comparison (optional)', dest='test_bed',metavar="FILE")    
    parser.add_option('--ref-bed',          help='An optional BED file whose coordinates will be used to constraint the comparison (optional)', dest='ref_bed',metavar="FILE")    
    parser.add_option('--temp-dir', help='When the test-vcf\'s parent dir is Permission denied to write, will compress and index test-vcf in this tempdir. (default: OUTDIR/../TEMP_OUTDIR)', dest='tempdir')
    parser.add_option('--all-records',  action="store_true",   help='use all records regardless of FILTER status (Default is to only process records where FILTER is "." or "PASS")',dest='all_records')
    parser.add_option('--squash-ploidy',  action="store_true",   help='treat heterozygous genotypes as homozygous ALT in both baseline and calls, to allow matches that ignore zygosity differences',dest='squash_ploidy')
    #parser.add_option('--ref-overlap',  action="store_true",   help='allow alleles to overlap where bases of either allele are same-as-ref (Default is to only allow VCF anchor base overlap)',dest='ref_overlap')
    parser.add_option('--sample', help='the name of the sample to select. Use <baseline_sample>,<calls_sample> to select different sample names for baseline and calls.(Required when using multi-sample VCF files)', dest='sample',metavar="STRING")
    #parser.add_option('-m', '--output-mode', help='output reporting mode (Must be one of [split, annotate, combine]) (Default is split)', dest='output_mode',metavar="STRING")
    #parser.add_option('-O', '--sort-order', help='the order in which to sort the ROC scores so that "good" scores come before "bad" scores (Must be one of [ascending,descending]) (Default is descending)', dest='sort_order',metavar="STRING")
    parser.add_option('-f', '--vcf-score-field', help='the name of the VCF FORMAT field to use as the ROC score. Also valid are "QUAL" or "INFO=<name>" to select the named VCF INFO field (Default is QUAL)', dest='vcf_score_field',metavar="STRING", default='QUAL')
    #parser.add_option('-Z', '--no-gzip',  action="store_true",   help='do not gzip the output',dest='gzip')
    
    (options, args) = parser.parse_args()
    eval_commands = "rtg vcfeval"
	
    if not options.test_vcf or not options.ref_vcf:
        parser.print_help()
        exit(1)

    if not options.sdf:
        printtime('ERROR: please set -t argument (SDF), use "rtg format" to generate SDF.') 
        parser.print_help()
        exit(1)
        

    if os.path.exists(options.outdir):
        printtime('"WARNING: outdir already exists (%s).'% options.outdir )
        #exit(1)
    test_vcf = os.path.abspath(options.test_vcf)
    ref_vcf = os.path.abspath(options.ref_vcf)
    
    
    eval_commands += " --template=%s" % options.sdf

    # Check the benchmark VCF for any required compression or indexing
    p = subprocess.Popen("htsfile %s | cut -f2" % ref_vcf, shell=True,stdout=subprocess.PIPE)
    (out,err) = p.communicate()
    if not re.search(r'BGZF-compressed variant calling data',out):
        printtime('"Invalid test VCF file: %s'% ref_vcf )
        parser.print_help()
        exit(1)
    eval_commands += ' --baseline=%s' % ref_vcf

    ## Check the test VCF for any required compression or indexing
    vcf_dir = os.path.dirname(test_vcf)
    perm = check_permission(vcf_dir)
    if perm:
        test_vcf = check_vcf_type(test_vcf)
    else:
        if not options.tempdir:
            #options.tempdir = get_default_tempdir(options.outdir)
            options.tempdir = os.path.join(options.outdir,"temp")
        #if os.path.exists(options.tempdir):
            #RunCommand('rm -r "%s"' % options.tempdir, 'Remove temp dir')
        test_vcf_name = os.path.basename(test_vcf)
        temp_vcf = os.path.join(options.tempdir,test_vcf_name)
        if not os.path.exists(options.tempdir):
            os.makedirs(options.tempdir)
            RunCommand('ln -s %s %s' % (test_vcf, options.tempdir), 'link test vcf to tempdir')
        else:
            if not os.path.exists(temp_vcf):
                RunCommand('ln -s %s %s' % (test_vcf, options.tempdir), 'link test vcf to tempdir')
        test_vcf = check_vcf_type(temp_vcf)
        vcf_dir = options.tempdir
    eval_commands += ' --calls=%s' % test_vcf

    # Handle zero, one or two BED files
    if options.test_bed and options.ref_bed:
        RunCommand('bedtools intersect -a %s -b %s > "%s/intersect.bed"' % (options.test_bed,options.ref_bed, vcf_dir), 'Run bedtools intersect')
        eval_commands += ' --bed-regions="%s/intersect.bed"' % vcf_dir
    elif options.test_bed:
        eval_commands += ' --bed-regions="%s"' % options.test_bed
    elif options.ref_bed:
        eval_commands += ' --bed-regions="%s"' % options.ref_bed

    #addtional arguments
#    if options.gzip:
#        eval_commands += ' --no-gzip'
    if options.all_records:
        eval_commands += ' --all-records'
#    if options.ref_overlap:
#        eval_commands += ' --ref-overlap'
#    if options.output_mode:
#        eval_commands += ' --output_mode=%s' % options.output_mode
#    if options.sort_order:
#        eval_commands += ' --sort_order=%s' % options.sort_order
    if options.vcf_score_field:
        eval_commands += ' --vcf-score-field=%s' % options.vcf_score_field
    if options.sample:
        eval_commands += ' --sample=%s' % options.sample
    

    # Run RTG vcfeval
    eva_outdir = os.path.join(options.outdir,'eva')
    if os.path.exists(eva_outdir):
        RunCommand('rm -r "%s"' % eva_outdir, 'Remove eva_outdir dir')
    eval_commands_eva = eval_commands + " --output=%s" % eva_outdir
    RunCommand(eval_commands_eva, 'Running allele evaluation (rtg vcfeval)...')

    if options.squash_ploidy:
        evasp_outdir = os.path.join(options.outdir,'evasp')
        if os.path.exists(evasp_outdir):
            RunCommand('rm -r "%s"' % evasp_outdir, 'Remove evasp_outdir dir')
        eval_commands_evasp = eval_commands + " --squash-ploidy --output=%s" % evasp_outdir
        RunCommand(eval_commands_evasp, 'Running allele evaluation (rtg vcfeval --squash-ploidy)...')

    # Run RTG vcfstats
    report = os.path.join(options.outdir,'report.txt')
    stats_commands = "rtg vcfstats "
    stats_commands += ' %s' % test_vcf
    stats_commands += ' >%s' % report
    if options.sample:
        pass
    RunCommand(stats_commands, 'Running VCF statistics (rtg vcfstats)...')

    RunCommand("echo '\nrtg vcfeval:'| cat - %s/summary.txt >> %s"% (eva_outdir,report), 'cat summary.txt to report.txt (eva)... ')
    if options.squash_ploidy:
        RunCommand("echo '\nrtg vcfeval --squash-ploidy:'| cat - %s/summary.txt >> %s"% (evasp_outdir,report), 'cat summary.txt to report.txt (evasp)... ')


if __name__ == '__main__':
    os.environ["PATH"] = '/hwfssz1/BIGDATA_COMPUTING/software/bin:'+os.environ["PATH"]
    main()
