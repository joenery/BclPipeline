#!/usr/bin/env python

import os
import sys
import re
import signal
import subprocess
import argparse
from warnings import filterwarnings
import MySQLdb as mdb
from collections import defaultdict


class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

# These will be used as the canonical functions for uploading to the MySQL database
def getAssemblyNameFromSam(chromosome_line):

    if "Chr" in chromosome_line or "chr" in chromosome_line and not "chromosome" in chromosome_line and not "Chromosome" in chromosome_line:
        return chromosome_line.replace("Chr","").replace("chr","")

    elif "chromosome" in chromosome_line and "AGP" in chromosome_line:
        return chromosome_line.split(":")[2]

    else:
        return chromosome_line.replace("Chr","").replace("chr","")

def getChromosomeFiles(sam,tdna_filter=False,remove_clones=False):
    """
    """
    open_files    = {}
    open_files_id = {}

    print("Creating Chromosome Files")
    with open(sam,"r") as sam_chrom_count_object:

        passed_first_chromosome_header = False

        for i,line in enumerate(sam_chrom_count_object):
            row = line.strip().split()

            marker = "@SQ"

            if row[0] != marker and not passed_first_chromosome_header:
                # We are at the beginning of sam file
                continue

            elif row[0] == "@SQ":
                passed_first_chromosome_header = True
                chromosome = row[1].replace("SN:","")

                # Open These Files
                open_files[chromosome]    = open(chromosome.replace(":","_") + ".aj","w")
                open_files_id[chromosome] = 0

            elif row[0] != marker and passed_first_chromosome_header:
                break

            elif i > 10 and not passed_first_chromosome_header:
                # There aren't any headers in the sam file
                # Can't continue
                print("\nError: It looks like either the SAM file you've given doesn't have headers!\n")
                print("Please re-run Import2AnnojSimple with a SAM file that has headers\n")
                sys.exit(1)


    # --------------------- Parsing Sam File Aligns in to respective Chromosome Files ------ #
    print("Parsing Sam file for Alignments")
    with open(sam,"r") as sam_file:
        
        for i,line in enumerate(sam_file):

            # Create a hash of things to skip
            skip_these_lines = set()
            skip_these_lines.add("@HD")
            skip_these_lines.add("@SQ")
            skip_these_lines.add("@PG")
            skip_these_lines.add("*")
            skip_these_lines.add("chloroplast")
            skip_these_lines.add("mitochondira")
            skip_these_lines.add("ChrC")
            skip_these_lines.add("ChrM")

            header = line.strip().split()[0]

            if header in skip_these_lines:
                continue
            
            # Get Variables
            row         = line.strip().strip().split("\t")
            chromosome  = row[2]
            read_start  = row[3]
            snip_string = row[5]
            direction   = row[1]
            sequence    = row[9]

            # Skip unmapped reads 
            if chromosome in skip_these_lines:
                continue

            # From snip string get length of match and create end of read
            match            = re.search("([0-9][0-9](?=M)|[0-9][0-9][0-9](?=M))",snip_string)
            alignment_length = match.group(0)
            read_end         = str( int(read_start) + int(alignment_length) - 1 )

            # Change direction from Sam form to Annoj form
            if direction == "0":
                direction = "+"

            elif direction == "16":
                direction = "-"

            # Write to output
            if chromosome in open_files:
                open_files_id[chromosome] += 1

                count = open_files_id[chromosome]

                # Write The Assembly Chromosome in a way that Annoj Can Handle 
                assembly = getAssemblyNameFromSam(chromosome)

                open_files[chromosome].write("\t".join([str(count),assembly,direction,read_start,read_end,sequence + "\n"]))

        # Close Chromosomes

        for f in open_files:
            open_files[f].close()
        
        # sort Chromosome files by position and direction
        print("Sorting Chromosomes")
        for chrom in open_files:
            file_to_sort = chrom.replace(":","_")
            command = "cat %s | sort -k4,4n -k3,3 > x; mv x %s" % ( str(file_to_sort) + ".aj" , str(file_to_sort) + ".aj" )
            subprocess.call(command,shell = True)

        print("Joining Chromosomes in to all.aj")
        if "all.aj" in os.listdir(os.getcwd()):
            remove_all_aj = "rm all.aj"
            subprocess.call(remove_all_aj,shell=True)

        join_chromosomes_command = "cat *.aj > all.aj"
        subprocess.call(join_chromosomes_command,shell=True)

        if tdna_filter:
            filter_all("all.aj",remove_clones=remove_clones)

def upload2mysql(host,database,tablename,mysql_user,mysql_password,tdna_filter=False,remove_clones=False):
    """
    """
    
    host = host.replace("_","-")

    to_upload = [("all.aj",tablename)]

    if tdna_filter and remove_clones:
        to_upload.append(("all.noclones.filter.aj",tablename + "_tDNA_Filter"))
    elif tdna_filter and not remove_clones:
        to_upload.append(("all.filter.aj",tablename + "_tDNA_Filter"))

    for f in to_upload:
        chrom_file = f[0]
        tablename  = f[1]

        print("Uploading %s" % chrom_file)
        # ------------------------ MySQL Upload --------------------------- #
        
        # Filter those stupid Mysql warnings
        filterwarnings('ignore',category = mdb.Warning)
       
        # Connect to MySQL Database:
        print("\tConnecting to MySQL Database")
        
        try:
            db = mdb.connect(host=host,user = mysql_user,passwd = mysql_password,local_infile = 1)

        except mdb.Error,e:
            print("Error %d: %s") % (e.args[0],e.args[1])
            print("It looks like you gave a host name that didn't exist!")
            sys.exit(1)

        # With connection create an object to send queries
        print("\tConnected. Uploading File.")
        with db:
            cur   = db.cursor()

            query = "create database if not exists %s" % (database)
            cur.execute(query)

            query = "drop table if exists %s.%s" % (database,tablename)
            cur.execute(query)

            query = "create table %s.%s(id INT,assembly VARCHAR(2), strand VARCHAR(1), start INT, end INT, sequenceA VARCHAR(100), sequenceB VARCHAR(100))"% (database,tablename)
            cur.execute(query)

            query = """LOAD DATA LOCAL INFILE '%s' INTO TABLE %s.%s""" % (os.path.realpath(chrom_file),database,tablename)
            cur.execute(query)

            # End of Connection
            cur.close()

        print("\tFinished Uploading")
        print("\tCreating Fetcher and Track Information in Current Working Directory")

        # ---------------------- Creating Fetcher Information ------------------- #
        with open(tablename + ".php","w") as fetcher:
            fetcher.write("<?php\n")
            fetcher.write("$append_assembly = false;\n")
            fetcher.write("$table = '%s.%s';\n" % (database,tablename) )
            fetcher.write("$title = '%s';\n" % (tablename))
            fetcher.write("$info = '%s';\n"  % (tablename.replace("_"," ")))
            fetcher.write("""$link = mysql_connect("%s","mysql","rekce") or die("failed");\n""" % (host))
            fetcher.write("require_once '<PUT RELATIVE PATH TO INCLUDES>/includes/common_reads.php';\n")
            fetcher.write("?>\n")

        # --------------------- Create Track Information ------------------------ #
        with open(tablename + ".trackDefinition","w") as track_def:
            track_def.write("{\n")
            track_def.write(" id: '%s',\n"   % tablename)
            track_def.write(" name: '%s',\n" % tablename)
            track_def.write(" type: 'ReadsTrack',\n")
            track_def.write(" path: 'NA',\n")
            track_def.write(" data: '<INSERT RELATIVE PATH TO FETCHER>/%s',\n" % (tablename + ".php"))
            track_def.write(" height: '25', \n")
            track_def.write(" scale: 0.1\n")
            track_def.write("},\n")

def filter_all(input_file,window_size=200,min_reads=5,remove_clones=False):

    # Remove Clones
    # Compare everything after R
    # If the bp's 11 - 53 are the same then it is a clone
    # Since all.aj is already sorted by chrom, start,end this is a pretty safe
    # bet

    if remove_clones:
        print("\tRemoving Clones")
        remove_clones = "cat %s | uniq -f 5 -s 10 -w53 > all.noclones.aj" % (input_file)
        subprocess.call(remove_clones,shell=True)

        aj_file_to_parse = "all.noclones.aj"
        aj_output        = "all.noclones.filter.aj"
    else:
        aj_file_to_parse = input_file
        aj_output        = "all.filter.aj"

    # Use Window Technique to mow through clones.
    with open(aj_output,"w") as output_file:
        with open(aj_file_to_parse,"r") as input_file:
            """
            """
            print("\tFiltering")

            peaks = defaultdict(dict)
            temp = []
            prev_bin = None

            for line in input_file:
                row = line.split()
                chrom = row[1]

                # Get Position we care about
                if row[2] == "-":
                    num = int(row[3])
                else:
                    num = int(row[4])

                # What bin are we in?
                bin = num/window_size

                # Is Bin the Same as prev bin?
                if bin == prev_bin:
                    temp.append(line)

                else:
                    if len(temp) >= min_reads:
                        output_file.write("".join(temp))
                        peaks[chrom][bin] = len(temp)

                    temp = [line]

                prev_bin = bin

    # Print Out the Hit counts
    with open("hit.count","w") as hits:

        count = 0

        for chrom in peaks.keys():
            count += len(peaks[chrom])

        hits.write("Hits:%s\n" % (count))
        hits.write("Hits signify the amount of bins the size of the window (%s basepairs) that have more than %s reads\n" %(window_size,min_reads))


if __name__ == "__main__":
    """
    Import2AnnojSimple
    """
    
    # Makes playing with Unix nicer. Mostly a hold over from using STDIN
    signal.signal(signal.SIGPIPE,signal.SIG_DFL)

    # Configure Arguement Parser
    parser = MyParser(description = "Take a Sam file and put it into a database of your choice.\
                                     A track definition and fetcher are created in your Current Working Directory\
                                     for use in your Annoj Setup.")

    mandatory = parser.add_argument_group("MANDATORY")
    advanced  = parser.add_argument_group("ADVANCED -> Ninjas or Jedi only!")

    mandatory.add_argument("-i","--input",           help = "Sam file you'd like to put in database")
    mandatory.add_argument("-ho","--host",           help = "This is the mysql host you'd like to put your data on. Eg - thumper-e3, thumper-e4, etc...")
    mandatory.add_argument("-db","--database",       help = "What is the name of the database you'd like to put your data in. If it does not exist it will be created for you.")
    mandatory.add_argument("-t","--tablename",       help = "The name of the table you'd like to call this data")
    
    advanced.add_argument("-mu","--mysql-user",      help = "The mysql user you would like to login in as.  DEFAULT: mysql",      default="mysql")
    advanced.add_argument("-pw","--mysql-password",  help = "The corresponding password for the MySQL user. DEFAULT: rekce",      default="rekce")
    advanced.add_argument("-s","--store-flat-files", help = "Keep the flat chromosome files that are created. DEFAULT: false",    action="store_true")
    advanced.add_argument("-su","--skip-to-upload",  help = "If all.aj has already been created. Skip to Upload. DEFAULT: false", action="store_true")
    advanced.add_argument("-tf","--tdna-filter",     help = "If this sample is tdna",                                             action="store_true")
    advanced.add_argument("-rc","--remove-clones",   help = "Remove clones in the TDNA Filter step",                              action="store_true")

    # Get Command Line Options
    command_line_options = vars(parser.parse_args())

    database       = command_line_options["database"]
    host           = command_line_options["host"]
    input_file     = command_line_options["input"]
    tablename      = command_line_options["tablename"]
    mysql_user     = command_line_options["mysql_user"]
    mysql_password = command_line_options["mysql_password"]
    store_flat_files = command_line_options["store_flat_files"]
    skip_to_upload   = command_line_options["skip_to_upload"]
    tdna_filter      = command_line_options["tdna_filter"]
    remove_clones    = command_line_options["remove_clones"]


    # Make sure all commands are present
    if not database or not host or not input_file or not tablename:
        print("All options must be given")
        parser.print_help()
        sys.exit(1)

    # SCRIPT!
    try:
        #local2mysql(input_file,host,database,tablename,mysql_user,mysql_password,skip_to_upload=skip_to_upload)
        if not skip_to_upload:
            getChromosomeFiles(input_file,tdna_filter=tdna_filter,remove_clones=remove_clones)
        
        upload2mysql(host,database,tablename,mysql_user,mysql_password,tdna_filter=tdna_filter,remove_clones=remove_clones)

    except IOError:
        # User has given a file name / path that is not correct
        print("It looks like --> %s <-- doesn't exist!") % (input_file)
        sys.exit(1)

    if not store_flat_files:
        print("Finished Upload and Cleaning up Directory")
        clean_up_directory = ["rm","*.aj"]
        subprocess.call(" ".join(clean_up_directory),shell=True)
