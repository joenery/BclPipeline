#!/usr/bin/env python

import os
import sys
import argparse
import MySQLdb as mdb
import re
import signal
from warnings import filterwarnings
import subprocess

# My Modules
from import2annojsimple import local2mysql
from bowtie import bowtie_folder

class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

def get_folder_paths(project):
    """
    Creates a list of "Sample" folders from the Unaligned/Project/ folder specified
    """
    return [project + "/" + x for x in os.listdir(project) if os.path.isdir(project + "/" +x)]

if __name__=="__main__":
    # Makes playing with Unix nicer. Mostly a hold over from using STDIN
    signal.signal(signal.SIGPIPE,signal.SIG_DFL)

    # Configure Arguement Parser
    parser = MyParser(description = "Given a Project Folder this script will Bowtie check each folder for Fastq's.\
                                     If there are Fastq's then Bowtie2 will be performed.\
                                     Additionally, a fetcher and Track Definition are created. ")

    parser.add_argument("-p","--project", help = "Absolute path to a Project Folder")
    parser.add_argument("-ho","--host",help = "This is the mysql host you'd like to put your data on. Eg - thumper-e3, thumper-e4, etc...")
    parser.add_argument("-db","--database",help = "What is the name of the database you'd like to put your data in. If it does not exist it will be created for you.")
    parser.add_argument("-i","--bowtie-indexes",help="Path to the Bowtie Indexes.",default="/home/seq/bin/bowtie2/INDEXES/tair10")
    parser.add_argument("-o","--bowtie-only",help="Only Bowtie will be performed",action="store_true")

    # Get Command Line Options
    command_line_options = vars(parser.parse_args())

    database   = command_line_options["database"]
    host       = command_line_options["host"]
    project    = command_line_options["project"]
    bowtie_indexes = command_line_options["bowtie_indexes"]
    bowtie_only = command_line_options["bowtie_only"]


    # -------------------- BOWTIE ARGUMENTS --------------------- #

    path_to_bowtie = "bowtie2"
    bowtie_options = "--local -p 8"
    indexes_genome = os.path.basename(bowtie_indexes)
    indexes_folder = os.path.split(bowtie_indexes)[0]

    # --------------------- MySQL USER and PASSWORD -------------- #

    mysql_user     = "mysql"
    mysql_password = "rekce"

    # ------------------------------------------------------------ #

    if not database or not host or not project:
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(project):
        print
        print("%s is not a folder") % (project)
        print
        sys.exit(1)

    # Main Script
    folders = get_folder_paths(project)
    
    for folder in folders:
        print("Working on %s" % (folder)) 

        current_folder = folder

        os.chdir(current_folder)

        # Get Folder name for Table Name
        basename  = os.path.basename(current_folder).replace("Sample_","")
        tablename = basename

        # Run Bowtie
        bowtie_folder(os.getcwd(),indexes_folder=indexes_folder,indexes_genome=indexes_genome)

        # Run local2mysql
        if not bowtie_only:
            print("Running Import to Annoj")
            create_annoj_folder = "mkdir annoj"
            subprocess.call(create_annoj_folder,shell=True)

            os.chdir(current_folder + "/annoj")

            # We've moved one folder down and the path to the file reflects this
            local2mysql("../bowtie2.out.sam",host,database,tablename,mysql_user,mysql_password)


# Deprocated
# def local2mysql(sam_file,host,database,tablename,mysql_user,mysql_password,genome):
    
#     # assume the files are straight out of Bowtie 2 with no options and they contain unmapped reads

#     # ----------------- Create the chromosome files here ----------------------- #

#     number_of_chromosomes = genome["number_of_chromosomes"]
    
#     print("Creating AJ Files")

#     # Use a Dictionary to keep track of the Output objects and the ID counts for each one
#     open_files    = {str(f):open(str(f) + ".aj","w") for f in range(1,number_of_chromosomes + 1)}
#     open_files_id = {str(f):0 for f in range(1,number_of_chromosomes + 1)} 


#     for i,line in enumerate(sam_file):

#         # Create a hash of things to skip
#         skip_these_lines = set()
#         skip_these_lines.add("@HD")
#         skip_these_lines.add("@SQ")
#         skip_these_lines.add("@PG")
#         skip_these_lines.add("*")
#         skip_these_lines.add("chloroplast")
#         skip_these_lines.add("mitochondira")
#         skip_these_lines.add("ChrC")
#         skip_these_lines.add("ChrM")

#         header = line.strip().split()[0]

#         if header in skip_these_lines:
#             continue

#         # Get Variables
#         row         = line.strip().split()
#         readID      = row[0]                                     # This can also be a header!
#         chromosome  = row[2].replace("Chr","").replace("chr","")
#         read_start  = row[3]
#         snip_string = row[5]
#         direction   = row[1]
#         sequence    = row[9]

#         # Skip unmapped reads or bullshit headers
#         if readID in skip_these_lines or chromosome in skip_these_lines:
#             continue

#         # From snip string get length of match and create end of read
#         match            = re.search("([0-9][0-9](?=M)|[0-9][0-9][0-9](?=M))",snip_string)
#         alignment_length = match.group(0)
#         read_end         = str( int(read_start) + int(alignment_length) - 1 )

#         # Change direction from Sam form to Annoj form
#         if direction == "0":
#             direction = "+"

#         elif direction == "16":
#             direction = "-"

#         # Write to output
#         if chromosome in open_files:
#             open_files_id[chromosome] += 1

#             count = open_files_id[chromosome]

#             open_files[chromosome].write("\t".join([str(count),chromosome,direction,read_start,read_end,sequence + "\n"]))

#     # Close Chromosomes

#     for f in open_files:
#         open_files[f].close()
    
#     # sort Chromosome files by position and direction
#     print("Sorting Chromosomes")
#     for i in range(1,number_of_chromosomes + 1):
#         command = "cat %s | sort -k3,3n -k1,1n -k3,3 > x; mv x %s" % ( str(i) + ".aj" , str(i) + ".aj" )
#         subprocess.call(command,shell = True)

    
#     # ------------------------ MySQL Upload --------------------------- #
    
#     # Filter those stupid Mysql warnings
#     filterwarnings('ignore',category = mdb.Warning)
   
#     # Connect to MySQL Database:
#     print("Connecting to MySQL Database")
    
#     try:
#         db = mdb.connect(host=host,user = mysql_user,passwd = mysql_password,local_infile = 1)

#     except mdb.Error,e:
#         print("Error %d: %s") % (e.args[0],e.args[1])
#         print("It looks like you gave a host name that didn't exist!")
#         sys.exit(1)

#     # With connection create an object to send queries
#     with db:
#         cur   = db.cursor()

#         query = "create database if not exists %s" % (database)
#         cur.execute(query)

#         for i in range(1,number_of_chromosomes + 1):
#             chrom_file = str(i) + ".aj"

#             query = "drop table if exists %s.reads_%s_%d" % (database,tablename,i)
#             cur.execute(query)

#             query = "create table %s.reads_%s_%d(id INT,assembly VARCHAR(2), strand VARCHAR(1), start INT, end INT, sequenceA VARCHAR(100), sequenceB VARCHAR(100))"% (database,tablename,i)
#             cur.execute(query)

#             query = """LOAD DATA LOCAL INFILE '%s' INTO TABLE %s.reads_%s_%d""" % (os.path.realpath(chrom_file),database,tablename,i)
#             cur.execute(query)

#         cur.close()

#     print("Finished Uploading")

#     # ---------------------- Creating Fetcher Information ------------------- #
#     with open(tablename + ".php","w") as fetcher:
#         fetcher.write("<?php\n")
#         fetcher.write("$append_assembly = true;\n")
#         fetcher.write("$table = '%s.reads_%s_';\n" % (database,tablename) )
#         fetcher.write("$title = '%s';\n" % (tablename))
#         fetcher.write("$info = '%s';\n"  % (tablename.replace("_"," ")))
#         fetcher.write("""$link = mysql_connect("%s","mysql","rekce") or die("failed");\n""" % (host))
#         fetcher.write("require_once '<PUT RELATIVE PATH TO INCLUDES FOLDER>/includes/common_reads.php';\n")
#         fetcher.write("?>\n")

#     # --------------------- Create Track Information ------------------------ #
#     with open(tablename + ".trackDefinition","w") as track_def:
#         track_def.write("{\n")
#         track_def.write(" id: '%s',\n"   % tablename)
#         track_def.write(" name: '%s',\n" % tablename)
#         track_def.write(" type: 'ReadsTrack',\n")
#         track_def.write(" path: 'NA',\n")
#         track_def.write(" data: '<INSERT RELATIVE PATH TO FETCHER>/%s',\n" % (tablename + ".php"))
#         track_def.write(" height: '90', \n")
#         track_def.write(" scale: 0.03\n")
#         track_def.write("},\n")
