#!/usr/bin/python -u

import os
import sys
import argparse
import MySQLdb as mdb
import re
import signal
from warnings import filterwarnings
import subprocess

class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

def local2mysql(sam_file,host,database,tablename,mysql_user,mysql_password):
    
    # assume the files are straight out of Bowtie 2 with no options and they contain unmapped reads

    # ----------------- Create the chromosome files here ----------------------- #
    
    print("Creating AJ Files")
    chromosome1 = open("1.aj","w")
    chromosome2 = open("2.aj","w")
    chromosome3 = open("3.aj","w")
    chromosome4 = open("4.aj","w")
    chromosome5 = open("5.aj","w")

    for i,line in enumerate(sam_file):

        # Skip the headers
        if i < 9:
            continue

        # Get Variables
        row         = line.strip().split()
        chromosome  = row[2].replace("Chr","").replace("chr","")
        read_start  = row[3]
        snip_string = row[5]
        direction   = row[1]
        sequence    = row[9]


        # Skip unmapped reads 
        if chromosome in ["*","chloroplast","mitochondira","ChrC","ChrM"] :
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
        if   chromosome == "1":
            chromosome1.write("\t".join([chromosome,direction,read_start,read_end,sequence + "\n"]))

        elif chromosome == "2":
            chromosome2.write("\t".join([chromosome,direction,read_start,read_end,sequence + "\n"]))

        elif chromosome == "3":
            chromosome3.write("\t".join([chromosome,direction,read_start,read_end,sequence + "\n"]))

        elif chromosome == "4":
            chromosome4.write("\t".join([chromosome,direction,read_start,read_end,sequence + "\n"]))

        elif chromosome == "5":
            chromosome5.write("\t".join([chromosome,direction,read_start,read_end,sequence + "\n"]))

    # Close Chromosomes
    chromosome1.close()
    chromosome2.close()
    chromosome3.close()
    chromosome4.close()
    chromosome5.close()
    
    # sort Chromosome files by position and direction
    print("Sorting Chromosomes")
    for i in range(1,6):
        command = "cat %s | sort -k3,3n -k2,2 > x; mv x %s" % ( str(i) + ".aj" , str(i) + ".aj" )
        subprocess.call(command,shell = True)

    
    # ------------------------ MySQL Upload --------------------------- #
    
    # Filter those stupid Mysql warnings
    filterwarnings('ignore',category = mdb.Warning)
   
    # Connect to MySQL Database:
    print("Connecting to MySQL Database")
    
    try:
        db = mdb.connect(host=host,user = mysql_user,passwd = mysql_password,local_infile = 1)

    except mdb.Error,e:
        print("Error %d: %s") % (e.args[0],e.args[1])
        print("It looks like you gave a host name that didn't exist!")
        sys.exit(1)

    # With connection create an object to send queries
    with db:
        cur   = db.cursor()

        query = "create database if not exists %s" % (database)
        cur.execute(query)

        for i in range(1,6):
            chrom_file = str(i) + ".aj"

            query = "drop table if exists %s.reads_%s_%d" % (database,tablename,i)
            cur.execute(query)

            query = "create table %s.reads_%s_%d(assembly VARCHAR(2), strand VARCHAR(1), start INT, end INT, sequenceA VARCHAR(100), sequenceB VARCHAR(100))"% (database,tablename,i)
            cur.execute(query)

            query = """LOAD DATA LOCAL INFILE '%s' INTO TABLE %s.reads_%s_%d""" % (os.path.realpath(chrom_file),database,tablename,i)
            cur.execute(query)

        cur.close()

    print("Finished Uploading")

    # ---------------------- Creating Fetcher Information ------------------- #
    with open(tablename + ".php","w") as fetcher:
        fetcher.write("<?php\n")
        fetcher.write("$append_assembly = true;\n")
        fetcher.write("$table = '%s.reads_%s_';\n" % (database,tablename) )
        fetcher.write("$title = '%s';\n" % (tablename))
        fetcher.write("$info = '%s';\n"  % (tablename.replace("_"," ")))
        fetcher.write("""$link = mysql_connect("%s","mysql","rekce") or die("failed");\n""" % (host))
        fetcher.write("require_once '<PUT RELATIVE PATH TO INCLUDES FOLDER>/includes/common_reads.php';\n")
        fetcher.write("?>\n")

    # --------------------- Create Track Information ------------------------ #
    with open(tablename + ".trackDefinition","w") as track_def:
        track_def.write("{\n")
        track_def.write(" id: '%s',\n"   % tablename)
        track_def.write(" name: '%s',\n" % tablename)
        track_def.write(" type: 'ReadsTrack',\n")
        track_def.write(" path: 'NA',\n")
        track_def.write(" data: '<INSERT RELATIVE PATH TO FETCHER>/%s',\n" % (tablename + ".php"))
        track_def.write(" height: '90', \n")
        track_def.write(" scale: 0.03\n")
        track_def.write("},\n")

def get_folder_paths(project):
    """
    Creates a list of "Sample" folders from the Unaligned/Project/ folder specified
    """
    if project[-1] != "/":
        project = project + "/"

    return [project + x for x in os.listdir(project) if os.path.isdir(project + "/" +x)]

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

        # Gunzip Everything
        gunzip_command = "gunzip *.gz"
        subprocess.call(gunzip_command,shell=True)

        # Gather up all fastq's
        fastqs = [x for x in os.listdir(current_folder) if "fastq" in x and "R1_" in x]

        if len(fastqs) < 1:

            fastqs = [x for x in os.listdir(current_folder) if ".fastq" in x and "_R1_" not in x and "_R2_" not in x]

            if len(fastqs) == 0:
                print("Skipping %s since there are no fastqs" % folder)
                continue

        # Call Bowtie2
        bowtie_command = " ".join([path_to_bowtie,bowtie_options,bowtie_indexes,",".join(fastqs),"1> bowtie2.out.sam","2> bowtie2.stats"])
        subprocess.call(bowtie_command,shell=True)

        # # Run local2mysql
        if not bowtie_only:
            print("Running Import to Annoj")
            create_annoj_folder = "mkdir annoj"
            subprocess.call(create_annoj_folder,shell=True)

            with open("bowtie2.out.sam","r") as sam_file:

                os.chdir(current_folder + "/annoj")
                
                local2mysql(sam_file,host,database,tablename,mysql_user,mysql_password)







