#!/usr/bin/env python

import os
import sys
import MySQLdb as mdb
import re
import signal
from warnings import filterwarnings
import subprocess


def local2mysql(sam,host,database,tablename):
    
    # assume the files are straight out of Bowtie 2 with no options and they contain unmapped reads

    # ----------------- Create the chromosome files here ----------------------- #
    with open(sam,"r") as sam_file:
        
        print("Creating AJ Files")
        chromosome1 = open("1.aj","w")
        chromosome2 = open("2.aj","w")
        chromosome3 = open("3.aj","w")
        chromosome4 = open("4.aj","w")
        chromosome5 = open("5.aj","w")

        count_1 = 0
        count_2 = 0
        count_3 = 0
        count_4 = 0
        count_5 = 0

        for i,line in enumerate(sam_file):

            # Skip the headers
            if i < 9:
                continue

            # Get Variables
            row         = line.strip().strip().split("\t")
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
                count_1 += 1
                chromosome1.write("\t".join([str(count_1),chromosome,direction,read_start,read_end,sequence + "\n"]))

            elif chromosome == "2":
                count_2 +=1
                chromosome2.write("\t".join([str(count_2),chromosome,direction,read_start,read_end,sequence + "\n"]))

            elif chromosome == "3":
                count_3 += 1
                chromosome3.write("\t".join([str(count_3),chromosome,direction,read_start,read_end,sequence + "\n"]))

            elif chromosome == "4":
                count_4 += 1
                chromosome4.write("\t".join([str(count_4),chromosome,direction,read_start,read_end,sequence + "\n"]))

            elif chromosome == "5":
                count_5 += 1
                chromosome5.write("\t".join([str(count_5),chromosome,direction,read_start,read_end,sequence + "\n"]))

        # Close Chromosomes

        chromosome1.close()
        chromosome2.close()
        chromosome3.close()
        chromosome4.close()
        chromosome5.close()
        
        # sort Chromosome files by position and direction
        print("Sorting Chromosomes")
        for i in range(1,6):
            command = "cat %s | sort -k4,4n -k3,3 > x; mv x %s" % ( str(i) + ".aj" , str(i) + ".aj" )
            subprocess.call(command,shell = True)

        
        # ------------------------ MySQL Upload --------------------------- #
        
        # Filter those stupid Mysql warnings
        filterwarnings('ignore',category = mdb.Warning)
       
        # Connect to MySQL Database:
        print("Connecting to MySQL Database")
        
        try:
            db = mdb.connect(host=host,user = 'mysql',passwd ='rekce',local_infile = 1)

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

                query = "create table %s.reads_%s_%d(id INT,assembly VARCHAR(2), strand VARCHAR(1), start INT, end INT, sequenceA VARCHAR(100), sequenceB VARCHAR(100))"% (database,tablename,i)
                cur.execute(query)

                query = """LOAD DATA LOCAL INFILE '%s' INTO TABLE %s.reads_%s_%d""" % (os.path.realpath(chrom_file),database,tablename,i)
                cur.execute(query)

            cur.close()

        print("Finished Uploading")
        print("Creating Fetcher and Track Information in Current Working Directory")

        # ---------------------- Creating Fetcher Information ------------------- #
        with open(tablename + ".php","w") as fetcher:
            fetcher.write("<?php\n")
            fetcher.write("$append_assembly = true;\n")
            fetcher.write("$table = '%s.reads_%s_';\n" % (database,tablename) )
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
            track_def.write(" height: '90', \n")
            track_def.write(" scale: 0.03\n")
            track_def.write("},\n")

if __name__ == "__main__":
    None
