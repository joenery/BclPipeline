#!/usr/bin/env python

import os
import sys
import argparse
import signal
import subprocess

# My Modules
from import2annojsimple import local2mysql
from bowtieSimple       import bowtie_folder

class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

def get_folder_paths(project):
    """
    Creates a list of Folders
    """
    return [project + "/" + x for x in os.listdir(project) if os.path.isdir(project + "/" + x)]

if __name__=="__main__":
    # Makes playing with Unix nicer. Mostly a hold over from using STDIN
    signal.signal(signal.SIGPIPE,signal.SIG_DFL)

    # Configure Arguement Parser
    parser = MyParser(description = "Given a Folder this script will Bowtie check each subfolder for Fastq's.\
                                     If there are Fastq's then Bowtie2 will be performed.\
                                     Additionally, if Annoj is wanted then the SAM file will be split in to \
                                     chromosomes and then uploaded in to the MySQL database as well as creating \
                                     a fetcher and track definition in the corresponding directory.")

    mandatory = parser.add_argument_group("MANDATORY")
    advanced  = parser.add_argument_group("ADVANCED")

    mandatory.add_argument("-p","--project", help = "Absolute path to a Project Folder")
    mandatory.add_argument("-ho","--host",help = "This is the mysql host you'd like to put your data on. Eg - thumper-e3, thumper-e4, etc...")
    mandatory.add_argument("-db","--database",help = "What is the name of the database you'd like to put your data in. If it does not exist it will be created for you.")

    advanced.add_argument("-bt","--bowtie-call",help="Path to / or call to shell for bowtie DEFAULT: bowtie2",default="bowtie2")
    advanced.add_argument("-i","--bowtie-indexes",help="Path to the reference genome indexes for Bowtie.",default="/home/seq/bin/bowtie2/INDEXES/tair10")
    advanced.add_argument("-o","--bowtie-only",help="Only Bowtie will be performed",action="store_true")
    advanced.add_argument("-u","--mysql-user",help="MySQL user name. DEFAULT: mysql",default="mysql")
    advanced.add_argument("-pw","--mysql-password",help="MySQL user password. DEFAULT: rekce",default="rekce")

    # Get Command Line Options
    command_line_options = vars(parser.parse_args())

    database       = command_line_options["database"]
    host           = command_line_options["host"]
    project        = command_line_options["project"]
    path_to_bowtie = command_line_options["bowtie_call"]
    bowtie_indexes = command_line_options["bowtie_indexes"]
    bowtie_only    = command_line_options["bowtie_only"]
    mysql_user     = command_line_options["mysql_user"]
    mysql_password = command_line_options["mysql_password"]


    # -------------------- BOWTIE ARGUMENTS --------------------- #

    bowtie_options = "--local -p 8"
    indexes_genome = os.path.basename(bowtie_indexes)
    indexes_folder = os.path.split(bowtie_indexes)[0]

    # --------------------- Checks! ----------------------- #
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
            local2mysql("../bowtie.R1.sam",host,database,tablename,mysql_user,mysql_password)
