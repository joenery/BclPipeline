#!/usr/bin/python -u

import argparse
import os
import re
import subprocess
import sys
import time

# Non standard Modules
import daemon
import MySQLdb as mdb

# My Modules
from guts import *

#--------------------- Script Specific Functions ----------------- #

class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

def watchRunFolder(run,sleep):
    """
    Args:
    run -> A folder that contains an RTAcomplete.txt

    Method: The file will be polled every hour. If the first line is not the same as the last time it checked it will kick out and run the rest
            of the BCL pipeline
    """

    RTAcomplete = run +"/RTAComplete.txt"
    iteration = 0

    while True:

        time.sleep(sleep)

        if not os.path.isfile(RTAcomplete):
            print("Real Time Analysis has not begun yet.")
            continue
        
        else:
            with open(RTAcomplete,"r") as input_file:
                current_line = input_file.readline().strip()

                if iteration == 0:
                    prev_line = current_line
                    iteration +=1

                if current_line != prev_line:
                    print("Checked file at %s and it has been changed." % time.strftime("%m-%d-%y %H:%M:%S",time.localtime()))
                    print("Moving on to Bcl Analysis")

                    break

if __name__=="__main__":
    
    # --------------------------- Configure Arg Parser ----------------------------- #

    parser = MyParser(description = "Bcl Pipeline takes in an absolute path to the Top Level of an Illumina Run and watches the RTAComplete.txt\
                                     file for changes every hour. When the file is updated to reflect the finish time the script runs BCL.\
                                     Options for running Bowtie and and MySQL Upload are handled in the SampleSheet.csv file\
                                     \
                                     The SampleSheet.csv located in the Run/Data/Intensities/BaseCalls folder will act as the configuration file.\
                                     See README.md for more information.")

    mandatory = parser.add_argument_group("MANDATORY")
    optional  = parser.add_argument_group("OPTIONAL")
    advanced  = parser.add_argument_group("ADVANCED -> Use only if you're a Ninja or Jedi!")

    mandatory.add_argument("-r","--run",help = "Absolute path to the run folder you would like to watch and run Bcl on.")

    optional.add_argument("-nw","--no-watch",help="If the run has already completed and you would like to just run Bcl etc then turn this flag on. DEFAULT: off",\
                         action="store_true")
    optional.add_argument("-nn","--no-notifications",help="Turn notifications off. DEFAULT: notifications are on",
                                                     action="store_true") 

    advanced.add_argument("-s","--sample-sheet",help = "Name of the SampleSheet you'd like to use. DEFAULT: SampleSheet",default="SampleSheet")
    advanced.add_argument("-o","--output-dir",help = "NAME of FOLDER to create at the top of the RUN folder provided. DEFAULT: Unaligned",default="Unaligned") 
    advanced.add_argument("-a","--admin-only",help = "Send notifications to Admins only. Helpful for debugging. DEFAULT: off",action = "store_true")

    #---------------------------- Parse Command Line Options ---------------------------- #
    
    command_line_options = vars(parser.parse_args())

    run              = command_line_options["run"]
    no_watch         = command_line_options["no_watch"]
    no_notifications = command_line_options["no_notifications"]
    sample_sheet     = command_line_options["sample_sheet"]
    bcl_output_dir   = command_line_options["output_dir"]
    admin_only       = command_line_options["admin_only"]

    #-------------------------- Checking the options! ------------------------------------ #
    if not run:
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(run):
        print("\nIt looks like that Path: %s doesn't exist. Try again.\n" % (run))
        sys.exit(1)

    # -------------------------- Parse SampleSheet.csv  ------------------------------- #
    
    print("Parsing The Sample Sheet you gave me! Just a moment :-]")
    p = project(run_path=run,sample_sheet=sample_sheet,bcl_output_dir=bcl_output_dir)
    p.parseSampleSheet()

    # ------------------------- Pre-Start Check and Log Creation   -------------------------------------#
    # Create Run Log
    run_log = open(run + "/Bcl_log.%s.%s.txt" % (bcl_output_dir,time.strftime("%m-%d-%y--%H:%M",time.localtime())),"a")

    # Write Entire Command to log
    run_log.write( time.strftime("%m-%d-%y %H:%M:%S",time.localtime()) + " --> " + " ".join(sys.argv[:]) + "\n")

    while True:

        answer = raw_input("\nDo you want to launch the Bcl Pipeline Daemon? (y,n): ")

        if answer == "y":
            break

        elif answer == "n":
            print("Script Aborted. Daemon NOT running.")
            sys.exit(1)

        else:
            print " (y/n) only please! "

    #  ---------------------------- Start of Daemon ------------------------------- #
    print("\nStarting the Daemon. Bye!\n")

    with daemon.DaemonContext(stdout=run_log,stderr=run_log):

        print("Daemon is now running")

        if not no_notifications:
            p.adminEmailBlast("Daemon running for %s" % (run),"")

        if no_watch == False:
                watchRunFolder(run,1800)

        if not no_notifications:
            p.adminEmailBlast("Bcl Started for %s" % (run),"")

            if not admin_only:
                p.bclStartEmailBlast()            

        print("Starting BCL Analysis")
        p.runConfigureBclToFastq()
        print("Finished BCL Analysis")

        print("Running Bowtie Analysis")
        p.bowtieProjects()

        print("Running Annoj prep and upload")
        p.import2annojsimple()

        # Alert the Masses!
        if not no_notifications:
            p.adminEmailBlas("Bcl Finished for %s" % (run),"")

            if not admin_only:
                p.bclCompleteEmailBlast()

        # Clean up
        print("Finished BCL Pipeline :-]")
        run_log.close()

    # # ----- Tests
    # print no_notifications
    # print admin_only