#!/usr/bin/env python
# Standard Python Modules
import argparse
import time

# Non Standard Modules
import daemon

# My modules
from guts import *
from emailnotifications import notification

class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

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
    optional.add_argument("-n","--notifications",help="Turn notifications on. An email blast will be sent to the ADMIN and the OPERATORS when BCL has started and when\
                                                     it is complete. DEFAULT: off",
                                                     action="store_true") 

    advanced.add_argument("-s","--sample-sheet",help = "Name of the SampleSheet you'd like to use. DEFAULT: SampleSheet",default="SampleSheet")
    advanced.add_argument("-o","--output-dir",help = "NAME of FOLDER to create at the top of the RUN folder provided. DEFAULT: Unaligned",default="Unaligned") 
    advanced.add_argument("-a","--admin-only",help = "Send notifications to Admins only. Helpful for debugging. DEFAULT: off",action = "store_true")

    #---------------------------- Parse Command Line Options ---------------------------- #
    
    command_line_options = vars(parser.parse_args())

    # Variables
    run           = command_line_options["run"]
    sample_sheet  = command_line_options["sample_sheet"]
    bcl_output_dir = command_line_options["output_dir"]
    
    # Switches
    admin_only    = command_line_options["admin_only"]
    no_watch      = command_line_options["no_watch"]
    notifications = command_line_options["notifications"]

    # --------------------------- Validate Inputs before Continuing ------------------------ #
    # Check the prerequisites for continuing:
    # 1) RTAcomplete.txt exists in the top level
    # 2) There is a sample sheet (SampleSheet.csv) in Run/Data/Intensities/Basecalls/

    if not run:
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(run):
        print("\nIt looks like that Path: %s doesn't exist. Try again.\n" % (run))
        sys.exit(1)

    # --------------------- Running Parsing etc for script ---------------------------------- #

    p = project(run,sample_sheet,bcl_output_dir)

    p.parseSampleSheet()

    # ------------------------- Pre-Start Check    -------------------------------------#
    # # Create Run Log
    # run_log = open(run + "Bcl_log.%s.%s.txt" % (bcl_output_dir,time.strftime("%m-%d-%y--%H:%M",time.localtime())),"a")

    # # Write Entire Command to log
    # run_log.write( time.strftime("%m-%d-%y %H:%M:%S",time.localtime()) + " --> " + " ".join(sys.argv[:]) + "\n")

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

        if notifications:
            n.admin_message("Daemon running for %s" % (run),"")

        if no_watch == False:
                watchRunFolder(run,1800)

        if notifications:
            n.admin_message("Bcl Started for %s" % (run),"")

            if not admin_only:
                n.bcl_start_blast(run,owners_and_samples)            

        print("Starting BCL Analysis")
        p.runConfigureBclToFastq()
        print("Finished BCL Analysis")

        print("Starting Bowtie")
        p.bowtieProjects()

        print("Importing Files to Annoj")


        # Alert the Masses!
        if notifications:
            n.admin_message("Bcl Finished for %s" % (run),"")

            if not admin_only:
                n.bcl_complete_blast(run,owners_and_samples,bcl_output_dir)

        # Clean up
        print("Finished BCL Pipeline :-]")
        run_log.close()