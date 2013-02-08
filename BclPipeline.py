#!/usr/bin/env python

import os
import sys
import subprocess
import argparse
import re
from warnings import filterwarnings
import time
import smtplib
from collections import defaultdict

# Non-Standard Python Modules
import daemon
import MySQLdb as mdb

# NOTE: Sequences that are upload to annoj should only be what the snip string specifies not the entire sequence
# NOTE: Bowtie 2 should auto-magically run seperate analysis if the reads are paired-end. COS IT DON"T!!

# Classes and Functions
class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

class notification(object):

    def __init__(self):
        self.username = "genomic.analysis.ecker"
        self.password = "g3n0m3analysis"
        self.FROM     = "genomic.analysis.ecker@gmail.com"

        self.admin    = ["jfeeneysd@gmail.com","jnery@salk.edu"]
        # self.admin = ["jfeeneysd@gmail.com"]

    def send_message(self,TO,SUBJECT,TEXT):
        """
        ARGS:
        - TO      -> a list of recipients
        - SUBJECT ->
        - TEXT    ->
        """

        message = """\From: %s\nTo: %s\nSubject: %s\n\n%s""" % (self.FROM, ", ".join(TO), SUBJECT, TEXT)

        try:
            server = smtplib.SMTP("smtp.gmail.com",587)
            server.ehlo()
            server.starttls()
            server.login(self.username,self.password)
            server.sendmail(self.FROM,TO,message)
            server.close()
            print("Notification to %s successful" % " ".join(TO))

        except smtplib.SMTPRecipientsRefused:
            print("Couldn't send email to %s. Skipping Message. Bcl Script Continues" % (",".join(TO)))

        except Exception,err:
            sys.stderr.write('ERROR: %s -> Not Sending Message. Script Continues\n' % str(err))

    def bcl_complete_blast(self,run,owners_and_samples,bcl_output_dir):
        """
        ARGS:
        owners_and_samples: A dicitionary where Keys are mapped to email address and a list of samples are the values. This is created in Sample_sheet parser
        """

        for email in owners_and_samples.keys():

            # Get a list of all the samples paths that was associated with that person.
            samples = []

            for tup in owners_and_samples[email]:
                project = tup[0]
                sample  = tup[1]

                samples.append(run + "/" + bcl_output_dir + "/Project_" + project + "/Sample_" + sample)

            text = " Your files are located at: \n\n%s\n" % "\n".join(samples)

            self.send_message([email],"Bcl Run Complete!",text)

    def admin_message(self,SUBJECT,TEXT):
        self.send_message(self.admin,SUBJECT,TEXT)

    def bcl_start_blast(self,run,owners_and_samples):
        """
        Mass email to all those who have samples in the run
        """

        if len(owners_and_samples) != 0:
            self.send_message(owners_and_samples.keys(),"Bcl Analysis Has Started for %s" % (os.path.basename(run)),"Just a friendly reminder from GAL-E to let you know that the following run: %s has started it's Bcl Analysis.\n\nI'll send you an email with the path(s) to your sample(s) on Oberon when it's done.\n" % (os.path.basename(run)))

        else:
            print("No one to BCL_Start_Blast")

def parseSampleSheet(run,sample_sheet):
    """
    Function: Given a run path the function will check the /Data/Intensities/BaseCalls folder for a SampleSheet.csv
              then it will parse the file and return a dictionary such that { Project_Name:[(Sample_1,Options),(Sample_2,options)...] }

    Args:
    - run -> A valid ABSOLUTE run path. Validation occurs much earlier in script

    Returns:
    - projects_and_samples,bowtie_projects,annoj_projects -> A tuple containing the dictionaries
    """

    with open(run + "/Data/Intensities/BaseCalls/%s.csv" % sample_sheet,"r") as csv:

        # Note: If you query against a default dict and don't place anything in there
        # A key will Still be created!
        projects_and_samples = defaultdict(list)
        bowtie_projects      = defaultdict(list)
        annoj_projects       = defaultdict(list)
        owners_and_samples   = defaultdict(list)

        for i,line in enumerate(csv):

            if i == 0:
                continue

            row = line.strip().split(",")

            sample_name    = row[2]
            options        = row[5]
            operator_email = row[8]
            project        = row[9]

            # Add the information to the Project-> Sample List
            if sample_name not in projects_and_samples[project]:
                projects_and_samples[project].append(sample_name)

            # Begin Parsing the Bowtie and Annoj Stuff
            if options != "" and options != line.strip() and filter(None,options.split(";")) != []:
                row = options.split(";")
                genome = row[0]
                annoj_thumper = row[1]
                annoj_database = row[2]

                # Check Constraints
                if genome == "" and (annoj_thumper != "" or annoj_database != ""):
                    print("%s %s options cannot be parsed. Annoj option selected but Bowtie was not." % (project,sample_name))
                    continue

                if genome != "" and annoj_thumper == "" and annoj_database !="":
                    print("%s %s options cannot be parsed. Bowtie selected and MySQL table specified but no MySQL Host specified" % (project,sample_name))
                    continue

                # Populate the Project Dictionaries
                # Bowtie
                if genome !="" and (sample_name,genome) not in bowtie_projects[project]:
                    bowtie_projects[project].append((sample_name,genome))

                # Annoj
                if annoj_thumper != "" and (sample_name,annoj_thumper,annoj_database) not in annoj_projects[project]:
                    annoj_projects[project].append((sample_name,annoj_thumper,annoj_database))

            # Get the operator_emails and samples all together
            if operator_email != "":
                if (project,sample_name) not in owners_and_samples[operator_email]:
                    owners_and_samples[operator_email].append((project,sample_name))

    return {"projects_and_samples":projects_and_samples, "bowtie_projects":bowtie_projects, "annoj_projects":annoj_projects, "owners_and_samples":owners_and_samples}

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

def runBCL(run,sample_sheet,bcl_output_dir):
    """
    Function: Makes system calls to casava in the run/Data/intensities/BaseCalls folder. Once the Unaligned Folder has been created it moves to it
              and runs the make command
    """

    # Change Current Working Dir to BaseCalls run BCL
    os.chdir(run + "/Data/Intensities/BaseCalls")
    print("Current working Dir is %s") % os.getcwd()

    bcl_command = " ".join(["configureBclToFastq.pl","--output-dir","../../../" + bcl_output_dir,"--sample-sheet",sample_sheet + ".csv"])

    print("Finished Bcl Command")

    subprocess.call(bcl_command,shell=True)

    # Change to Top level/Unaligned and run make
    os.chdir(run + "/" + bcl_output_dir)
    print("Running Make Command")
    print("Current working directory is %s") % os.getcwd()

    make_command = "make -j 12"

    subprocess.call(make_command,shell=True)

def bowtieProjects(run,projects_and_samples,processors,bcl_output_dir):
    """
    Function: Given a dictionary that contains Projects as keys and a list of samples as values this function will go into every directory

    To Do: Eventually need to be able to switch out genomes from TAIR10. Check for Paired end!

    """

    for project in projects_and_samples.keys():

        samples = projects_and_samples[project]

        # Create a list of the full paths to the Sample folders:
        sample_folders = [run + "/" + bcl_output_dir + "/Project_" + project + "/Sample_" + name[0] for name in samples]

        for s in sample_folders:

            # Get a list of all files that contain "FASTQ"
            files = [x for x in os.listdir(s) if ".gz" in x]

            # Don't want to wast time on folders that don't have anythinf in them
            if len(files) < 1:
                print("No Files to Bowtie or Unzip. Skipping " + s)
                continue

            # Change working Directory to sample Folder
            os.chdir(s)

            # Gunzip everything .gz
            gunzip_command = "gunzip *.gz"
            subprocess.call(gunzip_command,shell=True)

            # Get a list of all files that contain "FASTQ"
            fastq_files = [x for x in os.listdir(s) if "fastq" in x]

            # Bowtie
            bowtie_command = "bowtie2 --local -p %s /data/home/seq/bin/bowtie2/INDEXES/tair10 %s 1> bowtie2.out.sam 2> bowtie2.stats" % (processors,",".join(fastq_files))
            subprocess.call(bowtie_command,shell=True)

            print("Finished gunzipping and Bowtie-ing %s:%s" % (project,s))

def getAlignedSequence(raw_sequence,match_string):
    """
    Parses a CIGAR string and return a list of aligned sequences
    """

    num = []
    matches_and_snips = []

    for char in match_string:

        if char not in ["S","M","s","m","N","n","D","d","I","i"]:
            num.append(char)
        else:
            length = int("".join(num))

            matches_and_snips.append((length,char))

            num = []

    # Using that information construct the list of sequences.
    # note: Sam starts on 1 and python starts on 0

    start = 0

    sequences = []

    for m in matches_and_snips:

        length_of_match = m[0] 
        type_of_match   = m[1]

        if type_of_match == "M":
            sequences.append(raw_sequence[start:start + length_of_match])

        start += length_of_match

    return sequences

def convert_and_upload_sam2_annoj(run,annoj_samples,bcl_output_dir):
    """
    Function: given a dictionary Annoj_samples that must be a subset (or the entire set) of bowtie_samples. Each one of these samples will be sorted and loaded
    to a MySQL Database of the user's choosing. The SampleSheet.csv takes care of this.
    """

    for project in annoj_samples.keys():

        for sample in annoj_samples[project]:

            sample_folder = run + "/" + bcl_output_dir + "/Project_" + project + "/Sample_" + sample[0]

            # Same as Bowtie
            fastq_files = [x for x in os.listdir(sample_folder) if "fastq" in x]

            if len(fastq_files) < 1:
                print("No Files to upload to MySQL. Skipping " + sample_folder)
                continue

            # Parse the information from the Sample
            sample_name = sample[0]
            host        = sample[1]
            database    = sample[2]

            # If no database is given assume that the Project name is the Database
            if database == "":
                database = project

            tablename = sample_name + "_" + os.path.basename(run)

            # Create Annoj subfolder:
            folder_command = "mkdir %s/annoj" % (sample_folder)
            subprocess.call(folder_command,shell=True)

            # Change to that directory
            os.chdir(sample_folder + "/annoj")

            print("Creating AJ Files for %s:%s" % (project,sample))
            chromosome1 = open("1.aj","w")
            chromosome2 = open("2.aj","w")
            chromosome3 = open("3.aj","w")
            chromosome4 = open("4.aj","w")
            chromosome5 = open("5.aj","w")

            with open(sample_folder + "/bowtie2.out.sam") as bt:

                for i,line in enumerate(bt):
                    # Skip the headers

                    if i < 9:
                        continue

                    # Get Variables
                    row         = line.strip().split()
                    chromosome  = row[2].replace("Chr","").replace("chr","")
                    read_start  = row[3]
                    snip_string = row[5]
                    direction   = row[1]
                    raw_sequence    = row[9]

                    # Assume there is only one or at most one significant sequence in the read
                    parsed_sequence = getAlignedSequence(raw_sequence,snip_string)
                    sequence        = parsed_sequence[0]
                    
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

                # Close Chromosome Files
                chromosome1.close()
                chromosome2.close()
                chromosome3.close()
                chromosome4.close()
                chromosome5.close()

            # Sort Chromosomes:
            for k in range(1,6):
                sort_command = "cat %s.aj| sort -k3,3n -k2,2 > x ; mv x %s.aj" % (str(k),str(k))
                subprocess.call(sort_command,shell=True)

            # -------------------------- MySQL Upload --------------------------- #
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

                    query = "create table %s.reads_%s_%d(assembly VARCHAR(2), strand VARCHAR(1), start INT, end INT, sequenceA VARCHAR(100), sequenceB VARCHAR(100))"% (database,tablename,i)
                    cur.execute(query)

                    query = """LOAD DATA LOCAL INFILE '%s' INTO TABLE %s.reads_%s_%d""" % (os.path.realpath(chrom_file),database,tablename,i)
                    cur.execute(query)

                cur.close()

            print("Finished Uploading %s:%s to host:%s database:%s table:%s" % (project,sample,host,database,tablename))

            # ---------------------- Creating Fetcher Information ------------------- #
            with open(tablename + ".php","w") as fetcher:
                fetcher.write("<?php\n")
                fetcher.write("$append_assembly = true;\n")
                fetcher.write("$table = '%s.reads_%s_';\n" % (database,tablename) )
                fetcher.write("$title = '%s';\n" % (tablename))
                fetcher.write("$info = '%s';\n"  % (tablename.replace("_"," ")))
                fetcher.write("""$link = mysql_connect("%s","mysql","rekce") or die("failed");\n""" % (host))
                fetcher.write("require_once '<PUT RELATIVE PATH TO HTML PAGE>/includes/common_reads.php';\n")
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

def check_path_for_file(f):
    path = os.environ["PATH"].split(":")

    for p in path:

        if os.path.isfile(p + "/" + f):
            return True

    else:
        return False

# Main Running Function
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

    advanced.add_argument("-p","--processors",help = "Number of processors to run if/when Bowtie2 is excecuted DEFAULT: 12.",default=12)
    advanced.add_argument("-s","--sample-sheet",help = "Name of the SampleSheet you'd like to use. DEFAULT: SampleSheet",default="SampleSheet")
    advanced.add_argument("-o","--output-dir",help = "NAME of FOLDER to create at the top of the RUN folder provided. DEFAULT: Unaligned",default="Unaligned") 
    advanced.add_argument("-a","--admin-only",help = "Send notifications to Admins only. Helpful for debugging. DEFAULT: off",action = "store_true")

    #---------------------------- Parse Command Line Options ---------------------------- #
    
    command_line_options = vars(parser.parse_args())

    run           = command_line_options["run"]
    no_watch      = command_line_options["no_watch"]
    processors    = command_line_options["processors"]
    notifications = command_line_options["notifications"]
    sample_sheet  = command_line_options["sample_sheet"]
    bcl_output_dir = command_line_options["output_dir"]
    admin_only    = command_line_options["admin_only"]

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

    # ---------------------------- Clean-Up Inputs and Turn on Flags ----------------------------------- #
    
    # Check for BCL fastq converter and bowtie
    if not check_path_for_file("configureBclToFastq.pl"):
        print("\nExpected configureBclToFastq.pl to be in your $PATH but it wasn't there!\n")
        sys.exit(1)

    if not any([os.path.exists(os.path.join(p,"bowtie2")) for p in os.environ["PATH"].split(os.pathsep)]):
        print("bowtie2 is not in your path. Add it to your path.")
        sys.exit(1)

    # Make Sure Run is formatted correctly
    if run[-1] == "/":
        run = run[:-1]

    # Turn on Notifications
    if notifications:
        n = notification()

    # Make sure there isn't a .csv at the end of file
    if os.path.splitext(sample_sheet)[1] != "":
        sample_sheet = os.path.splitext(sample_sheet)[0]

    # Check to make sure that the SampleSheet exists
    if not os.path.isfile(run + "/Data/Intensities/BaseCalls/" + sample_sheet + ".csv"):
        print("\nIt looks like %s doesn't exist in %s Can you create it?\n" % (sample_sheet,run + "/Data/Intensities/BaseCalls/"))
        sys.exit(1)

    # -------------------------- Parse SampleSheet.csv  ------------------------------- #
    
    project_dicts = parseSampleSheet(run,sample_sheet)

    projects_and_samples = project_dicts["projects_and_samples"]
    bowtie_samples       = project_dicts["bowtie_projects"]
    annoj_samples        = project_dicts["annoj_projects"]
    owners_and_samples   = project_dicts["owners_and_samples"]

    # ------------------------- Pre-Start Check    -------------------------------------#
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

        if notifications:
            n.admin_message("Daemon running for %s" % (run),"")

        if no_watch == False:
                watchRunFolder(run,1800)

        if notifications:
            n.admin_message("Bcl Started for %s" % (run),"")

            if not admin_only:
                n.bcl_start_blast(run,owners_and_samples)            

        print("Starting BCL Analysis")
        runBCL(run,sample_sheet,bcl_output_dir)
        print("Finished BCL Analysis")

        print("Running Bowtie Analysis")
        bowtieProjects(run,bowtie_samples,processors,bcl_output_dir)

        print("Running Annoj prep and upload")
        convert_and_upload_sam2_annoj(run,annoj_samples,bcl_output_dir)

        # Alert the Masses!
        if notifications:
            n.admin_message("Bcl Finished for %s" % (run),"")

            if not admin_only:
                n.bcl_complete_blast(run,owners_and_samples,bcl_output_dir)

        # Clean up
        print("Finished BCL Pipeline :-]")
        run_log.close()

    # # ------------------------------ Testing --------------------------------------- #

    # print("Running Bowtie Analysis For All Files")
    # bowtieProjects(run,bowtie_samples,processors)

    # print("Running Annoj prep and upload")
    # convert_and_upload_sam2_annoj(run,annoj_samples)

    # n.send_message(['jfeeneysd'],"Exception Test","Did this work?")
    # n.bcl_complete_blast(run,owners_and_samples)
    # n.admin_message("Bcl Running","Bcl Running")
    # n.bcl_start_blast(run,owners_and_samples)
