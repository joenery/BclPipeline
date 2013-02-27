import subprocess
import os
import sys
from collections import defaultdict

# My modules
from bowtie import bowtie_folder
from import2annojsimple import local2mysql


def system_call(command,err_message):
    """
    A wrapper for subprocess.call()

    It allows me to exit out of the script if the shell call results in an
    error code of 1 or greater.

    Hides this logic from the rest of the script since shell calls are made frequently
    """

    val = subprocess.call(command)

    if val !=0 or val != None:
        print("".join(["\n",err_message,"\n","Terminating Script"]))
        sys.exit(1)

class project(object):

    def __init__(self,run_path,sample_sheet,bcl_output_dir):
        # Format run correctly:
        if run_path[-1] == "/":
            run_path = run_path[:-1]

        # Make sure the run path exists
        if os.path.isdir(run_path) and os.path.isdir(run_path + "/Data/Intensities/BaseCalls"):
            self.run       = run_path
            self.basecalls = run_path + "/Data/Intensities/BaseCalls"
        else:
            print("\nCheck to make sure the path you've given for the run is correct.\n")
            sys.exit(1)

        # Make sure The Sample Sheet exists
        if os.path.isfile(run_path + "/Data/Intensities/BaseCalls/" + sample_sheet):
            self.sample_sheet = os.path.abspath(run_path + "/Data/Intensities/BaseCalls/" + sample_sheet)

        elif os.path.isfile(sample_sheet):
            self.sample_sheet = os.path.abspath(sample_sheet)

        else:
            print("\nThe Sample Sheet file you've given me doesn't seem to exist!")
            sys.exit(1)

        self.bcl_output_dir = bcl_output_dir

        # Check undetermined Indices?
        self.undetermined = False

    def parseSampleSheet(self):
        """
        """
        projects = defaultdict(dict)

        # Strategy: Take two passes at the file
        # 1) Get all the Sample Information and save in to a dictionary. If there is a lane with
        #    no Index then mark a flag and alert the user that a modified version of their sample
        #    sheet will be created
        # 2) If FLAG from above then create a new sample_sheet

        samples_with_no_indexes = False

        with open(self.sample_sheet, "r") as sample_sheet:

            for i,line in enumerate(sample_sheet):
                
                if i == 0:
                    continue

                row = line.strip().split(",")

                sample_name  = row[2].replace(".","_").replace("-","_").replace("#","_num_")
                index        = row[4]
                bowtie_annoj = row[5]
                owner_email  = row[8]
                project      = row[9].replace(".","_").replace("-","_").replace("#","_num_")

                parsed_options = self.parseBowtieAndAnnojOptions(bowtie_annoj)

                genome      = parsed_options["genome"]
                destination = parsed_options["destination"]
                database    = parsed_options["database"]
                barcode1    = parsed_options["barcode1"]
                barcode2    = parsed_options["barcode2"]

                # Check for Bad Entries
                if genome and database and not destination:
                    print("Error in line %s of %s: %s and %s specified but not a MySQL server destination" % (i,os.path.basename(self.sample_sheet),genome,database))
                    sys.exit(1)

                if not genome and database and destination:
                    print("Error in line %s of %s: %s and %s specified but bowtie was not selected to run" % (i,os.path.basename(self.sample_sheet),databasem,destination))

                if not index:
                    samples_with_no_indexes = True

                # Yup
                if genome == "bt":
                    genome = "tair10"

                # Add to dictionary
                projects[project][sample_name] = {"genome":genome,"destination_server":destination,"database":database,"barcode1":barcode1,"barcode2":barcode2}

        self.projects = projects

        if samples_with_no_indexes:

            self.convertSampleSheet()

    def runConfigureBclToFastq(self):
        """
        """

        # Change current Working Directory
        os.chdir(self.basecalls)
        print("Current working dir is %s" % os.getcwd())

        print("Running Configure Bcl to Fastq command")
        system_call(["configureBclToFastq.pl","--output-dir","../../../" + self.bcl_output_dir,"--sample-sheet", os.path.basename(self.sample_sheet)],"Error at configureBclToFastq.pl")

        # Change to output dir
        os.chrdir(self.run + "/" + self.bcl_output_dir)
        print("Running make command in %s" %(os.getcwd()))

        system_call(["make","-j","12"],"Make Failed")

    def bowtieProjects(self):

        output_dir = self.run + "/" + self.output_dir

        for project in self.projects:

            for sample in self.projects[project]:

                # Using Calling Bowtie Module that was imported bowtie.py
                # As long as API remains the same this method will always work
                genome = self.projects[project][sample]["genome"]

                folder = output_dir + "/Project_" + project + "/Sample_" + sample

                bowtie_folder(folder,indexes_genome=genome)

    def importProjects2Annoj(self):
        """
        """
        bcl_output_dir = self.run + "/" + self.bcl_output_dir

        for project in self.projects:

            for sample in self.projects[project]:

                destination = self.projects[project][sample]["destination"]
                database    = self.projects[project][sample]["database"]

                if not destination and not database:
                    print("Skipping %s" % sample)
                    continue
                
                if not database:
                    database = project

                sample_dir = bcl_output_dir + "/Project_" + project + "/Sample_" + sample

                os.chrdir(sample_dir)
                subprocess.call(["mkdir","annoj"])
                os.chrdir("annoj")

                local2mysql("../bowtie2.out.sam",destination,database,sample)

    # Private Methods
    @staticmethod
    def parseBowtieAndAnnojOptions(bowtie_and_annoj_options):

        options = bowtie_and_annoj_options.split(";")
        genome      = options[0]
        destination = options[1]
        database    = options[2]

        try:
            barcode1 = options[3]
        except IndexError:
            barcode1 = ""

        try:
            barcode2 = options[4]
        except:
            barcode2 = ""


        return {"genome":genome, "destination":destination,"database":database,"barcode1":barcode1,"barcode2":barcode2}

    def convertSampleSheet(self,number_of_lanes=8):

        samples_with_no_indexes = defaultdict(dict)
        all_lanes = {str(x):[] for x in range(1,number_of_lanes + 1)}

        # grab the lanes and sort on them
        with open(self.sample_sheet,"r") as sample_sheet:

                for i,line in enumerate(sample_sheet):

                    if i == 0:
                        header = line.strip()
                        continue

                    row = line.strip().split(",")

                    lane = row[1]

                    all_lanes[lane].append(line)

        for lane in all_lanes:
            all_lanes[lane].sort(key=lambda x: x[4],reverse=True)

            print all_lanes[lane]


        self.undetermined = True

if __name__=="__main__":
    print("Testing...")

    p = project(run_path="/mnt/thumper-e3/home/jfeeneysd/130116_JONAS_2139",sample_sheet="SampleSheet.Test.2.csv",bcl_output_dir="SuperTest")

    p.parseSampleSheet()
