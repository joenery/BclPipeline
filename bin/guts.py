import subprocess
import os
import sys
from collections import defaultdict
import re

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

    if val != 0:
        print("".join(["\n",err_message,"\n","Terminating Script"]))
        sys.exit(1)

class project(object):

    # --------------- Public Methods
    # These are the methods that are meant to be called
    # by the user in scripts.

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
        # 2) If FLAG from above then create a new sample_sheet and save old as samplesheet.csv.old

        samples_with_no_indexes = False

        with open(self.sample_sheet, "r") as sample_sheet:

            for i,line in enumerate(sample_sheet):
                
                if i == 0:
                    continue

                row = line.strip().split(",")

                lane         = row[1]
                sample_name  = row[2].replace(".","_").replace("-","_").replace("#","_num_")
                index        = row[4]
                bowtie_annoj = row[5]
                owner_email  = row[8]
                project      = row[9].replace(".","_").replace("-","_").replace("#","_num_")

                # Private Method Call
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
                projects[project][sample_name] = {"genome":genome,"destination_server":destination,
                                                  "database":database,"barcode1":barcode1,"barcode2":barcode2,
                                                  "lane":lane,"index":index,"project":project,"sample_name":sample_name}

        self.projects = projects

        # TURN BACK ON AFTER TESTING GRAB UNDETERMINED
        # if samples_with_no_indexes:

        #     # Private Method Call
        #     self.convertSampleSheet()

    def runConfigureBclToFastq(self):

        # Change current Working Directory
        os.chdir(self.basecalls)
        print("Current working dir is %s" % os.getcwd())

        print("Running Configure Bcl to Fastq command")
        configureCommand = ["configureBclToFastq.pl","--output-dir","../../../" + self.bcl_output_dir,"--sample-sheet", os.path.basename(self.sample_sheet)]
        print("Configure Command: %s" % " ".join(configureCommand))
        system_call(configureCommand,"Error at configureBclToFastq.pl")

        # Change to output dir
        os.chdir(self.run + "/" + self.bcl_output_dir)
        print("Running make command in %s" %(os.getcwd()))

        system_call(["make","-j","12"],"Make Failed")

        # You can Rip this out when we move to the all index system.
        # If there are Samples With Undetermined Indicies
        # Filter and move the Samples in to the appropriate Project

        # if self.undetermined:
        #     self.grabUndetermined()

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
        mysql_user     = "mysql"
        mysql_password = "rekce"

        bcl_output_dir = self.run + "/" + self.bcl_output_dir

        for project in self.projects:

            for sample in self.projects[project]:

                destination = self.projects[project][sample]["destination"]
                database    = self.projects[project][sample]["database"]

                if not destination and not database:
                    print("Skipping %s" % sample)
                    continue
                
                elif not database:
                    database = project

                elif not destination:
                    print("Skipping %s. No destination specified" % sample)
                    continue

                sample_dir = bcl_output_dir + "/Project_" + project + "/Sample_" + sample

                os.chrdir(sample_dir)
                subprocess.call(["mkdir","annoj"])
                os.chrdir("annoj")

                local2mysql("../bowtie.out.sam",destination,database,sample,mysql_user=mysql_user,mysql_password=mysql_password)

    # --------------- Private Methods
    # These are subroutines that the public methods call
    # Modify here at your own risk

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
        print("Found Samples without Indexes. Modifying SampleSheet.csv")
        print("Saving unmodified SampleSheet as .csv.old")

        all_lanes = {str(x):[] for x in range(1,number_of_lanes + 1)}

        # grab the lanes and sort on them
        with open(self.sample_sheet,"r") as sample_sheet:

                for i,line in enumerate(sample_sheet):

                    if i == 0:
                        header = line
                        continue

                    row = line.strip().split(",")

                    lane = row[1]

                    all_lanes[lane].append(line)

        keys = all_lanes.keys()
        keys.sort()

        # Call the old sample Sheet something else
        subprocess.call(["mv",self.sample_sheet,self.sample_sheet + ".old"])

        with open(self.sample_sheet,"w") as output_file:

            output_file.write(header)

            for lane in keys:
                # ConfigureBclToFastq only needs one Sample in a lane to create an Undetermined Indicies
                # If there happens to be Samples with Indicies in the lane then those samples will be written
                # back out to disk.

                all_lanes[lane].sort(key=lambda x: x[4],reverse=True)

                if not all_lanes[lane]:
                    continue

                # Check and see if the first Sample in the lane has a Index. If so loop over lane and print
                # out all the samples with Indexes. Samples without Indexes will be put in Undetermined Indexes for 
                # that lane.

                first_sample = all_lanes[lane][0].split(",")

                if first_sample[4] != "":

                    for sample in all_lanes[lane]:
                        if sample.split(",")[4] != "":
                            output_file.write(sample)

                else:
                    first_sample[2]  = "lane" + str(first_sample[1])
                    first_sample[-1] = "Undetermined_indices\n"
                    output_file.write(",".join(first_sample))


        self.undetermined = True

    def grabUndetermined(self):
        """
        """

        # Grab all the undetermined indices samples
        # Files are going to be in Undetermined_indices at the top of the run folder in output dir
        # Get all the samples in a particular lane and loop through them.

        lanes = defaultdict(list)

        # Put all samples without indexes in to a dict where
        # Keys: lane 
        # Vals: list of samples in lane

        for project in self.projects:

            for sample in self.projects[project]:

                if self.projects[project][sample]["index"] == "":

                    lanes[self.projects[project][sample]["lane"]].append(self.projects[project][sample])

                else:
                    continue

        # Now using the lanes dictionary
        # Loop through the run/Bcl_Output_Dir/Undertermined_indices
        # 1) If there are gz files. Gunzip them.
        # 2) Create a list of R1 and R2 files. sort them and paste each file together
        # 3) If those steps have been done. Loop through each file and pull out reads
        #    corresponding to the barcodes for those samples. Save the output to this in the Project -> Sample
        #    folder structure.

        print("Preparing the Undetermined_indices folder in %s" % (self.run + "/" + self.bcl_output_dir))

        # Check to see if the Project_Whatever folder exists in the BCL output directory
        # If a lane is all undetermined (for instance) create that directory
        self.checkProjectFolders()

        # Move in to Undetermined and start parsing out Samples
        undetermined_indices_path = self.run + "/" + self.bcl_output_dir + "/Undetermined_indices"

        os.chdir(undetermined_indices_path)
        print("Currently working in %s" % os.getcwd() )

        for lane in lanes:
            # Change dir into the lane we're in
            # if there are gz's gunzip them
            # paste together the files and output them with a uniq name 001.fastq.tmp etc

            lane_path = os.getcwd() + "/Sample_lane" + lane 
            os.chdir(lane_path)
            print("Now in %s" % os.getcwd())

            # If there are gz_files...gunzip them!
            gz_files = [x for x in os.listdir(os.getcwd()) if ".gz" in x]

            if len(gz_files) != 0:
                gunzip = ["gunzip","*"]
                subprocess.call(gunzip)

            # Get All the R1's and R2's
            R1 = [x for x in os.listdir(os.getcwd()) if "_R1_" in x]
            R2 = [x for x in os.listdir(os.getcwd()) if "_R2_" in x]

            R1.sort()
            R2.sort()

            R1andR2 = [(x,y) for x,y in zip(R1,R2)]

            # Since this step takes for ever, check to make sure
            # if this has already been done. If not do it.
            combined_R1andR2_files = [x for x in os.listdir(os.getcwd()) if "R1" in x and "R2" in x and ".fastq" in x]

            if len(combined_R1andR2_files) == 0:
                print("Combining R1's and R2's in lane %s" % (lane))

                for pair in R1andR2:
                    R1_name = pair[0]
                    R2_name = pair[1]

                    match       = re.search("[0-9][0-9][0-9](?=[.fastq])",R1_name)
                    pair_number = match.group(0)

                    paste_command = ["paste",R1_name,R2_name,"> R1_R2_" + pair_number + ".fastq"]
                    paste_command = " ".join(paste_command)
                    subprocess.call(paste_command,shell=True)

                combined_R1andR2_files = [x for x in os.listdir(os.getcwd()) if "R1" in x and "R2" in x and ".fastq" in x]

            else:
                print("R1 and R2's already combined. Skipping step")

            # Start going through samples and pulling them out
            print("Pulling Out Samples")
            for sample in lanes[lane]:

                project     = sample["project"]
                sample_name = sample["sample_name"]
                sample_lane = sample["lane"]
                barcode1    = sample["barcode1"]
                barcode2    = sample["barcode2"]

                barcode1_length = len(barcode1)
                barcode2_length = len(barcode2)

                # With sample make a folder in the Project folder called Sample_sample
                # Open an output file in there.
                # for each Fastq in the R1 R2 fastq's
                # if the barcodes match spit that out to the output file
                make_sample_dir = ["mkdir",self.run + "/" + self.bcl_output_dir + "/Project_" + project + "/Sample_" + sample_name]
                subprocess.call(make_sample_dir)


                # Writing the output to be in the run -> Project -> Sample format
                # That way the Bowtie and Import2Annoj steps don't have to be modified

                with open(self.run + "/" + self.bcl_output_dir + "/Project_" + project + "/Sample_" + sample_name + "/lane" + sample_lane + "_" + \
                          sample_name + ".fastq","w") as output_file:
                    
                   # Loop through combined R1and R2 files
                    for r1andr2 in combined_R1andR2_files:

                        with open(r1andr2,"r") as fastq_file:

                            for i,line in enumerate(fastq_file):

                                if i % 4 == 0:
                                    readID = line.strip().split()[0]

                                elif i % 4 == 1:
                                    sequences = line.strip().split()

                                    sequence1 = sequences[0]
                                    sequence2 = sequences[1]

                                    # Pull out the beginning part of sequence
                                    sequence_barcode1 = sequence1[:barcode1_length]
                                    sequence_barcode2 = sequence2[:barcode2_length]

                                elif i % 4 == 2:
                                    qual1 = line.strip()

                                elif i % 4 == 3:
                                    qual2 = line.strip()

                                    if sequence_barcode1 == barcode1 and sequence_barcode2 == barcode2:
                                        output_file.write(readID.strip() + "\n")

                                        # Removing Barcodes
                                        output_file.write(sequence1[barcode1_length:].strip() + "\n")

                                        # Since the barcodes are removed the Quality information needs to
                                        # be truncated too!
                                        output_file.write(qual1.split()[0].strip() + "\n" )
                                        output_file.write(qual2.split()[0].strip()[barcode1_length:] + "\n")

            # Change Directory back to Top of Undetermined
            os.chdir(undetermined_indices_path)

    def checkProjectFolders(self):

        for project in self.projects:
            # Check to see if that project folder exists in
            # the bcl output dir

            project_folder_path = self.run + "/" + self.bcl_output_dir + "/" + "Project_" + project

            if not os.path.isdir(project_folder_path):
                create_project_folder = ["mkdir",project_folder_path]
                subprocess.call(create_project_folder)

            else:
                # That project Folder Exists and we don't have to create it
                continue

if __name__=="__main__":
    print("Testing...")

    p = project(run_path="/mnt/thumper-e4/illumina_runs/130213_HAL_1222_AC112TACXX/",sample_sheet="SampleSheet_130213_HAL_1222_AC112TACXX_FILTER.csv",bcl_output_dir="ChlamyTest")

    print("Parsing Sample Sheet")
    p.parseSampleSheet()

    # print("Running ConfigureBclToFastq")
    # p.runConfigureBclToFastq()

    # Don't forget to turn back on call to convertSampleSheet
    p.grabUndetermined()

    # p.bowtieProjects()
    # p.import2annojsimple
    