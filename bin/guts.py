import subprocess
import os
import sys
from collections import defaultdict
import re
from socket import gethostname
import csv

# My modules
from bowtieSimple import bowtie_folder
from import2annojsimple import *
from emailnotifications  import notifications
from tdna_seq_caller import fillChromosomeFromMySQL,pool_caller,pool_cleaner

def system_call(command,err_message,admin_message=False,extra=None,shell=False):
    """
    A wrapper for subprocess.call()

    It allows me to exit out of the script if the shell call results in an
    error code of 1 or greater.

    Hides this logic from the rest of the script since shell calls are made frequently
    """

    if not shell:
        val = subprocess.call(command)
    else:
        val = subprocess.call(" ".join(command),shell=shell)

    if (val != 0 and admin_message == False) or (val != 0 and extra==None and admin_message == True):
        print("".join(["\n",err_message,"\n","Terminating Script"]))
        sys.exit(1)

    elif val != 0 and admin_message == True and extra:
        print("".join(["\n",err_message,"\n","Terminating Script"]))

        subject = "Pipeline failed on %s" % (os.path.basename(extra.run))
        text    = "Error Message--> %s" % (err_message)

        extra.adminRunInfoBlast(subject,text)
        sys.exit(1)

class project(object):

    # ---- Public Methods
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

        # Get Host name
        self.host = gethostname()

    def parseSampleSheet(self):
        """
        """
        projects = defaultdict(dict)
        samples_with_no_projects = False

        with open(self.sample_sheet, "rU") as sample_sheet:
            sample_sheet = csv.reader(sample_sheet)          

            for i,row in enumerate(sample_sheet):
                
                if i == 0:
                    continue

                lane         = row[1]
                sample_name  = row[2].replace(".","_").replace("-","_").replace("#","_num_")
                index        = row[4]
                bowtie_annoj = row[5]
                owner_email  = row[8]
                project      = row[9].replace(".","_").replace("-","_").replace("#","_num_")

                # If there are no names in the projects Quit!
                if project == "":
                    print("\nSAMPLE:%s on LINE:%s IN:%s has no value for Sample_Project!\n" % (sample_name,i,os.path.basename(self.sample_sheet)))
                    samples_with_no_projects = True

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

                # Make the assumption that BT means tair10
                if genome == "bt":
                    genome = "tair10"

                if genome == "tair10":
                    chromosomes = [1,2,3,4,5]
                else:
                    chromosomes = []


                # Is the sample part of a TDNA project?
                if "tdna" in project.lower():
                    tdna = True
                else:
                    tdna = False

                # Since some Samples will be re-run in the same database with the
                # same name the sample names need to be more specific
                run_path = self.run
                run_name = os.path.basename(self.run[:-1]) if self.run[-1] == "/" else os.path.basename(self.run)
                sample_name_with_run_info = sample_name + "_" + run_name

                # Add to dictionary
                projects[project][sample_name] = {"genome":genome,"chromosomes":chromosomes,"destination":destination,
                                                  "database":database,"barcode1":barcode1,"barcode2":barcode2,
                                                  "lane":lane,"index":index,"project":project,"sample_name":sample_name,
                                                  "owner_email":owner_email,"tdna":tdna,
                                                  "sample_name_with_run_info":sample_name_with_run_info}

                projects[project]["chromosomes"] = chromsomes

        # Save info to object
        if samples_with_no_projects:
            print("Samples with no projects will result in emails to owners without paths to their files.")

        self.projects = projects

        # Parse Emails
        self.getEmailsAndProjects()

        # Deprocated
        #     # Private Method Call
        #     self.convertSampleSheet()

    def runConfigureBclToFastq(self,bcl_options):

        # Change current Working Directory
        os.chdir(self.basecalls)
        print("Current working dir is %s" % os.getcwd())

        print("Running Configure Bcl to Fastq command")
        
        configureCommand = ["configureBclToFastq.pl",bcl_options,
                            "--output-dir","../../../" + \
                            self.bcl_output_dir,"--sample-sheet", \
                            os.path.basename(self.sample_sheet)]

        print("Configure Command: %s" % " ".join(configureCommand))
        subprocess.call(" ".join(configureCommand),shell=True)

        # Change to output dir
        os.chdir(self.run + "/" + self.bcl_output_dir)
        print("Running make command in %s" %(os.getcwd()))

        system_call(["make","-j","8"],"Make Failed",admin_message=True,extra=self,shell=True)

        # DEPROCATED
        # if self.undetermined:
        #     self.grabUndetermined()

    def bowtieProjects(self):

        output_dir = self.run + "/" + self.bcl_output_dir

        for project in self.projects:
            print("Working on Project %s" % project)

            for sample in self.projects[project]:

                # Using Calling Bowtie Module that was imported bowtie.py
                # As long as API remains the same this method will always work
                genome = self.projects[project][sample]["genome"]

                if not genome:
                    print("Skipping %s" % sample)
                    continue

                folder = output_dir + "/Project_" + project + "/Sample_" + sample

                print("\tSample %s" % sample)
                bowtie_folder(folder,indexes_genome=genome)

    def importProjects2Annoj(self):
        """
        """
        print("Starting upload of Samples to MySQL Database(s)")
        mysql_user     = "mysql"
        mysql_password = "rekce"

        bcl_output_dir = self.run + "/" + self.bcl_output_dir

        for project in self.projects:

            # Actual API call to Import2AnnojSimple
            for sample in self.projects[project]:
                print("Working on %s from Project %s" % (sample,project))

                destination = self.projects[project][sample]["destination"]
                database    = self.projects[project][sample]["database"]
                tdna        = self.projects[project][sample]["tdna"]
                sample_name = self.projects[project][sample]["sample_name_with_run_info"]
                input_file  = "../bowtie.R1.sam"

                if not destination and not database:
                    print("Skipping %s" % sample)
                    continue
                
                elif not database:
                    database = project

                elif not destination:
                    print("Skipping %s. No destination specified" % sample)
                    continue

                sample_dir = bcl_output_dir + "/Project_" + project + "/Sample_" + sample

                os.chdir(sample_dir)
                subprocess.call(["mkdir","annoj"])
                os.chdir("annoj")

                getChromosomeFiles(input_file,tdna_filter=tdna)
                upload2mysql(destination,database,sample_name,mysql_user,mysql_password,tdna_filter=tdna)

            # This Method is optimized only for TDNA 
            if tdna:
                self.getTrackDefintionsAndFetchers(project)

    def callTDNAPools(self):
        """
        The chromsomes must refer to the Assembly number that is in the SQL
        database.

        To do: remove dependencies on chromosome list!
        """

        tdna_projects = []

        # Get TDNA Projects
        for project in self.projects:
            if "tdna" in project.lower():
                tdna_projects.append(project)

        # Now For each Project Call Pools
        for tdna_project in tdna_projects:
            # Get chromosomes from project
            project_chromsomes = tdna_projects[tdna_project]["chromosomes"]

            # Check to make sure every pool has the SQL information
            samples = [x for x in self.projects[tdna_project] if self.projects[tdna_project][x]["destination"] != ""]
            samples.sort(key=lambda x:(x[0],int(x[1:3])))

            if len(samples) != len(self.projects[tdna_project]):
                print("Not all Samples have import information. Skipping %s" % tdna_project)
                continue

            # Format Samples
            samples_and_sql_info = defaultdict(dict)
            for sample in samples:
                samples_and_sql_info[sample]["user"] = "mysql"
                samples_and_sql_info[sample]["password"] = "rekce"
                samples_and_sql_info[sample]["host"] = self.projects[tdna_project][sample]["destination"]
                samples_and_sql_info[sample]["table"] = self.projects[tdna_project][sample]["database"] + "." + self.projects[tdna_project][sample]["sample_name_with_run_info"]

            # Call Pools. The same as to HTML Pipeline Implementation
            output_file_name = os.path.join(self.run,self.bcl_output_dir)
            output_file_name = os.path.join(output_file_name,"Project_" + tdna_project)
            output_file_name = os.path.join(output_file_name,"Called_Pools.out")

            with open(output_file_name,"w") as pools_output:
                print("Calculating Pools for %s" % (tdna_project))
                for chromosome in project_chromsomes:
                    chromosome_name = "chr"+str(chromosome)

                    print("\t" + chromosome_name)
                    print("\tGenerating Chromsome Data Frame")

                    chrom_frame = fillChromosomeFromMySQL(samples_with_sql_information=samples_and_sql_info,
                                                          chromosome=chromosome_name,
                                                          min_reads=2)

                    # There must be at least 4 non NA's in a column
                    # Replace NA's in column with 0's
                    print("\tCleaning")
                    chrom_frame = chrom_frame.dropna(axis=1,thresh=4)
                    chrom_frame = chrom_frame.fillna(0)

                    # Start Calling Pools from Columns
                    print("\tCalling Pools")
                    pool_caller(chrom_frame,pools_output,chromosome_name)
                    pool_cleaner(output_file_name)

            # From Called Pools Output All the pools and the number of times they 
            # had hits.
            samples = [x[:3] for x in samples]
            pools_frequencies = {sample:0 for sample in samples}

            print("Getting Frequencies")
            with open(output_file_name + ".clean","r") as called_pools:
                for line in called_pools:
                    row = line.strip().split(",")
                    pools = row[2:]

                    for pool in pools:
                        pools_frequencies[pool] += 1

            with open(output_file_name + ".clean.freqs","w") as pools_freqs:

                pools = pools_frequencies.keys()[:]
                pools.sort(key=lambda x:(x[0],int(x[1:])))

                for pool in pools:
                    freq = str(pools_frequencies[pool])
                    to_write = " ".join([pool,freq])
                    pools_freqs.write(to_write + "\n")

    def bclStartEmailBlast(self):
        """
        """
        try: 
            self.notifications
        except AttributeError:
            self.notifications = notifications()

        for email in self.emails_and_projects:

            subject = "Bcl Analysis Has Started for %s" % (os.path.basename(self.run))

            # I'm sorry this is a heinously long string
            # The Text would not format correctly if not done this way :-)
            message = """Just a friendly reminder from GAL-E to let you know that the following run:\n\n%s has started it's Bcl Analysis.\n\nI'll send you an email with the path(s) to your project(s) on the server when it's done.\n""" % (os.path.basename(self.run))

            # API Note: send_message expects a list of email addresses.

            self.notifications.send_message(TO=[email],SUBJECT=subject,TEXT=message)

    def bclCompleteEmailBlast(self):

        try: 
            self.notifications
        except AttributeError:
            self.notifications = notifications()

        for email in self.emails_and_projects:

            subject = "Analysis Complete for %s!" % (os.path.basename(self.run))

            projects = [self.run + "/" + self.bcl_output_dir + "/Project_" + project for project in self.emails_and_projects[email].keys()]

            message = "%s has completed its analysis!\n\nHere are the Paths to your Projects:\n\n%s" % (os.path.basename(self.run),"\n".join(projects))

            self.notifications.send_message(TO=[email],SUBJECT=subject,TEXT=message)

    def adminRunInfoBlast(self,subject_message,inner_message):
        """
        """
        try:
            self.notifications
        except AttributeError:
            self.notifications = notifications()

        projects = ["- " + project for project in self.projects.keys()]
        projects.sort()

        subject = "%s for %s" % (subject_message,os.path.basename(self.run))

        message = "%s for %s\n\nPATH on %s:\n%s\n\nPROJECTS:\n%s" % (inner_message,os.path.basename(self.run),self.host.upper(),self.run,"\n".join(projects))

        for address in self.notifications.admin:

            self.notifications.send_message(TO=[address],SUBJECT=subject,TEXT=message)

    # ---- Private Methods
    # These are subroutines that the public methods call
    # Modify here at your own risk

    @staticmethod
    def parseBowtieAndAnnojOptions(bowtie_and_annoj_options):

        options = bowtie_and_annoj_options.split(";")
        try:
            genome      = options[0]
            destination = options[1]
            database    = options[2]

        except IndexError:
            """
            If the Code hits here it is very probable that the code is handmade, or at least
            some of it is.
            """
            genome      = ""
            destination = ""
            database    = ""

        try:
            barcode1 = options[3]
        except IndexError:
            barcode1 = ""

        try:
            barcode2 = options[4]
        except:
            barcode2 = ""


        return {"genome":genome, "destination":destination,"database":database,"barcode1":barcode1,"barcode2":barcode2}

    # Deprocated
    def convertSampleSheet(self,number_of_lanes=8):
        print("Found Samples without Indexes")
        print("Saving new SampleSheet as _tmp.csv")
        print("Old SampleSheet will be unmodified")

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

        # New Sample sheet
        sample_sheet_no_extension = os.path.splitext(os.path.basename(self.sample_sheet))[0]
        sample_sheet_no_extension += "_tmp.csv"


        with open(self.basecalls + "/" + sample_sheet_no_extension,"w") as output_file:

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

        self.sample_sheet = os.path.abspath(sample_sheet_no_extension)
        self.undetermined = True

        print("Will now use %s as SampleSheet" % sample_sheet_no_extension)

    # Deprocated
    def grabUndetermined(self):
        """
        # NOTE: This code has since been deprocated. It is still avaiable in case there is a need for this method
        This code is ugly as sin. Sorry :-(
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

            read_not_pair_end = False

            lane_path = os.getcwd() + "/Sample_lane" + lane 
            os.chdir(lane_path)
            print("Now in %s" % os.getcwd())

            # If there are gz_files...gunzip them!
            gz_files = [x for x in os.listdir(os.getcwd()) if ".gz" in x]

            if len(gz_files) != 0:
                print("Found gz files. Uncompressing them.")
                gunzip = "gunzip *.gz"
                subprocess.call(gunzip,shell=True)

            # Get All the R1's and R2's
            R1 = [x for x in os.listdir(os.getcwd()) if "_R1_" in x]
            R2 = [x for x in os.listdir(os.getcwd()) if "_R2_" in x]

            R1.sort()
            R2.sort()

            R1andR2 = [(x,y) for x,y in zip(R1,R2)]

            if len(R2) != 0:
                # Since this step takes for ever, check to make sure
                # if this has already been done. If not do it.

                fastq_files = [x for x in os.listdir(os.getcwd()) if "R1" in x and "R2" in x and ".fastq" in x]

                if len(fastq_files) == 0:
                    print("Combining R1's and R2's in lane %s" % (lane))

                    for pair in R1andR2:
                        R1_name = pair[0]
                        R2_name = pair[1]

                        match       = re.search("[0-9][0-9][0-9](?=[.fastq])",R1_name)
                        pair_number = match.group(0)

                        paste_command = ["paste",R1_name,R2_name,"> R1_R2_" + pair_number + ".fastq"]
                        paste_command = " ".join(paste_command)
                        subprocess.call(paste_command,shell=True)

                    fastq_files = [x for x in os.listdir(os.getcwd()) if "R1" in x and "R2" in x and ".fastq" in x]

                else:
                    print("R1 and R2's already combined. Skipping step")

            else:
                print("Run is not Pair End ->")
                read_not_pair_end = True
                fastq_files = [x for x in os.listdir(os.getcwd()) if "R1" in x and ".fastq" in x]

            # Start going through samples and pulling them out
            print("Pulling Out Samples:")
            for sample in lanes[lane]:

                project     = sample["project"]
                sample_name = sample["sample_name"]
                sample_lane = sample["lane"]
                barcode1    = sample["barcode1"]
                barcode2    = sample["barcode2"]

                barcode1_length = len(barcode1)
                barcode2_length = len(barcode2)

                print "\t",sample_name,barcode1,barcode2

                # If the stuff isn't barcoded. Skip
                if barcode1_length == 0:
                    continue

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
                    

                    print("\tWorking on %s in lane %s" % (sample_name,sample_lane))
                   # Loop through combined R1and R2 files
                    for r1andr2 in fastq_files:

                        with open(r1andr2,"r") as fastq_file:

                            for i,line in enumerate(fastq_file):

                                if i % 4 == 0:
                                    readID = line.strip().split()[0]

                                elif i % 4 == 1:
                                    sequences = line.strip().split()

                                    sequence1 = sequences[0]
                                    sequence_barcode1 = sequence1[:barcode1_length]
                                    
                                    if read_not_pair_end == False:
                                        sequence2 = sequences[1]
                                        sequence_barcode2 = sequence2[:barcode2_length]

                                elif i % 4 == 2:
                                    qual1 = line.strip()

                                elif i % 4 == 3:
                                    qual2 = line.strip()

                                    if (read_not_pair_end == False and sequence_barcode1 == barcode1 and sequence_barcode2 == barcode2) \
                                            or (read_not_pair_end == True and sequence_barcode1 == barcode1):
                                        
                                        output_file.write(readID.strip() + "\n")

                                        # Removing Barcodes
                                        output_file.write(sequence1[barcode1_length:].strip() + "\n")

                                        # Since the barcodes are removed the Quality information needs to
                                        # be truncated too!
                                        output_file.write(qual1.split()[0].strip() + "\n" )
                                        output_file.write(qual2.split()[0].strip()[barcode1_length:] + "\n")


            # Change Directory back to Top of Undetermined
            os.chdir(undetermined_indices_path)

    # Deprocated
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

    def getEmailsAndProjects(self):
        """
        Go through projects and create a hash where Keys are Emails and
        Values are the projects associated with that email
        """

        emails_and_projects = defaultdict(dict)

        for project in self.projects:

            for sample in self.projects[project]:
                email_address = self.projects[project][sample]["owner_email"]

                if project not in emails_and_projects[email_address]:
                    emails_and_projects[email_address][project] = [sample]

                else:
                    emails_and_projects[email_address][project].append(sample)

        self.emails_and_projects = emails_and_projects

        try: 
            self.notifications
        except AttributeError:
            self.notifications = notifications()

    def getTrackDefintionsAndFetchers(self,project):
        """
        This is optimized for TDNA 20k, 30k etcetc formats ONLY
        """
        fetcher_dir = """FETCHER_DIR"""
        top_dir = os.getcwd()

        # Make dir
        os.chdir(os.path.join(self.run,os.path.join(self.bcl_output_dir,"Project_" + project)))
        make_dir = "mkdir complete_fetchers_and_definitions"
        subprocess.call(make_dir,shell=True)
        os.chdir("complete_fetchers_and_definitions")

        # Assuming that the first number in a sample name relates to the
        # K numbers... eg all the 20's belong to one pool, 30's to another
        pools = []

        for sample in self.projects[project]:
            index = self.indexOfFirstDigit(sample)
            pool_number = sample[index]

            if pool_number not in pools:
                pools.append(pool_number)

        # loop through pools and make Track definitions for
        # non filtered and Filtered
        for pool in pools:
            regular_track_definitions     = open(project + "." + pool + ".trackdefintions","w")
            tdna_filter_track_definitions = open(project + "." + pool + ".tdnafilter.trackdefinitions","w")

            active_filter = []
            active        = []

            regular_track_definitions.write("tracks : [\n\n")
            regular_track_definitions.write("""//Models\n{\nid   : 'tair9',\nname : 'Gene Models',\ntype : 'ModelsTrack',\npath : 'Annotation models',\ndata : '../../fetchers/models/tair9.php',\nheight : 80,\nshowControls : true\n},\n""")
            tdna_filter_track_definitions.write("tracks : [\n\n")
            tdna_filter_track_definitions.write("""//Models\n{\nid   : 'tair9',\nname : 'Gene Models',\ntype : 'ModelsTrack',\npath : 'Annotation models',\ndata : '../../fetchers/models/tair9.php',\nheight : 80,\nshowControls : true\n},\n""")

            for sample in self.projects[project]:
                if sample[self.indexOfFirstDigit(sample)] != pool:
                    continue
                
                run_path = self.run
                run_name = os.path.basename(self.run[:-1]) if self.run[-1] == "/" else os.path.basename(self.run)
                sample += "_" + run_name

                tablename = sample
                active.append(tablename)
                active_filter.append(tablename + "_tDNA_Filter")

                # Non Filtered
                regular_track_definitions.write("{\n")
                regular_track_definitions.write(" id: '%s',\n"   % tablename)
                regular_track_definitions.write(" name: '%s',\n" % tablename)
                regular_track_definitions.write(" type: 'ReadsTrack',\n")
                regular_track_definitions.write(" path: 'NA',\n")
                regular_track_definitions.write(" data: '%s/%s',\n" % (fetcher_dir,tablename + ".php"))
                regular_track_definitions.write(" height: '25', \n")
                regular_track_definitions.write(" scale: 0.1\n")
                regular_track_definitions.write("},\n")

                # Filtered
                tdna_filter_track_definitions.write("{\n")
                tdna_filter_track_definitions.write(" id: '%s',\n"   % (tablename + "_tDNA_Filter"))
                tdna_filter_track_definitions.write(" name: '%s',\n" % (tablename + "_tDNA_Filter"))
                tdna_filter_track_definitions.write(" type: 'ReadsTrack',\n")
                tdna_filter_track_definitions.write(" path: 'NA',\n")
                tdna_filter_track_definitions.write(" data: '%s/%s',\n" % (fetcher_dir,tablename + "_tDNA_Filter.php"))
                tdna_filter_track_definitions.write(" height: '25', \n")
                tdna_filter_track_definitions.write(" scale: 0.1\n")
                tdna_filter_track_definitions.write("},\n")

        regular_track_definitions.write("\n\n],\n\nactive : [\n'tair9',")
        tdna_filter_track_definitions.write("\n\n],\n\nactive : [\n'tair9',")


        # This will Sort alphabetically then numerically
        active.sort(key= lambda x:(x[0],int(x[1:3])))
        active_filter.sort(key= lambda x:(x[0],int(x[1:3])))

        for sample in active:
            regular_track_definitions.write("'" + sample + "',")

        for sample in active_filter:
            tdna_filter_track_definitions.write("'" + sample + "',")

        regular_track_definitions.write("\n],\n")
        tdna_filter_track_definitions.write("\n],\n")

        # Dump The Fetchers
        for sample in self.projects[project]:
            run_path = self.run
            run_name = os.path.basename(self.run[:-1]) if self.run[-1] == "/" else os.path.basename(self.run)

            tablename = sample + "_" + run_name
            database  = self.projects[project][sample]["database"]
            host      = self.projects[project][sample]["destination"]
            tdna      = self.projects[project][sample]["tdna"]
            includes_dir = "../../includes"

            with open(tablename + ".php","w") as fetcher:
                fetcher.write("<?php\n")
                fetcher.write("$append_assembly = false;\n")
                fetcher.write("$table = '%s.%s';\n" % (database,tablename) )
                fetcher.write("$title = '%s';\n" % (tablename))
                fetcher.write("$info = '%s';\n"  % (tablename.replace("_"," ")))
                fetcher.write("""$link = mysql_connect("%s","mysql","rekce") or die("failed");\n""" % (host))
                fetcher.write("require_once '%s/common_reads.php';\n" % (includes_dir))
                fetcher.write("?>\n")


            tablename = tablename + "_tDNA_Filter"
            with open(tablename + ".php","w") as fetcher:
                fetcher.write("<?php\n")
                fetcher.write("$append_assembly = false;\n")
                fetcher.write("$table = '%s.%s';\n" % (database,tablename) )
                fetcher.write("$title = '%s';\n" % (tablename))
                fetcher.write("$info = '%s';\n"  % (tablename.replace("_"," ")))
                fetcher.write("""$link = mysql_connect("%s","mysql","rekce") or die("failed");\n""" % (host))
                fetcher.write("require_once '%s/common_reads.php';\n" % (includes_dir))
                fetcher.write("?>\n")

        os.chdir(top_dir)

    @staticmethod
    def indexOfFirstDigit(string):
        for i,char in enumerate(string):
            if char.isdigit():
                return i
        else:
            return -1


if __name__=="__main__":
    print("Testing...")

    p = project(run_path       = "/mnt/thumper-e4/illumina_runs/130405_LAMARCK_3152_BC1N7KACXX",\
                sample_sheet   = "SampleSheet.csv",\
                bcl_output_dir = "UnalignedTDNA")

    print("Parsing Sample Sheet")
    p.parseSampleSheet()
    p.callTDNAPools("SampleSheet_130405_Lamarck_3153.csv")
