import subprocess
import os
import sys
from collections import defaultdict
import re
from socket      import gethostname
import csv

# My modules
from bowtieSimple       import bowtie_folder
from import2annojsimple import *
from emailnotifications import notifications
from genomes            import genomes
from tdna_seq_caller    import fillChromosomeFromMySQL,pool_caller,pool_cleaner
from parseGFF           import *


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

class GffError( Exception ):
    pass


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

        # Get Script Location
        path_to_script = os.path.realpath(sys.argv[0])
        path_to_script_dir = os.path.split(path_to_script)[0]
        
        self.script_dir = path_to_script_dir

    def parseSampleSheet(self):
        """
        """
        projects = defaultdict(dict)
        project_chromosomes = {}
        samples_with_no_projects = False

        # You can add more genomes to the genomes module and
        # they will be imported here!
        g = genomes()

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

                # Get Number of Chromosomes
                try:
                    chromosomes = g.genomes[genome]
                except KeyError:
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

                project_chromosomes[project] = chromosomes[:]

        # Save info to object
        if samples_with_no_projects:
            print("Samples with no projects will result in emails to owners without paths to their files.")

        self.projects = projects
        self.project_chromosomes = project_chromosomes

        # Parse Emails
        self.getEmailsAndProjects()

        

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


    def bowtieProjects(self,processors=10):

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
                bowtie_folder(folder,indexes_genome=genome,options="--local -p %s" % processors)

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
           
            self.getTrackDefintionsAndFetchers(project,is_tdna=tdna)

    
    def callTDNAPools(self,debug=False):
        """
        The chromsomes must refer to the Assembly number that is in the SQL
        database. Chromosomes can be inferred from the genome selected to bowtie
        to from the Sample Sheet.

        If a genome or name of genome is not present you can add these
        """

        tdna_projects = []

        # Get TDNA Projects
        for project in self.projects:
            if "tdna" in project.lower():
                tdna_projects.append(project)

        # Now For each Project Call Pools
        for tdna_project in tdna_projects:
            # Get chromosomes from project
            if not debug:
                project_chromosomes = self.project_chromosomes[tdna_project]
            else:
                project_chromosomes = [1]

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

            # Create object for RAW output.
            raw_output_file = open(output_file_name.replace(".out",".raw.out"),"w")

            with open(output_file_name,"w") as pools_output:
                print("Calculating Pools for %s" % (tdna_project))

                for chromosome in project_chromosomes:
                    chromosome_name = "chr"+str(chromosome)
                    print(chromosome)
                    print("\tGenerating Chromsome Data Frame")

                    chrom_frame,positions_directions = fillChromosomeFromMySQL(samples_with_sql_information=samples_and_sql_info,
                                                          chromosome=chromosome_name,
                                                          debug=debug)

                    # There must be at least 4 non NA's in a column
                    # Replace NA's in column with 0's
                    print("\tCleaning")
                    chrom_frame = chrom_frame.dropna(axis=1,thresh=3)
                    chrom_frame = chrom_frame.fillna(0)

                    # Start Calling Pools from Columns
                    print("\tCalling Pools")
                    pool_caller(chrom_frame,
                                pools_output,
                                chromosome=chromosome_name,
                                min_percentage=0.92,
                                min_reads=2,
                                min_distance=50,
                                debug=debug,
                                raw_output=raw_output_file,
                                positions_directions=positions_directions)

            print("\tCleaning Pools")
            pool_cleaner(output_file_name)

            # From Called Pools Output All the pools and the number of times they 
            # had hits.
            samples = [x[:3] for x in samples]
            pools_frequencies = {sample:0 for sample in samples}

            print("\tGetting Frequencies")
            with open(output_file_name,"r") as called_pools:
                for line in called_pools:
                    row = line.strip().split(",")
                    pools = [x[:3] for x in row[2:-1]]

                    for pool in pools:
                        pools_frequencies[pool] += 1

            with open(output_file_name + ".freqs","w") as pools_freqs:

                pools = pools_frequencies.keys()[:]
                pools.sort(key=lambda x:(x[0],int(x[1:])))

                for pool in pools:
                    freq = str(pools_frequencies[pool])
                    to_write = " ".join([pool,freq])
                    pools_freqs.write(to_write + "\n")

            raw_output_file.close()

    def _loadGenomeAnnotations(self,genome,chromosomes_output):
        """
        Args:
            genome             -> Name of the genome to load into memory
            chromosomes_output -> an empty dictionary. Method will use pass by reference

        Returns:
            nothing
        """
        
        # Is the genome in chromosome annotations?
        chromosome_annotations_dir = os.path.join(self.script_dir,"Chromosome_Annotations")
        
        available_annotations = [x.lower() for x in os.listdir(chromosome_annotations_dir) if os.path.isdir(os.path.join(chromosome_annotations_dir,x))]
        available_gffs = [x for x in os.listdir(chromosome_annotations_dir) if "gff" in x.lower()]
        
        gff = [x for x in available_gffs if genome in x.lower()]

        if not genome.lower() in available_annotations and gff :
            # Check to see if there is a gff to make annotations
            gff = os.path.join(chromosome_annotations_dir,gff[0])

            print("Creating annotations for %s" % genome)
            parseGFF(gff)

        elif not genome.lower() in available_annotations and not gff:
            raise GffError("Could not find an appropriate gff or annotations for %s" % genome)

        genome_annotations_dir = os.path.join(chromosome_annotations_dir,genome)

        chromosomes = [os.path.join(genome_annotations_dir,x) for x in os.listdir(genome_annotations_dir)]

        # Prep output dictionary
        for chromosome in chromosomes:
            chromosome = os.path.basename(chromosome)

            chromosomes_output[chromosome] = {}

        # Load Chromosomes into Memory
        for chromosome in chromosomes:
            print("Loading %s into memory." % chromosome)
            
            with open(chromosome,"r") as chrm:
                chromosome = os.path.basename(chromosome)

                for line in chrm:
                    row             = line.strip().split()
                    position        = row[0]
                    positive_strand = row[1]
                    negative_strand = row[2]
                    
                    chromosomes_output[chromosome][position] = (positive_strand,negative_strand)

                
    def _annotatePosition(self,chromosomes,position):
        """
        This is a very general method that will
        """

    
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


    def getTrackDefintionsAndFetchers(self,project,is_tdna):
        """
        This method is called from the ImportProjectToAnnoj method.
        This is a general method for all projects. If the project is
        TDNA then the specialised method will run.
        
        Args:
        project: reference to a project in the self.projects dict
        is_tdna: a boolean representing if the projects is TDNA
        """
        print("Getting Track Definitions and fetchers for %s" % (project))

        if is_tdna:
            self.getTrackDefintionsAndFetchersTDNA(project)
            return

        # General Method
        fetcher_dir = """FETCHER_DIR"""
        top_dir = os.getcwd()

        # Make dir
        os.chdir(os.path.join(self.run,os.path.join(self.bcl_output_dir,"Project_" + project)))
        make_dir = "mkdir complete_fetchers_and_definitions"
        subprocess.call(make_dir,shell=True)
        os.chdir("complete_fetchers_and_definitions") 

        # Now The script is in the complete_fetchers_and_defintions directory
        # Method will now prep the track_definitions HTML snippet
        samples = self.projects[project].keys()[:]
        samples.sort()

        with open(project + ".trackdefintions","w") as track_definitions:
            track_definitions.write("tracks : [\n\n")
            track_definitions.write("""//Models\n{\nid   : 'tair9',\nname : 'Gene Models',\ntype : 'ModelsTrack',\npath : 'Annotation models',\ndata : '../../fetchers/models/tair9.php',\nheight : 80,\nshowControls : true\n},\n""")
            
            # Write out Sample information to the Track Definitions Page
            for sample in samples:
                run_path = self.run
                run_name = os.path.basename(self.run[:-1]) if self.run[-1] == "/" else os.path.basename(self.run)
                tablename = sample + "_" + run_name

                track_definitions.write("{\n")
                track_definitions.write(" id: '%s',\n"   % tablename)
                track_definitions.write(" name: '%s',\n" % tablename)
                track_definitions.write(" type: 'ReadsTrack',\n")
                track_definitions.write(" path: 'NA',\n")
                track_definitions.write(" data: '%s/%s',\n" % (fetcher_dir,tablename + ".php"))
                track_definitions.write(" height: '25', \n")
                track_definitions.write(" scale: 0.1\n")
                track_definitions.write("},\n")
                

            # Finish Defintitions by printing them to the active tracks
            track_definitions.write("\n\n],\n\nactive : [\n'tair9',")

            for sample in samples:
                track_definitions.write("'" + sample + "_" + run_name  + "',")
            track_definitions.write("\n],\n")

        # Create The fetchers for each Individual Sample
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


    def getTrackDefintionsAndFetchersTDNA(self,project):
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
    """
    This method is NOT static and can be used for testing any feature you like.
    """
    print("Testing...")

    p = project(run_path       = "/mnt/thumper-e4/illumina_runs/130306_JONAS_2147_BD1RY6ACXX/",\
                sample_sheet   = "SampleSheet_DAP.csv",\
                bcl_output_dir = "UnalignedAnna")

    print("Parsing Sample Sheet")
    p.parseSampleSheet()
    #p.importProjects2Annoj()
    #p.getTrackDefintionsAndFetchers("DAP",False)
    chromosomes = {}
    p._loadGenomeAnnotations("tair10",chromosomes)
