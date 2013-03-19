#!/usr/bin/env python

import os
import sys
import subprocess
import argparse

class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

def system_call(command,err_message):
    """
    A wrapper for subprocess.call()

    It allows me to exit out of the script if the shell call results in an
    error code of 1 or greater.

    Hides this logic from the rest of the script since shell calls are made frequently
    """

    val = subprocess.call(command)

    if val != 0 :
        print("".join(["\n",err_message,"\n","Terminating Script"]))
        sys.exit(1)

def bowtie_folder(folder,options="--local -p 10",bowtie_shell_call="bowtie2",indexes_folder="/home/seq/bin/bowtie2/INDEXES/",indexes_genome="tair10"):
    """
    This function takes in a folder and creates a list of fastq files within that folder. If the folder has no fastq's the function returns None.
    """
    # Change in to Sample Folder
    os.chdir(folder)

    # Is folder formatted properly?
    if folder[-1] == "/":
        folder = folder[-1]

    # Does the INDEXES folder have the specified genome?
    indexes_check = [x for x in os.listdir(indexes_folder) if indexes_genome in x]

    if len(indexes_check) == 0:
        print("Could not find specified Genome (%s) in %s" % (indexes_genome,indexes_folder))
        return

    # Are there any gunzipped fastq files?
    gz = [x for x in os.listdir(folder) if ".gz" in x and ".fastq" in x]

    if len(gz) > 0:
        print("Gunzipping all fastq's in %s" % (folder))
        gunzip = "gz *.gz"

        subprocess.call(gunzip,shel=True)

    # Getting Fastq's and prepping 
    # check for Pair end reads
    fastqs_R1 = [folder + "/" + x for x in os.listdir(folder) if "R1" in x and ".fastq" in x]
    fastqs_R2 = [folder + "/" + x for x in os.listdir(folder) if "R2" in x and ".fastq" in x]

    if len(fastqs_R1) == 0:

        fastqs_R1 = [x for x in os.listdir(folder) if ".fastq" in x and "R1" not in x and "R2" not in x]

        if len(fastqs_R1) == 0:
            print("No fastqs in folder! Bye!")
            return

    # Print Verions Information to thee bowtie.stats ouput file
    # When Commands are called the will be echo'd to the bowtie.stats file
    bowtie_version = "%s --version > bowtie.stats" % (bowtie_shell_call)
    subprocess.call(bowtie_version,shell=True)

    # Prepping Commands
    command_R1 = [bowtie_shell_call,options,indexes_folder + "/" + indexes_genome,",".join(fastqs_R1),"1> bowtie.R1.sam 2>> bowtie.stats"]
    command_R2 = [bowtie_shell_call,options,indexes_folder + "/" + indexes_genome,",".join(fastqs_R2),"1> bowtie.R2.sam 2>> bowtie.stats"]

    # Begin Bowtie
    print("Bowtie-ing %s" % folder)
    echo_R1 = "echo %s >> bowtie.stats" % ("\t" + " ".join(command_R1))
    subprocess.call(echo_R1,shell=True)

    system_call(command_R1,"Died at Bowtie2 R1 step")

    if len(fastqs_R2) != 0:

        echo_R2 = "echo %s >> bowtie.stats" % ("\t" + " ".join(command_R2))
        subprocess.call(echo_R1,shell=True)

        system_call(command_R2,"Died at Bowtie2 R2 step")

    print("Finished Bowtie-ing %s" % folder)

def parseConfigFile(config_file):
    """
    Line1: Bowtie call or path to bowtie exceutable. I'll check if the call is either in your bash or if the exceutable exists
    Line2: Options you would like to give to Bowtie.
    Line3: Path to the bowtie index you would like to use. Formatted just like you would give to bowtie
    """
    warned_user = False

    with open(config_file,"r") as bowtie_config:

        for i,line in enumerate(bowtie_config):

            if i == 0:
                bowtie_shell_call = line.strip()

            elif i == 1:
                options = line.strip()

            elif i == 2:
                bowtie_indexes = line.strip()

            if i > 2 and warned_user == False:
                print("\nWoah. Your config file is more than three lines.\n It might not be the right file and bowtie will get angry with the options I give it!\n")

                warned_user = True
                continue

            else:
                pass

    # Check the bowtie shell call
    if not os.path.isfile(bowtie_shell_call) or not isExcecutableInBash(bowtie_shell_call):
        # Also check the user's path
        print("\nYour Bowtie Shell call doesn't seem to exist! I checked your $PATH, too!\n")
        sys.exit(1)

    # Check the INDEXES folder
    indexes_genome = os.path.basename(bowtie_indexes)
    indexes_folder = os.path.split(bowtie_indexes)[0]

    if not indexes_folder:
        print("\nI couldn't find the INDEXES folder you specfied!\n")
        sys.exit(1)

    genomes = [x for x in os.listdir(indexes_folder) if indexes_genome in x]
    
    if len(genomes) < 1:
        print("\nCould not find the genome you specified in the Indexes folder!\n")
        sys.exit(1) 

    return {"bowtie_shell_call":bowtie_shell_call,"indexes_genome":indexes_genome,"indexes_folder":indexes_folder,"options":options}

def parseBowtieIndexes(bowtie_indexes_path):
    """
    the Bowtie Folder function automatically checks the veracity
    of the information.

    This will simple split out the components the function is looking 

    indexes_folder
    indexes_genome
    """

    indexes_genome = os.path.basename(bowtie_indexes_path)
    indexes_folder = os.path.split(bowtie_indexes_path)[0]

    return indexes_folder,indexes_genome

def isExcecutableInBash(exceutable_name):
    """
    Checks folders in Unix $PATH variable for the exceutable
    Picked up this code from StackOverflow
    """
    for path in os.environ["PATH"].split(os.pathstep):
        path = path.strip('"')

        exceutable_file = os.path.join(path,exceutable_name)
        if os.path.isfile(exceutable_file):
            return True

    else:
        return False


if __name__ == "__main__":
    parser = MyParser(description="Given a folder and optionally a configuration file Bowtie will be performed on all\
                                   the relative files in that folder")

    mandatory = parser.add_argument_group("MANDATORY")
    advanced  = parser.add_argument_group("ADVANCED")

    mandatory.add_argument("-f","--folder",help="Absolute path to a folder that contains Fastq files that you would like to Bowtie")

    advanced.add_argument("-c","--config-file",help="Absolute path to a config file that you would like to use for your Bowtie call and options. DEFAULT: None",\
                          default=None)
    advanced.add_argument("-i","--bowtie-index-path",help="Path to the Bowtie Index and Genome you want to align to. DEFAULT: /home/seq/bin/bowtie2/INDEXES/tair10"\
                          ,default="/home/seq/bin/bowtie2/INDEXES/tair10")

    # -------- Parse Options
    command_line_options = vars(parser.parse_args())

    folder      = command_line_options["folder"]
    config_file = command_line_options["config_file"]
    bowtie_indexes_path = command_line_options["bowtie_index_path"] 

    # -------- Validate Options

    if not folder:
        parser.print_help()
        sys.exit(1)

    if not os.path.isdir(folder):
        print("\nThe Folder you gave doesn't exist!\n")
        sys.exit(1)

    if config_file and (not os.path.isfile(os.getcwd() + "/" + config_file) or not os.path.isfile(config_file)):
        print("\nThe config file you gave doesn't exist!\n")
        sys.exit(1)

    # -------- Set up the Options for bowtie_folder
    if config_file:
        parsed_options = parseConfigFile(config_file)

        bowtie_shell_call = parsed_options["bowtie_shell_call"]
        indexes_folder    = parsed_options["indexes_folder"]
        indexes_genome    = parsed_options["indexes_genome"]
        options           = parsed_options["options"]

    elif not config_file:

        bowtie_shell_call = "bowtie2"
        options           = "--local -p 10"
        indexes_folder,indexes_genome = parseBowtieIndexes(bowtie_indexes_path)

    # --------- Run the Bowtie Command!
    bowtie_folder(folder,options=options,bowtie_shell_call=bowtie_shell_call,indexes_folder=indexes_folder,indexes_genome=indexes_genome)