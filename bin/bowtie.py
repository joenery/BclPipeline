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

    if val !=0 :
        print("".join(["\n",err_message,"\n","Terminating Script"]))
        sys.exit(1)

def bowtie_folder(folder,options="--local -p 8",bowtie_shell_call="bowtie2",indexes_folder="/home/seq/bin/bowtie2/INDEXES/",indexes_genome="tair10"):
    """
    This function takes in a folder and creates a list of fastq files within that folder. If the folder has no fastq's the function returns None.
    """
    
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
        gunzip = ["gz","*.fastq.gz"]

        subprocess.call(gunzip)

    # Getting Fastq's and prepping 

    # Make Sure to add bowtie output logs that contain
    # what the call was to bowtie etc

    # check for Pair end reads
    fastqs_R1 = [folder + "/" + x for x in os.listdir(folder) if "R1" in x and ".fastq" in x]
    fastqs_R2 = [folder + "/" + x for x in os.listdir(folder) if "R2" in x and ".fastq" in x]

    if len(fastqs) == 0:

        fastqs_R1 = [x for x in os.listdir(folder) if ".fastq" in x and "R1" not in x and "R2" not in x]

        if len(fastqs_R1) == 0:
            print("No fastqs in folder!")
            return

    # Prepping Command
    command_R1 = [bowtie_shell_call,options,indexes_folder+ "/" + indexes_genome,",".join(fastqs_R1),"1> bowtie.R1.sam 2> bowtie.stats"]
    command_R2 = [bowtie_shell_call,options,indexes_folder+ "/" + indexes_genome,",".join(fastqs_R2),"1> bowtie.R2.sam 2> bowtie.stats"]

    print("Bowtie-ing %s" % folder)
    print " ".join(command),os.getcwd()

    if len(fastqs_R2) == 0:
        system_call(command,"Died at Bowtie2 step")

    else:
        system_call(command,"Died at Bowtie2 R1 step")
        system_call(command,"Died at Bowtie2 R2 step")

    print("Finished Bowtie-ing %s" % folder)

if __name__ == "__main__":
    parser = MyParser(description="Given a folder and optionally a configuration file Bowtie will be performed on all\
                                   the relative files in that folder")
