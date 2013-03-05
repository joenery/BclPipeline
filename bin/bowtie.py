import os
import sys
import subprocess

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

def bowtie_folder(folder,options="--local -p 4",bowtie_shell_call="bowtie2",indexes_folder="/home/seq/bin/bowtie2/INDEXES/",indexes_genome="tair10"):
    """
    This function takes in a folder and creates a list of fastq files within that folder. If the folder has no fastq's the function returns None.
    """
    
    # Is folder formatted properly?
    if folder[-1] != "/":
        folder = folder + "/"

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

    # Getting Fastq's and prepping command

    fastqs = [folder + x for x in os.listdir(folder) if os.path.splitext(x)[1] == ".fastq"]

    if len(fastqs) == 0:
        print("No fastq files found in %s." % folder)
        return

    command = [bowtie_shell_call,options,indexes_folder+indexes_genome,",".join(fastqs),"1> bowtie.out.sam 2> bowtie.stats"]

    print("Bowtie-ing %s" % folder)
    system_call(command,"Died at Bowtie2 step")
    print("Finished Bowtie-ing %s" % folder)

if __name__ == "__main__":
    print("Testing...")

    bowtie_folder(folder="/mnt/thumper-e1/home/jfeeneysd/130108_JONAS_2137_AD16YKACXX/Unaligned_Anna/Project_DAP/Sample_ANAC029/",indexes_genome="tair10")