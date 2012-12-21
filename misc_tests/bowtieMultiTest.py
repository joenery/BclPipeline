import os
import sys
import subprocess
from multiprocessing import Process

def worker(path):

    os.chdir(path)

    # command = "gunzip *.gz"

    # subprocess.call(command,shell=True)

    # Get Bowtie Files

    bowtie_files = [x for x in os.listdir(os.getcwd()) if "fastq" in x]

    print bowtie_files

    bowtie_command = "bowtie2 --local -p 4 /data/home/seq/bin/bowtie2/INDEXES/tair10 %s 1> bowtie2.out.sam 2> bowtie2.stats" % ",".join(bowtie_files)

    subprocess.call(bowtie_command,shell=True)


if __name__=="__main__":

    p_and_s = {"Test":[("1",""),("2",""),("3","")]}
    run = "/data/home/seq/bin/BclPipeline/bcl_test"

    for project in p_and_s.keys():

    	l = p_and_s[project]
    	#Create a list of the Sample folders
    	sample_folders = [ run + "/Unaligned/Project_"+ project + "/" + name[0]  for name in l]
    	
    	print sample_folders

    	jobs = sample_folders[:]

        processes = []

        for j in jobs:

            p = Process(target=worker,args=(j,))
            p.start()

            processes.append(p)

        for p in processes:
            p.join()    	

