from import2annojsimple import *
import os
import sys
import subprocess


if __name__=="__main__":
    
    try:
        f = sys.argv[1]
    except IndexError:
        sys.exit(1)

    # Change to dir
    if os.path.isdir(f):
        os.chdir(f)
        print(os.getcwd())
        top = os.getcwd()

        list_of_folders = [x for x in os.listdir(os.getcwd()) if os.path.isdir(x)]
        print list_of_folders

        for folder in list_of_folders:
            folder_path = os.path.join(os.getcwd(),folder)

            os.chdir(folder_path)
            sample = os.path.basename(folder_path).replace("Sample_","")
            local2mysql("bowtie.R1.sam","thumper-e1","tDNA",sample,"mysql","rekce")
            
            # Set path back to top
            os.chdir(top)
