#!/usr/local/bin/python

import os
import sys
import csv
import subprocess
from collections import defaultdict
import re

def illegal_char_replace(string):
    illegal_bcl_characters = """ ?()[]\=+<>:;"',*|^&`"""

    string = string.replace("/","_").replace("-","_")

    new_string = []

    for char in string:

        if char not in illegal_bcl_characters:
            new_string.append(char)

    return "".join(new_string)


if __name__=="__main__":
    
    try:
        if not os.path.isfile(sys.argv[1]):
            print("CSV file doesn't exist")
            sys.exit(1)

        csv_file = sys.argv[1]
        month = sys.argv[2].lower()

        if month not in ["january","february","march","april","may","june","july","august","september","october","november","december"]:
            print("You typed in a weird month! Try again :-]")
            sys.exit(1)

    except IndexError:
        print("")
        print("adapterTrackerMonthlyParser.py <Monthly CSV Adapter Track file> <Month of File>")
        print("")
        
        sys.exit(1)


    runs = defaultdict(list)

    # Parse the runs in the file
    with open(csv_file,"rU") as csvfile:

        adapter_month = csv.reader(csvfile)

        for row in adapter_month:

            run_name = row[11]

            if run_name == "" or run_name == "Run":
                continue

            runs[run_name].append(row)

    # make the folder
    command = "mkdir %s" % (month.capitalize())
    subprocess.call(command,shell=True)

    # Change cwd to that folder
    os.chdir(os.getcwd() + "/"+ month.capitalize())

    # Loop through runs and Create Sample Sheets

    illegal_bcl_characters = """?()[]/\|=+<>:;"',*^&"""

    for k in runs.keys():

        with open("SampleSheet_" + k + ".csv","w") as sample_sheet_out:

            sample_sheet_out.write("D,Lane,Sample,Sample_Ref,Index,Descriptor,Control_lane,Recipe,Operator,Project\n")

            for row in runs[k]:

                owner            = row[1]
                email_address    = row[2]
                illumina_adapter = illegal_char_replace(row[3])
                project          = illegal_char_replace(row[4])
                sample_name      = illegal_char_replace(row[5])
                illumina_adapter_sequence = row[6]
                bowtie           = row[8]
                mysql_host       = row[9]
                mysql_database   = row[10]
                lane             = row[12]

                sample_sheet_out.write(",".join(["",lane,sample_name,illumina_adapter,illumina_adapter_sequence,bowtie + ";" + mysql_host + ";" + mysql_database,
                                "","",email_address,project + "\n"]))
