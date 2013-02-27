#!/usr/bin/env python

import os
import sys
import csv
import subprocess
from collections import defaultdict
import re

def illegal_char_replace(string):
    illegal_bcl_characters = """ ?()[]\=+<>:;"',*|^&`"""

    string = string.replace("/","_").replace("-","_").replace(".","_").replace(" ","_")

    return string

if __name__=="__main__":
    
    try:
        if not os.path.isfile(sys.argv[1]):
            print("CSV file doesn't exist")
            sys.exit(1)

        csv_file = sys.argv[1]

    except IndexError:
        print("")
        print("adapterTrackerMonthlyParser.py <Monthly CSV Adapter Track file>")
        print("")
        
        sys.exit(1)


    runs = defaultdict(list)

    # get Barcodes
    barcodes_path = os.path.split(sys.argv[0])[0] + "/barcodes"

    if os.path.isfile(barcodes_path):

        barcodes = {}

        with open(barcodes_path,"r") as barcodes_file:
            for line in barcodes_file:

                row = line.strip().split()
                
                key = row[1]
                value = row[2]

                barcodes[key] = value 


    # Parse the runs in the file
    with open(csv_file,"rU") as csvfile:

        adapter_month = csv.reader(csvfile)

        for row in adapter_month:

            run_name = row[11]

            if run_name == "" or run_name == "Run":
                continue

            runs[run_name].append(row)

    # Loop through runs and Create Sample Sheets

    for k in runs.keys():

        with open("SampleSheet_" + k + ".csv","w") as sample_sheet_out:

            sample_sheet_out.write("FCID,Lane,Sample_ID,Sample_Ref,Index,Descripton,Control,Recipe,Operator,Sample_Project\n")

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
                barcode1         = row[13]
                barcode2         = row[14]

                if barcode1 in barcodes:
                    barcode1_sequence = barcodes[barcode1]
                else:
                    barcode1_sequence = ""

                if barcode2 in barcodes:
                    barcode2_sequence = barcodes[barcode2]
                else:
                    barcode2_sequence = ""


                sample_sheet_out.write(",".join(["",lane,sample_name,illumina_adapter,illumina_adapter_sequence,bowtie + ";" + mysql_host + ";" + mysql_database + ";"
                                                  + barcode1_sequence + ";" + barcode2_sequence,"","",email_address,project + "\n"]))
