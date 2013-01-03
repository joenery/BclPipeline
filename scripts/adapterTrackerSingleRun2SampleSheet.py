import os
import sys
import csv


if __name__=="__main__":
    """
    Input file is a csv from the GAL-E Adapter adapter_tracker

    Output is to STDOUT
    """

    if not os.path.isfile(sys.argv[1]):
        print("Couldn't Find that File!")
        sys.exit(1)

    with open(sys.argv[1],"r") as csvfile:

        adapter_tracker = csv.reader(csvfile)

        # Header
        print("D,Lane,Sample,Sample_Ref,Index,Descriptor,Control_lane,Recipe,Operator,Project")

        for row in adapter_tracker:

            owner            = row[1]
            email_address    = row[2]
            illumina_adapter = row[3]
            project          = row[4]
            sample_name      = row[5]
            illumina_adapter_sequence = row[6]
            bowtie           = row[8]
            mysql_host       = row[9]
            mysql_database   = row[10]
            lane             = row[12]

            print(",".join(["",lane,sample_name,illumina_adapter,illumina_adapter_sequence,bowtie + ";" + mysql_host + ";" + mysql_database,
                            "","",email_address,project]))




