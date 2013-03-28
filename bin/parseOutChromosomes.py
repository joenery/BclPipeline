#!/usr/bin/env python
import subprocess
import os
import sys
import re

def parseChromosomesFromSam(sam):
    open_files    = {}

    print("Creating Chromosome Files")
    with open(sam,"r") as sam_chrom_count_object:

        passed_first_chromosome_header = False

        for i,line in enumerate(sam_chrom_count_object):
            row = line.strip().split()

            marker = "@SQ"

            if row[0] != marker and not passed_first_chromosome_header:
                # We are at the beginning of sam file
                continue

            elif row[0] == "@SQ":
                passed_first_chromosome_header = True
                chromosome = row[1].replace("SN:","")

                # Open These Files
                open_files[chromosome]    = open(chromosome.replace(":","_").replace("|","") + ".aj","w")

            elif row[0] != marker and passed_first_chromosome_header:
                break

            elif i > 10 and not passed_first_chromosome_header:
                # There aren't any headers in the sam file
                # Can't continue
                print("\nError: It looks like either the SAM file you've given doesn't have headers!\n")
                print("Please re-run Import2AnnojSimple with a SAM file that has headers\n")
                sys.exit(1)


    # --------------------- Parsing Sam File Aligns in to respective Chromosome Files ------ #
    print("Parsing Sam file for Alignments")
    with open(sam,"r") as sam_file:
        
        for i,line in enumerate(sam_file):

            # Create a hash of things to skip
            skip_these_lines = set()
            skip_these_lines.add("@HD")
            skip_these_lines.add("@SQ")
            skip_these_lines.add("@PG")
            skip_these_lines.add("*")
            skip_these_lines.add("chloroplast")
            skip_these_lines.add("mitochondira")
            skip_these_lines.add("ChrC")
            skip_these_lines.add("ChrM")

            header = line.strip().split()[0]

            if header in skip_these_lines:
                continue
            
            # Get Variables
            row         = line.strip().strip().split("\t")
            chromosome  = row[2]
            read_start  = row[3]
            snip_string = row[5]
            direction   = row[1]
            sequence    = row[9]

            # Skip unmapped reads 
            if chromosome in skip_these_lines:
                continue

            # From snip string get length of match and create end of read
            match            = re.search("([0-9][0-9](?=M)|[0-9][0-9][0-9](?=M))",snip_string)
            alignment_length = match.group(0)
            read_end         = str( int(read_start) + int(alignment_length) - 1 )

            # Change direction from Sam form to Annoj form
            if direction == "0":
                direction = "+"

            elif direction == "16":
                direction = "-"

            # Write to output
            if chromosome in open_files:

                open_files[chromosome].write("\t".join(row))

        # Close Chromosomes

        for f in open_files:
            open_files[f].close()


if __name__=="__main__":
    top_dir = os.getcwd()


    if not sys.argv[1:]:
        sys.exit(1)

    for f in sys.argv[1:]:

        abs_path_to_file = os.path.abspath(f)
        folder_name      = os.path.basename(f)

        create_folder = "mkdir %s" % (folder_name)
        subprocess.call(create_folder,shell=True)

        os.chdir(folder_name)

        parseChromosomesFromSam(abs_path_to_file)

        os.chdir(top_dir)



