#!/usr/bin/env python

import os
import sys
import subprocess
from collections import defaultdict

def parseGFF(gff):
    """
    """
    # GLOBAL
    sections = set(["five_prime_UTR","CDS","three_prime_UTR"])
    genome_name = os.path.basename(gff).split("_")[0].lower()
    gff = os.path.abspath(gff)

    # Get Script Dir
    script_dir = os.path.abspath(os.path.split(sys.argv[0])[0])

    # Make Annotations Folder
    annotations_folder = os.path.join(script_dir,"Chromosome_Annotations")
    genome_folder      = os.path.join(annotations_folder,genome_name)

    if not os.path.isdir(annotations_folder):
        command            = "mkdir %s;mkdir %s" % (annotations_folder,genome_folder)
    else:
        command = "mkdir %s" % (genome_folder)

    subprocess.call(command,shell=True)


    # Change into that directory
    os.chdir(genome_folder)


    # First Pass to Get the different Chromosomes
    print("Gathering Chromosome Data")
    with open(gff,"r") as gff_in:
        chromosomes_in_gff = set()

        for line in gff_in:
            row = line.strip().split()
            chromosome = row[0]

            chromosomes_in_gff.add(chromosome)


    for gff_chrom in chromosomes_in_gff:
        print("Reading GFF for %s" % (gff_chrom))

        with open(gff,"r") as gff_in:

            chromosome_positions = {}

            # Reads Positions into Memory
            for line in gff_in:
                row = line.strip().split()

                chromosome    = row[0]
                gene_section  = row[2]
                section_start = int(row[3])
                section_end   = int(row[4])
                direction     = row[6]
                parent_info   = row[8]

                if chromosome != gff_chrom:
                    continue

                if gene_section not in sections:
                    continue

                gene = parent_info.split(",")[0].replace("Parent=","")
                gene = gene.split(".")[0]

                gene_info_to_print = ":".join([gene,gene_section])
                # Add these Positions to the Dictionary
                for position in xrange(section_start,section_end + 1):
                    # Does that position exist?
                    if not chromosome_positions.get(position,False):
                        chromosome_positions[position] = (set(),set())

                    # Pos on the right,Neg on the left
                    if direction == "+":
                        direction_index = 0
                    else:
                        direction_index = 1
                    
                    chromosome_positions[position][direction_index].add(gene_info_to_print)


            print("Writing GFF out to File for %s" % (gff_chrom))
            positions = chromosome_positions.keys()[:]
            positions.sort()

            with open(gff_chrom.replace("Chr","").replace("chr",""),"w") as chrom_out:
                for position in positions:

                    pos = list(chromosome_positions[position][0])
                    neg = list(chromosome_positions[position][1])

                    if not pos:
                        pos = ["Intergenic"]
                    if not neg:
                        neg = ["Intergenic"]

                    pos_to_print = ";".join(pos)
                    neg_to_print = ";".join(neg)

                    line_to_write = "\t".join([str(position), pos_to_print, neg_to_print]) + "\n"

                    chrom_out.write(line_to_write)


if __name__=="__main__":
    """
    Tested on the TAIR10 GFF
    """
    try:
        gff = sys.argv[1]

    except IndexError:
        print("You didn't give me a file!")
        sys.exit(1)

    parseGFF(gff)
