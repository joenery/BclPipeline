#!/usr/bin/env python

"""
This is NOT a generalised version for the TDNA Pool calling mechanism.
It is assumed that the samples will be pulled down from a MySQL database.

This will NOT work for non Arabidopsis genomes(maybe).

The input to the pool calling mechanism is a Dictionary of Samples and the
corresponding MySQL connection info needed to pull down that sample.
"""

# ---- Standard Modules
import os
import sys
from collections import defaultdict

# ---- Non Standard
import MySQLdb as sql
import pandas as pd
from numpy import log

# ---- My Modules
import fetcher_html_parser


# ---- Chromosome Data Frame Creation and Filling
def create_chrom_frame(unique_indexes,unique_columns):
    """
    Returns a Data Frame that contains all the BasePair Positions that have any reads.
    Inputs:
    - unique_columns: A list of integers representing the basepair positions that contain Pileups
    - unique_indexes: A list of unique strings that represent the sample names
    """
    print("\tAllocating Memory for Data Frame")
    indexes = unique_indexes[:]
    columns = unique_columns[:]

    columns.sort(key = lambda x:int(x))
    indexes.sort(key = lambda x:(x[0],int(x[1:3])))

    return pd.DataFrame(index=indexes,columns=columns)


def fillChromosomeFromMySQL(chromosome,samples_with_sql_information,min_reads):
    """
    With a Dicitionary of Samples and chromosomes information create a DataFrame and
    fill it up with absolute values of the pileup counts
    Inputs:
    - chromosome: string representation of chromosome name
    - samples_with_sql_information: dictionary where Keys are sample names and
                                    Values are dictionaries that contain the
                                    MySQL information.
    - min_reads: integer representing the minimum amount of reads needed in a bin
    """
    # Create a Chrom Frame
    chrom_frame = create_chrom_frame_from_sql(chromosome,samples_with_sql_information)

    # Loop through Samples and pull out relavent assembly info
    # and add to the DataFrame
    print("\tFilling Data Frame")
    for sample in samples_with_sql_information.keys():

        # Pull out MySQL information
        host         = samples_with_sql_information[sample]["host"]
        user         = samples_with_sql_information[sample]["user"]
        password     = samples_with_sql_information[sample]["password"]
        db_dot_table = samples_with_sql_information[sample]["table"]
        database = db_dot_table.split(".")[0]
        table    = db_dot_table.split(".")[1]

        # Connect to Database and get Rows
        connection = sql.connect(host=host,user=user,passwd=password,db=database)
        cursor    = connection.cursor()
        cursor.execute("SELECT * FROM %s where assembly = %s" % (table,chromosome.replace("chr","")))

        # Move to Plus strand
        sql_data = list(cursor.fetchall())
        sql_data.sort(key = lambda x: x[4] if x[2] == "+" else x[3])

        # Calculate and add Pileups to Data Frame
        add_pileups_to_frame(chrom_frame=chrom_frame,index=sample,data_to_parse=sql_data,
                             direction_column=2,start_column=3,end_column=4,min_reads=min_reads)

    return chrom_frame


def create_chrom_frame_from_sql(chromosome,samples_with_sql_information):
    """
    Preallocate Data Frame with the right columns before filling it.
    """
    print("\tDetermining Amount of Basepairs Needed")
    columns = set()

    for sample in samples_with_sql_information.keys():

        # Pull out MySQL information
        host         = samples_with_sql_information[sample]["host"]
        user         = samples_with_sql_information[sample]["user"]
        password     = samples_with_sql_information[sample]["password"]
        db_dot_table = samples_with_sql_information[sample]["table"]
        database = db_dot_table.split(".")[0]
        table    = db_dot_table.split(".")[1]

        # Connect to Database and get Rows
        connection = sql.connect(host=host,user=user,passwd=password,db=database)
        cursor    = connection.cursor()
        cursor.execute("SELECT * FROM %s where assembly = %s" % (table,chromosome.replace("chr","")))

        # Get the Pileup Positions
        for row in cursor.fetchall():
            direction = row[2]
            start     = row[3]
            end       = row[4]

            if direction == "+":
                position = int(end)
            else:
                position = int(start)

            columns.add(position)
            
    columns = list(columns)
    columns.sort()

    return create_chrom_frame(samples_with_sql_information.keys(),columns)


def add_pileups_to_frame(chrom_frame,index,data_to_parse,direction_column,start_column,end_column,min_reads,pos="+",neg="-",split_string=" ",chromosome_column=None,chromosome_number=None):
    """
    Add Pileups to data frame is a pretty versitale function. Given a data frame new information
    is added to that frame by Pass by Reference ie nothing is returned. Any sort of delimited Object
    can be passed through as long as it has the correct information for direction, start and end.

    Index:             sample name to add information to in Data Frame. The sample name
                       must be from the same list that was used to create the INDEXES in
                       the Chrom Frame.
    Data_to_parse:     Can either be a list of lists or a File Object.
    chromosome_column: An integer representing the column that contains the chromosome information.
                       It is assumed that the chromsome is encoded in a simple 1,2,3,4,5 format.
    chromosome_number: A string representing the chromosome number (in case the data is in a lump file)
    """

    pileup_height = 0
    prev_position = 0

    for row in data_to_parse:

        # Sometimes shit is not a list, and it might be a File Object
        if not isinstance(row,list) and isinstance(row,str):
            row = row.split(split_string)

        direction = row[direction_column]
        start     = row[start_column]
        end       = row[end_column]

        # if we have this information check. If false continue
        if (chromosome_column and chromosome_number) and not (row[chromosome_column] == chromosome_number):
            continue

        if direction == pos:
            pileup_position = int(end)

        elif direction == neg:
            pileup_position = int(start)

        # Add to Frame
        if pileup_position == prev_position:
            pileup_height += 1
        else:
            if pileup_height >= min_reads:

                # Initialize the position
                if pd.isnull(chrom_frame.ix[index,prev_position]):
                    chrom_frame.ix[index,prev_position] = 0

                chrom_frame.ix[index,prev_position] += pileup_height

            pileup_height = 1

        prev_position = pileup_position


# ---- Calling Pools from Columns
def pool_caller(chrom_frame,output_file,chromosome):
    """
    Input: Pandas DataFrame for a chromosome. Assume that the frame is clean.
           Only should have columns that have at least 4 non null values

    - output_file: Python File Object
    - chromosome: string representation of chromosome
    """
    # Get Unique Pools
    # e.g. Group all the S's and A's and L's and K's
    unique_pools = defaultdict(list)
    prev_call = {}

    for index in chrom_frame.index:
        unique_pools[index[0]].append(index)

    # Loop through Columns. Calculate "Winners" for each unique pool
    for column in chrom_frame.columns:
        winners = []
        noise_coefficients = []

        for pool in unique_pools:
            winners += calculate_winners(chrom_frame.ix[unique_pools[pool],column])
            noise_coefficients += calculate_noise_coefficient(chrom_frame.ix[unique_pools[pool],column])

            noise_coefficient = sum(noise_coefficients)

        # If there are N samples in a pool and N winners in that column
        if len(winners) >= len(unique_pools.keys()) - 1:
            output_string = ",".join([chromosome]+[str(column)] + winners)
            output_file.write(output_string + "\n")


def calculate_winners(chrom_frame_subset,min_percentage = 0.8):
    """
    Takes a subset of a DF and calculate a winner from
    that specific subset of columns

    Returns a list with the winning arguement.

    NOTE: This is chosen to expoit the fact that adding a NULL list to a list object
          does not increase the size of the list.
    """
    frequency_totals = sum(chrom_frame_subset)

    # If the max value comprises 80% of the reads in the pool return
    # The name of the corresponding Index
    if frequency_totals == 0:
        return []

    if max(chrom_frame_subset) / frequency_totals >= min_percentage:
        return [chrom_frame_subset.index[chrom_frame_subset.argmax()][:3]]

    return []


def calculate_noise_coefficient(chrom_frame_subset):
    """
    """
    frequency_totals = sum(chrom_frame_subset)

    if frequency_totals == 0:
        return []

    highest_percentage = max(chrom_frame_subset)/frequency_totals

    return [-1*log(highest_percentage**2)]


# ---- Pipelines
def html_pipeline(abs_path_to_html_page,output_dir=os.getcwd()):
    """
    This is the canonical version for the HTML TDNA Seq pipeline.
    Inputs: An Annoj formatted HTML.
    Output: A file with the HTML page name and a dot(.)out extension.
            contains the approximate basepair position and line calls.
    """
    # Global Variables
    chromosomes = ["chr" + str(x) for x in range(1,6)]
    output_file_name = os.path.splitext(os.path.basename(abs_path_to_html_page))[0]

    # Fetcher... returns a dict that has all the sql information with the samples
    samples = fetcher_html_parser.html_parser(abs_path_to_html_page)

    with open(output_file_name + ".out","w") as output_file:
        for chromosome in chromosomes:
            print(chromosome)
            print("\tGenerating Chromsome Data Frame")

            chrom_frame = fillChromosomeFromMySQL(samples_with_sql_information=samples,
                                                  chromosome=chromosome,
                                                  min_reads=2)

            # There must be at least 4 non NA's in a column
            # Replace NA's in column with 0's
            print("\tCleaning")
            chrom_frame = chrom_frame.dropna(axis=1,thresh=3)
            chrom_frame = chrom_frame.fillna(0)

            # Start Calling Pools from Columns
            print("\tCalling Pools")
            pool_caller(chrom_frame,output_file,chromosome)


# ---- Misc
def tdna_filter(no_clones_file,window_size=200,min_reads=5):
    """
    Assumes files are Tab seperated:
    Col1: Unique ID (only needed for a mysql upload)
    Col2: Integer -> Chromosome number
    Col3: +/-     -> Direction
    Col4: Integer -> Start in reference to 5'
    Col5: Integer -> End in reference to 5'
    Col6: String  -> Sequence. Raw or aligned
    """


    # Use Window Technique to mow through clones.
    with open("tdna.filter.out","w") as output_file:
        with open(no_clones_file,"r") as input_file:
            print("\tFiltering %s" % no_clones_file)

            peaks = defaultdict(dict)
            temp = []
            prev_bin = None

            for line in input_file:
                row = line.split()
                chrom = row[1]

                # Get Position we care about
                if row[2] == "-":
                    num = int(row[3])
                else:
                    num = int(row[4])

                # What bin are we in?
                bin = num/window_size

                # Is Bin the Same as prev bin?
                if bin == prev_bin:
                    temp.append(line)

                else:
                    if len(temp) >= min_reads:
                        output_file.write("".join(temp))
                        peaks[chrom][bin] = len(temp)

                    temp = [line]

                prev_bin = bin


if __name__=="__main__":
    """
    """
    try:
        html_page = sys.argv[1]
    except IndexError:
        print("I'm out!")
        sys.exit(1)

    if not os.path.isfile(html_page):
        print("I'm Out!")
        sys.exit(1)

    html_pipeline(html_page)
