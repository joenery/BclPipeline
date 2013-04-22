"""
This module is user editable and stores each genome
in a dictionary. This module is called when parsing the
SampleSheet.

keys: genome names
values: a list of integers representing the number of chromosomes 
"""

class genomes(object):

    def __init__(self):

        self.genomes = {

            "tair10":[1,2,3,4,5]
        }


if __name__=="__main__":
    print("Testing")
    g = genomes()

    print g.genomes["tair10"]