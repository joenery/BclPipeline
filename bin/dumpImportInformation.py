#!/usr/bin/env python
"""
Using a Sample Sheet this script will dump all the Track Definitions out to the screen
"""
import os
import sys
from guts import *

class sampleSheetParser(project):
    
    def __init__(self,sample_sheet):
        if os.path.isfile(sample_sheet):
            self.sample_sheet = sample_sheet
        else:
            print("Couldn't Find File!")
            sys.exit(1)

if __name__=="__main__":
    s = sampleSheetParser("/home/seq/SampleSheets/SampleSheet_Jonas_2152_TDAN.csv")
    
    s.parseSampleSheet()
