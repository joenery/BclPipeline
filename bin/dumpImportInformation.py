#!/usr/bin/env python
"""
Using a Sample Sheet this script will create a folder for each project, dump
all the fetchers and a file that includes the track defiinitions and active_track
track information in to one place.
"""
import argparse
import os
import sys
from guts import *

class MyParser(argparse.ArgumentParser):
    def error(self,message):
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)

class sampleSheetParser(project):
    
    def __init__(self,sample_sheet):
        if os.path.isfile(sample_sheet):
            self.sample_sheet = sample_sheet
        else:
            print("Couldn't Find File!")
            sys.exit(1)

    def getTrackDefintions(self,fetcher_dir):

        if fetcher_dir[-1] == "/":
            fetcher_dir = fetcher_dir[:-1]

        top_dir = os.getcwd()
        active_tracks = []

        for project in self.projects.keys():

            # Make dir
            make_dir = "mkdir %s" % (project)
            subprocess.call(make_dir,shell=True)
            os.chdir(project)


            with open(project + ".trackDefinitions","w") as track_def:
                track_def.write("tracks : [\n\n")

                for sample in self.projects[project]:
                    tablename = sample
                    active_tracks.append(tablename)

                    track_def.write("{\n")
                    track_def.write(" id: '%s',\n"   % tablename)
                    track_def.write(" name: '%s',\n" % tablename)
                    track_def.write(" type: 'ReadsTrack',\n")
                    track_def.write(" path: 'NA',\n")
                    track_def.write(" data: '%s/%s',\n" % (fetcher_dir,tablename + ".php"))
                    track_def.write(" height: '90', \n")
                    track_def.write(" scale: 0.03\n")
                    track_def.write("},\n")

                track_def.write("\n\n],\n\nactive : [\n")

                for sample in active_tracks:
                    track_def.write("'" + sample + "',")

                track_def.write("\n\n],\n")

        os.chdir(top_dir)

    def getFetchers(self,includes_dir):

        if includes_dir[-1] == "/":
            includes_dir = includes_dir[:-1]
        
        top_dir       = os.getcwd()

        for project in self.projects.keys():

            # Make dir
            make_dir = "mkdir %s" % (project)
            subprocess.call(make_dir,shell=True)
            os.chdir(project)

            for sample in self.projects[project]:
                tablename = sample
                database  = self.projects[project][sample]["database"]
                host      = self.projects[project][sample]["destination"]

                with open(tablename + ".php","w") as fetcher:
                    fetcher.write("<?php\n")
                    fetcher.write("$append_assembly = false;\n")
                    fetcher.write("$table = '%s.%s';\n" % (database,tablename) )
                    fetcher.write("$title = '%s';\n" % (tablename))
                    fetcher.write("$info = '%s';\n"  % (tablename.replace("_"," ")))
                    fetcher.write("""$link = mysql_connect("%s","mysql","rekce") or die("failed");\n""" % (host))
                    fetcher.write("require_once '%s/common_reads.php';\n" % (includes_dir))
                    fetcher.write("?>\n")

        os.chdir(top_dir)




if __name__=="__main__":

    parser = MyParser(description = "Takes in a SampleSheet with bowtie and \
                                     import2annoj information and creates a \
                                     folder full of the fetcher information \
                                     and an output of track definitions.")

    mandatory = parser.add_argument_group("MANDATORY")
    advanced  = parser.add_argument_group("ADVANCED")

    mandatory.add_argument("-ss","--sample-sheet", help="Sample Sheet you'd like to pull information out of.")

    advanced.add_argument("-p","--page-dir",help="Path to HTML page from dev/pages/ \
                                                     directory'DEFAULT= ..", default = "")
    advanced.add_argument("-f","--fetcher-dir", help="Path to Fetchers dir from /srv/www/htdocs/dev/fetchers/. DEFAULT = jfeeneysd/" \
                                              , default="jfeeneysd/")
    # Get Command Line Options
    command_line_options = vars(parser.parse_args())
    
    sample_sheet = command_line_options["sample_sheet"]
    page_dir     = command_line_options["page_dir"]
    fetcher_dir  = command_line_options["fetcher_dir"]

    # Check Options
    if not sample_sheet:
        parser.print_help()
        sys.exit(1)

    # Variables
    includes_abs_path = "/srv/www/htdocs/dev/fetchers/includes/"
    pages_abs_path    = "/srv/www/htdocs/dev/pages/"
    fetchers_abs      = "/srv/www/htdocs/dev/fetchers/"

    fetcher_sample_dir = os.path.join(fetchers_abs,fetcher_dir)
    page_sample_dir    = os.path.join(pages_abs_path,page_dir)

    fetcher_dir        = os.path.relpath(fetcher_sample_dir,page_sample_dir)
    includes_dir       = os.path.relpath(includes_abs_path,fetcher_sample_dir)

    # # Run the Script
    s = sampleSheetParser(sample_sheet)
    s.parseSampleSheet()
    s.getTrackDefintions(fetcher_dir)
    s.getFetchers(includes_dir)
