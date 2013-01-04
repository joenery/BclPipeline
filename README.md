# BCL Gal-E Salk Pipeline
Joe Feeney: jfeeney at salk edu
License: Buy a Joe Feeney a beer if this script doesn't suck.

# Overview
Bcl Pipeline autmates several steps in the bcl process by making a few key assumptions.

1. There is a sample sheet that contains all the information needed top run Bcl in <run folder>/Data/Intensities/BaseCalls/
2. Bcl writes its output to <run folder>/Unaligned/ in which the Projects and their samples are stored.

# What It Doesn't Do and Known Issues
- Currently, BCLPipeline does not handle paired-end reads for Bowtie2 and MySQL uploading. As such paired-end reads should only have BCL analysis run on them
- MySQL upload from Bowtie2 sam files include the entire read sequence and not what actually aligned. However, the actual length of the read is pulled out with regular expressions and is accurate.
- Script will not make you "successful with the ladies". Only working on your dance moves can do that.

## BCL Mandatory "option"
Bclpipeline.py -r <ABSOLUTE PATH TO RUN>

The only option that needs to be suppled is the absolute path to the run folder you'd like to run Bcl on. By default it is assumed that the run has not completed. Bcl pipeline will watch the RTAComplete.txt file in the specified run folder for changes. When the finish time is appended BCL will run.

## Other Options
-nw / --no-watch -> If the run has already finished and you'd like to run the script this flag will cause Bcl to run right away. DEFAULT: Off

-p / --processor -> If there are runs that will be Bowtie-ed (See Bowtie and Annoj Section below) this will specify how many processors to run on each pass of Bowtie. DEFAULT: 12

-n / --notifications -> By turning this feature on notifications will be set out to an ADMIN and the owners of Samples when the Bcl process has started and when it has finished (For more info on implementation see Notifications below). DEFAULT: Off

### Bowtie and Annoj/Mysql Upload
#### Bowtie
Bowtie and Annoj/MySQL upload can be run on a per sample basis. The configuration for this is contained within one of the unused columns in the SampleSheet.csv file.

Gal-E Sample Sheets are structured like so:
D/Lane/Sample/Sample_Ref/Index/Descriptor/Control_Lane/Recipe/Operator/Project

The sixth column, "Descriptor", is used as the column where this configuration sits. There are three semi-colon seperated sub columns in "Descriptor" that are used to specify whether or not Bowtie2 is performed and optionally, if Bowtie2 has been run, upload the sam file to a MySQL database in the annoj format.

Example Sample Sheet:

D,Lane,Sample,Sample_Ref,Index,Descriptor,Control_lane,Recipe,Operator,Project
,8,X,10,TAGCTT,bt;thumper-e2;bcl_test,,,bouchi@gmail.com,Bennett
,8,Z,12,CTTGTA,,,,bouchi@gmail.com,Bennett

For the first row of samples you can see column "Descriptor" has the following information:

	bt;thumper-e2;bcl_test

The first column signifies that bt should be run. The second indicates that MySQL upload should be performed and that the MySQL Host is thumper-e2. The last column is optional if the MySQL Host is present. It signifies which Database the data should be uploaded to. If it is not present it is assumed that the Project Name is the correct database. Additionally, if a Database is not present on a host it will be created.

More Examples:

bt;;             -> Run bowtie but don't upload to a MySQL database
bt;thumper-e3;   -> Run bowtie and Upload to MySQL use Project as database name and sample name + Illumina Run ID as tablename
;;               -> Only perform BCL analysis on sample.
                 -> :-} You can also leave column Descriptor blank and the script will just assume you only want Bcl.

Here's some Bad examples:

;thumper-e3;     -> Since you didn't ask to bowtie the sample the script will complain. You can run BCL anyway but bowtie nor MySQL upload will be performed
bt;;bcl_test     -> Now you're being just down right negligent! 

### Notifications
Notifications are sent out by email to an ADMIN whose email is hardcoded into the script and to email address that are associated with samples. These associations are handled in the Sample Sheet column "Operator". The contact email for the sample should be placed here. 

All parties are notified when BCL analysis has begun and when it has finished. At the end an email is sent out to each Operator address with the path to their specific runs. If an operator's email shows up more than once all their runs are sent in one email.


