WHY: I've broken out the modules in to their own scripts. Modules that can stand on their own are made in to excutable files. For instance: bowtie.py and import2annojsimple.py

Since BclPipeline and SimpleBCL run on the same internals I'd like to have bug fixes in one propogate to the other. By breaking out the guts into its own stand alone script and having BCL and SimpleBCL import the functions it makes up keep between the two easier. Furthermore some less script such as project2bowtie2annoj also relies on the same internals.

guts.py -> provides the logic for parsing the sample sheets and the configureBclToFastq.py implementation. Provides surrounding framework for the call to the bowtie.py and import2annojsimple.py modules

bowtie.py -> Excecuable script and module. Given a folder the bowtie-folder function will check to make sure all the info you passed it checks out and moves in to the folder given. If there are any .gz's present the y will be uncompressed. Afterwards bowtie is run.

import2annojsimple.py -> reference implementaion for import2annoj. Uses headers to calculate number of chromosomes and then parses the SAM file and uploads chromosomes to a MySQL Database.

## Module References
### Bowtie.py
	def system_call(command,err_message):
		"""
		Just a simple wrapper for subprocess.call().
		Allows for the checking of the exit number.
		If exit number does not equal 1 script is halted.
		"""

	def bowtie_folder(folder,options="--local -p 4",bowtie_shell_call="bowtie2",\
			  indexes_folder="/home/seq/bin/bowtie2/INDEXES/",indexes_genome="tair10"):
		"""
		Script checks for the existence of indexes folder and that genome is in folder
		If there are .gz files in directory they are uncompressed. 

		NOTE: As of right now only the R1 reads in a directory are bowtied
		      If the FASTQ's do not contain "R1" or "R2" then these files are bowtied
		
		Stats and SAM file are written to current working directory. 
		"""

### emailnotifications.py

	def __init__(self):
		"""
		Username, password, and admins are hardcoded. They can be changed here.
		"""

	def send_message(self,TO,SUBJECT,TEXT):
		"""
		TO      -> List of strings that contain one email per index.
		SUBJECT -> String
		TEXT    -> String
		"""
	def

### project2bowtie2annoj.py

### getAlignedSequence.py 

### guts.py

### import2annojsimple.py
