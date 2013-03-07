WHY: I've broken out the modules in to their own scripts. Modules that stand alone such as getAlignments and import2annoj exist on their own. The guts.py has all the internals that parse a sample sheet and run BCL.

Since BclPipeline and SimpleBCL run on the same internals I'd like bug fixes in one propogate to the other. By breaking out the guts into it's own stand alone script and having BCL and SimpleBCL import the functions it makes maitenance between the two easier.

Also: Since the modules are broken out it makes testing them easier! Every module has a "if __name__..." statement after which you can write code to test the functions defined.

$ python <module to test>.py

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
