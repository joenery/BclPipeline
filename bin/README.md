WHY: I've broken out the modules in to their own scripts. Modules that can stand on their own are made into excutable files. For instance: bowtieSimple.py and import2annojsimple.py

guts.py -> provides the logic for parsing the sample sheets and the configureBclToFastq.py implementation. Provides surrounding framework for the call to the bowtie.py and import2annojsimple.py modules

bowtiesimple.py -> Excecuable script and module. Given a folder the bowtie-folder function will check to make sure all the info you passed it checks out and moves in to the folder given. If there are any .gz's present the y will be uncompressed. Afterwards bowtie is run. Can also take

import2annojsimple.py -> reference implementaion for import2annoj. Uses headers to calculate number of chromosomes and then parses the SAM file and uploads chromosomes to a MySQL Database.

## Module References
### guts.py
### bowtiesimple.py

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


### import2annojsimple.py
