WHY: I've broken out the modules in to their own scripts. Modules that stand alone such as getAlignments and import2annoj exist on their own. The guts.py has all the internals that parse a sample sheet and runBCL as well as Bowtie.

Since BclPipeline and SimpleBCL run on the same internals I'd like bug fixes in one propogate to the other. By breaking out the guts into it's own stand alone script and having BCL and SimpleBCL import the functions it makes maitenance between the two easier.

Also: Since the modules are broken out it makes testing them easier! Every module has a "if __name__..." statement after which you can write code to test the functions defined. Run the test by

$ python <module to test>.py

HOW: Basically a list of every function in each of the modules and what they do and the variables they take and spit out
