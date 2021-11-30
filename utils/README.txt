How to use these tools.

Before using:
	Use Globus (globus.org) to download data from NCSA public datsets, Blue Waters Darshan Logs. 

	Build darshan-parser (https://www.mcs.anl.gov/research/projects/darshan/docs/darshan-util.html) and add to path. 


To build dataset:
	From child directory of this folder, run unzipper.sh to read in features from darshan logs for that month.
	e.g. '../unzipper.sh'

	splitter.sh is a utility to subdivide  a directory into dirs of equal size, and can be used to split up data for processing. 

	worker.py scans the darshan text log into a csv of aggregated raw features for the machine learning model. 

	compile_results.sh loads all csv files after running splitter.

	clean.sh will remove all results and intermediate files if an instance of unzipper.sh fails, so that it can be restarted from archives.

