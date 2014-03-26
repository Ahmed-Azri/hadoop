#!/usr/bin/python

import subprocess
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-r", action="store_true", dest="recursive", default=False)
options, args = parser.parse_args()

if len(args) == 0:
	print("No file to be remove!")
	sys.exit(0)
cmd = "$HADOOP_PREFIX/bin/hadoop dfs -rm"
if options.recursive == True:
	cmd += "r"

cmd += " " + args[0]

subprocess.call(cmd, shell=True)
