#!/usr/bin/python

import subprocess
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-r", action="store_true", dest="recursive", default=False)
options, args = parser.parse_args()

cmd = "$HADOOP_PREFIX/bin/hadoop dfs -ls"
if options.recursive == True:
	cmd += "r"

if len(args) == 0:
	cmd += " /hduser"
else:
	cmd += " " + args[0]

subprocess.call(cmd, shell=True)
