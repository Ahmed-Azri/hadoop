#!/usr/bin/python

import subprocess
from optparse import OptionParser

cmd = "$HADOOP_PREFIX/bin/hadoop dfs -copyFromLocal"
if len(args) <2 :
	print("Usage: add_data_to_dfs.py <local dir> <dfs dir>")
	sys.exit(0)

cmd += " " + args[0] + " " + args[1]

subprocess.call(cmd, shell=True)
