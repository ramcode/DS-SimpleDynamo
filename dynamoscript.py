#! /bin/python
import os
from environment import *

print "starting script...."

for i in range(0,10):

	cmd = './simpledynamo-grading.linux app-debug.apk >> dynamolog.txt'
	os.system(cmd)

	
