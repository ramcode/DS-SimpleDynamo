#! /bin/python
import os
from environment import *

print "uninstalling app from avds...."

for i in 5554, 5556, 5558, 5560, 5562:

	cmd = 'adb -s emulator-' + str(i) + ' shell pm uninstall edu.buffalo.cse.cse486586.simpledht '
	os.system(cmd)

print "installing app to avds...."

for i in 5554, 5556, 5558, 5560, 5562:

	cmd = 'adb -s emulator-' + str(i) + ' install app-debug.apk '
	os.system(cmd)

	
