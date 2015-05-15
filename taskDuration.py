import sys
import re
import operator
import time

class task:
	def __init__(self):
		self.name = ""
		self.start= ""
		self.end= ""
		self.type= ""
	def __str__(self):
		return "start: {} \nend: {}\nType:{}\n".format(self.start, self.end, self.type )

timepattern = '%Y-%m-%d %H:%M:%S'
def GetTime(line):
	timeraw = re.split("INFO",line)[0].split(",")[0]
	return time.mktime(time.strptime(timeraw, timepattern))	

def GetTaskName(line):
	return GetTaskType(line)+re.split(": | ",line)[7].split("_")[4]
def GetTaskType(name):
	return re.split("_", name)[3]
lines = []
starttime =0
with open(sys.argv[1], 'r') as myFile:
	lines= myFile.readlines()
with open(sys.argv[2], 'r') as myFile:
	starttime= int(myFile.readlines()[0])

tasks = {}

for line in lines:
	if "SCHEDULED to RUNNING" in line:
		tasks[GetTaskName(line)] = task()
		tasks[GetTaskName(line)].name = GetTaskName(line)
		tasks[GetTaskName(line)].start = GetTime(line)
		tasks[GetTaskName(line)].end = GetTime(line)
		tasks[GetTaskName(line)].type = GetTaskType(line)

	if "RUNNING to SUCCEEDED" in line:
		tasks[GetTaskName(line)].end = GetTime(line)

taskslist = sorted(tasks.values(), key=operator.attrgetter('start'))
print("name,start,end,type")
for t  in taskslist:
	print("{},{},{},{}".format(t.name, t.start - starttime ,t.end - starttime,t.type))	
