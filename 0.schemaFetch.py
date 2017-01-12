#!/usr/bin/python
import commands
import re
import json

db='bigbench'
dbDir="/user/hive/warehouse/%s.db/" %db
outPutFile='stats/schema.json'
schema={}

(status, output)=commands.getstatusoutput("hive -e 'use %s;show tables;'" %db)
output=re.compile(r'^.*?Time taken.*?OK\n(.*?)\nTime taken', re.DOTALL).match(output).group(1)
for line in output.split("\n"):
	if line.startswith('q') and line.endswith('_result'):
		continue
	if line in schema:
		print "Error: duplicated table: "+line
        	exit(1)
	schema[line]={'fields':[],'files':[]}

(status, output)=commands.getstatusoutput("hadoop fs -ls %s*" %dbDir)
fileList=re.compile(re.escape(dbDir)+r"\S*?\/\S*").findall(output)
for f in fileList:
	t=f.split(dbDir)[1].split("/")[0]
	if t not in schema:
		print "Error: extra file: "+f
        	exit(1)
	schema[t]['files'].append(f)

q_desc="use %s;" %db
tables=schema.keys()
for t in tables:
	q_desc+="desc %s;" %t
(status, output)=commands.getstatusoutput("hive -e '%s'" %q_desc)
output=output.split("OK\n")[2:]
for ti in range(len(output)):
	t=tables[ti]
	for f in output[ti].split("\n"):
		f=f.split()
		if len(f)>1 and f[0]!='Time':
			schema[t]['fields'].append(f)

with open(outPutFile,'w') as data_file:
	json.dump(schema,data_file)
