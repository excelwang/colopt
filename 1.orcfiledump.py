#!/usr/bin/python
import os
import json
import threading

orcFolder="stats/orcfiledump/"
schemaFile="stats/schema.json"

def tableFetch(table,tname):
	for fi in range(len(table['fields'])):
		path=orcFolder+tname+"."+table['fields'][fi][0]
		os.system("echo ''>"+path)
		for f in table['files']:
			os.system("hive --orcfiledump --rowindex %s %s>>%s" %(fi+1,f,path))

with open(schemaFile) as data_file:
        schema=json.load(data_file)
	for t in schema:
		tr=threading.Thread(target=tableFetch,args=(schema[t],t))
		tr.start()
