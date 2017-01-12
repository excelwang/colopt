#!/usr/bin/python
import re
import os
import json

orcFolder="stats/orcfiledump/"
schemaFile="stats/schema.json"
rowindexFolder="stats/rowindex/"

#Entry 0:count: 10000 hasNull: false min: 65946577 max: 65956576 sum: 659515765000 positions: 0,0,0
enpattern=re.compile("^\s+Entry\s+\d+:count:\s+(\d+).*min:\s+(\S+)\s+max:\s+(\S+)\s+sum:\s(\S+)")

def fetchEntries(orcFile,typeName):
	entries=[]
	for line in orcFile:
		enmatch=enpattern.match(line)
		if not enmatch:
			continue
		min=enmatch.group(2)
		max=enmatch.group(3)
		cnt=long(enmatch.group(1))
		sum=enmatch.group(4)
		if typeName in ('int','tinyint','smallint'):
                	min=int(min)
			max=int(max)
			sum=long(sum)
            	elif typeName=='bigint':
                        min=long(min)
                        max=long(max)
			sum=long(sum)
		elif typeName in ('float','double','decimal'):
			min=double(min) #todo:Decimal()
                        max=double(max)
			sum=double(sum)
		#todo Date/Time Types
		entries.append([min,max,cnt,sum])
	return sorted(entries,key=lambda p:p[0])

with open(schemaFile) as data_file:
        schema=json.load(data_file)
        for t in schema:
            for f in schema[t]["fields"]:
                path=orcFolder+t+'.'+f[0]
                try:
			with open(path,'r') as orcFile:
                    		stats={}
				stats['type']=f[1]
                    		stats['entries']=fetchEntries(orcFile,f[1])
                    		if len(stats['entries'])==0:
					continue
				stats['entries-schema']='min,max,cnt,sum'
				stats['min']=stats['entries'][0][0]
				stats['max']=stats['entries'][-1][1]
				stats['cnt']=long(0)
				stats['interleave']=False
				stats['entrySize']={}
				for ei in range(len(stats['entries'])):
					if not stats['interleave'] and ei!=len(stats['entries'])-1 and stats['entries'][ei][1]>=stats['entries'][ei+1][0]:
						stats['interleave']=True
                			if stats['max']<stats['entries'][ei][1]:
						stats['max']=stats['entries'][ei][1]
					cnt=stats['entries'][ei][2]
					stats['cnt']+=cnt
					if cnt not in stats['entrySize']:
						stats['entrySize'][cnt]=1
					else:
						stats['entrySize'][cnt]+=1
				stats['entrySize']=max(stats['entrySize'].iteritems(),key=lambda x:x[1])[0]
				with open(rowindexFolder+t+'.'+f[0],'w') as indexFile:
                    			json.dump(stats,indexFile)
		except IOError:
			continue 
