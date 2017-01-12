#!/usr/bin/python
import re
import os
import json


maxBucketNum=8500 # 100 times of reducer number is enought to control skew cell to 1 percent of average load per node, if not extreamly skew. To avoid huge bucketing number, and improve shuffle accurate.
rowindexFolder="stats/rowindex/"
bucketFolder="stats/buckets/"

for dirName, subdirList, fileList in os.walk(rowindexFolder):
	for fname in fileList:
		with open(rowindexFolder+fname) as data_file:
	    		stats=json.load(data_file)
			if not stats['interleave']:
				stats["buckets-schema"]=("min,max,cnt,sum")
				stats["buckets"]=stats.pop("entries")
				continue
	    		if stats['type'] in ('int','tinyint','smallint','bigint'):
	    			stats['step']=(stats['max']-stats['min']+1)/float(stats['cnt'])*stats['entrySize']
				stats['step']=(1 if stats['step']<1 else long(stats['step']))
			elif stats['type'] in ('float','double','decimal'):
				#todo:Decimal()
				stats['step']=(stats['max']-stats['min'])/float(stats['cnt'])*stats['entrySize']
			else:
				#todo Date/Time Types
				print "Skip "+fname+": "+stats['type']+" is unsupported by far!"
				continue
			print fname
			print 'step is '+str(stats['step'])
			stepMin=(stats['max']-stats['min'])/maxBucketNum
			if stepMin>stats['step']:
				stats['step']=stepMin
				print 'step reset to '+str(stepMin)
			bucket0=int(stats['min']/stats['step'])
			bucketCnt=int(stats['max']/stats['step'])-bucket0+1
			#todo: fix [min,max), avoid using max
			stats["buckets-schema"]=("min,max,cnt")
			stats["buckets"]=[]
			for bi in range(bucketCnt):
				min=stats['min']+bi*stats['step']
				stats["buckets"].append([min,min+stats['step'],0])
			i=0
			lenpar=len(stats["entries"])/100+1
			for e in stats["entries"]:
				lb=int(e[0]/stats['step'])
				hb=int(e[1]/stats['step'])
				if lb==hb:
			        	stats["buckets"][lb-bucket0][2]+=e[2]
					continue
	    			#todo:fix e[1]-e[0]+1
			        eweight=float(e[2])/(e[1]-e[0])
			        lbslice=eweight*((lb+1)*stats['step']-e[0])
	    			#todo:fix e[1]-hb*stats['step']+1
			        hbslice=eweight*(e[1]-hb*stats['step'])
			        stats["buckets"][lb-bucket0][2]+=lbslice
			        stats["buckets"][hb-bucket0][2]+=hbslice
			        rbCnt=hb-lb-1
				if rbCnt>0:
					bslice=(e[2]-hbslice-lbslice)/rbCnt
					for bi in range(lb+1,hb):
						stats["buckets"][bi-bucket0][2]+=bslice
				i+=1
                                if i%lenpar==1:
                                        print str(i/lenpar)+"%"
			del stats["entries"]
		with open(bucketFolder+fname,'w') as bucketFile:
	        	json.dump(stats,bucketFile)
