#!/usr/bin/python
import sys
import json
print sys.argv[1]
keys="web_sales.ws_item_sk,item.i_item_sk,store_sales.ss_item_sk,web_sales.ws_warehouse_sk,,warehouse.w_warehouse_sk,web_clickstreams.wcs_item_sk,web_clickstreams.wcs_user_sk,store_sales.ss_customer_sk,product_reviews.pr_item_sk,web_sales.ws_item_sk"
keys=keys.split(',')
with open(sys.argv[1]) as jf:
	i=0
	for je in json.load(jf):
		for e in je:
			print keys[i],'\t',e['x'],'\t',e['y']
		i+=1
