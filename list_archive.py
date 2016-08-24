#!/usr/bin/python
import csv, sys, MySQLdb, time, os, fnmatch, string, re

list_id = sys.argv[1]
list_name = list_id

db = MySQLdb.connect(user="cron", passwd="1234", host="127.0.0.1", db="asterisk")
conn = db.cursor()   

print "Creating list in lists_archive table"
conn.execute("INSERT IGNORE INTO vicidial_lists_archive SELECT * FROM vicidial_lists WHERE list_id = %s", (list_id))
result = conn.fetchone()
print "Inserting leads in list_archive table"
conn.execute("INSERT IGNORE INTO vicidial_list_archive SELECT * FROM vicidial_list WHERE list_id = %s", (list_id))
print "Deleting list from lists table"
conn.execute("DELETE from vicidial_lists WHERE list_id = %s", (list_id))
print "Deleting records from list table"
conn.execute("DELETE from vicidial_list WHERE list_id = %s", (list_id))
					
					
	



