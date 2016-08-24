#!/usr/bin/python
import csv, sys, MySQLdb, time, os, fnmatch, string, re


badStates = ['FL','IN','PA','PR','AA','AB','AE','AP','AS','ON','XX','NO','PW','FM','SK','MB','KA']

inputfile = sys.argv[1]
outputfile = sys.argv[2]

reader = csv.DictReader(open(inputfile, 'rb'), delimiter=',')	#input reader instance
writer = csv.writer(open(outputfile, 'w'), delimiter=',') 

count = 0 #counter

db = MySQLdb.connect(user="cron", passwd="1234", host="127.0.0.1", db="asterisk")
conn = db.cursor()   


"""Iterate over CSV and """
for row in reader:
	phoneDigits = re.sub("\D","",row['PHONE'])
	areaCode = phoneDigits[0:3]
	conn.execute("SELECT state FROM vicidial_phone_codes WHERE areacode = %s", (areaCode))
	result = conn.fetchone()
	if result:
		print count
		writer.writerow([row['PHONE'],result[0]])	
		count += 1
	else:
		print areaCode
os.remove(inputfile)


