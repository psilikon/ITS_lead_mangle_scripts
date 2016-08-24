#!/usr/bin/python
import csv, sys, MySQLdb, time, os, fnmatch, string, re


badStates = ['FL','IN','PA','PR','AA','AB','AE','AP','AS','ON','XX','NO','PW','FM','SK','MB','KA']

fname = sys.argv[1]
list_id = sys.argv[2]
campaign_id = sys.argv[3]
list_name = list_id

reader = csv.DictReader(open(fname, 'rb'), delimiter=',')	#input reader instance

phoneList = [] #de-dupe list
count = 0 #counter

db = MySQLdb.connect(user="cron", passwd="1234", host="127.0.0.1", db="asterisk")
conn = db.cursor()   


"""Iterate over CSV and """
for row in reader:
	phoneDigits = re.sub("\D","",row['PHONE'])
	state = row['STATE'].upper()
	if state not in badStates:
		if len(phoneDigits) == 10:
			if phoneDigits not in phoneList:
				phoneList.append(phoneDigits)
				conn.execute("SELECT COUNT(*) FROM nanpa_codes WHERE npanxx = %s AND ocn !=''", (phoneDigits[0:6]))
				result = conn.fetchone()
				print "		Checking NPANXX"
				if result[0] > 0:
					conn.execute("SELECT COUNT(*) FROM vicidial_list_archive WHERE phone_Number = %s", (phoneDigits))
					result = conn.fetchone()
					print "		Checking vicidial_list_archive"
					if result[0] < 1:
						conn.execute("SELECT COUNT(*) FROM vicidial_list WHERE phone_Number = %s", (phoneDigits))
						result = conn.fetchone()
						print "		Checking vicidial_list"
						if result[0] < 1:
							conn.execute("SELECT GMT_Offset FROM vicidial_phone_codes WHERE areacode = %s AND country='USA'", (phoneDigits[0:3]))
							result = conn.fetchone()
							if result:
								gmt_offset = result[0]+'.00' #GMT Offset
							else:
								gmt_offset = '0.00' #GMT Offset

							if campaign_id == '9000':
								conn.execute("SELECT did FROM DID_CID_9000 WHERE state = %s", (state))
								result = conn.fetchone()
							if campaign_id == '9001':
								conn.execute("SELECT did FROM DID_CID_9001 WHERE state = %s", (state))
								result = conn.fetchone()
							if result:
								cid_did = result[0]
							else:
								cid_did = ''
							
							sql = """INSERT INTO vicidial_list
								(lead_id,entry_date,modify_date,status,user,vendor_lead_code,source_id,list_id,gmt_offset_now,called_since_last_reset,phone_code,phone_number,title,
								first_name,middle_initial,last_name,address1,address2,address3,city,state,province,postal_code,country_code,gender,date_of_birth,alt_phone,email,
								security_phrase,comments,called_count,last_local_call_time,rank,owner) VALUES 
								('',now(),'','NEW','','','',%s,%s,'N','1',%s,'',%s,'','','','','',%s,%s,'',%s,'','','',%s,'',%s,'','','','','')"""

							conn.execute(sql, (list_id,gmt_offset,phoneDigits,'','',state,'','',cid_did))
							print fname, count
							count += 1
os.rename(fname,'used/'+fname)

#							print ('NEW','','','',list_id,gmt_offset,'','1',phoneDigits,'',row['FNAME'],'','','','','',row['CITY'],state,'',row['ZIP'],'','','',altPhoneDigits',''.cid_did,'','','2008-01-01 00:00:00','','')
#							print ('',insert_date,modify_date,status,user,vendor_lead_code,source_id,list_id,gmt_offset,'','1',phoneDigits,'',row['FNAME'],'','','','','',row['CITY'],state,'',row['ZIP'],'','','',altPhoneDigits',''.cid_did,'','','2008-01-01 00:00:00','','')
#							print ('',now(),modify_date,status,user,vendor_lead_code,source_id,list_id,gmt_offset,'','1',phoneDigits,'',row['FNAME'],'','','','','',row['CITY'],state,'',row['ZIP'],'','','',altPhoneDigits',''.cid_did,'','','2008-01-01 00:00:00','','')
			
					
			
					
					
	



