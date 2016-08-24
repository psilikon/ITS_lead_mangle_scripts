#!/usr/bin/python
import csv, sys, MySQLdb, time, os, fnmatch, paramiko, smtplib, email, email.encoders, email.mime.text, email.mime.base, string, re
from colorama import Fore, Back, Style, init


OUTPUTFILE = sys.argv[1]
OUTIP = sys.argv[2]
LISTID = sys.argv[3]	#list_id to use
CAMPID = sys.argv[4]	#Campaign ID to add leads to
csvInFolder = ('/root/leads/CSV_IN')	#Where to look for INPUT files
forbiddenStates = ['FL','IN','PA','PR','AA','AB','AE','AP','AS','ON','XX','NO','PW','FM','SK','MB','KA']
error = ''
debugSleepTime = 0.0
init(autoreset=True)

def deleteFiles():
	os.remove(OUTPUTFILE)
	os.remove('FINAL'+OUTPUTFILE)

""" CREATE LIST ON DESTINATION (OUTIP) SYSTEM AND MAKE IT ACTIVE"""
def createActiveList():
	#Connect to db on either local or remote server to insert new list and activate new list
	db=MySQLdb.connect(OUTIP,'cron','1234','asterisk')
	cl_conn = db.cursor()	#createlist (cl) database connection
	try:
		cl_conn.execute("INSERT IGNORE INTO vicidial_lists (list_id, list_name, campaign_id, active) VALUES (%s,%s,%s,%s)", (LISTID, LISTID, CAMPID, 'Y'))
		result = cl_conn.fetchone()
	except MySQLdb.Error, e:
		print e

""" PASS THE OUTPUTFILE TO VICIDIAL'S VICIDIAL_IN_new_leads_file.pl"""
def passItToVicidial():
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	rsa_key = paramiko.RSAKey.from_private_key_file('/root/.ssh/id_rsa')
	ssh.connect(OUTIP,username='root',pkey=rsa_key)
	stdin,stdout,stderr = ssh.exec_command("/usr/share/astguiclient/VICIDIAL_IN_new_leads_file.pl --quiet --duplicate-system-check --email-list=joelshas@yahoo.com:j@itsinc.co --email-sender=vicidialLeads@time.itsinc.co")
	output = ''
	while not stdout.channel.exit_status_ready():
		output += stdout.channel.recv(1024)
	x = output.splitlines()
	print output
	for l in x:
		print l,

""" MOVE OUTPUT FILE TO OUTPUT IP ADDRESS SO VICIDIAL CAN DO IT'S THING"""
def transferToDestination():
	if OUTIP != '127.0.0.1':
		t = paramiko.Transport((OUTIP, 22))
		rsa_key = paramiko.RSAKey.from_private_key_file('/root/.ssh/id_rsa')
		t.connect(username='root', pkey=rsa_key)
		sftp = paramiko.SFTPClient.from_transport(t)
		sftp.put('FINAL'+OUTPUTFILE, '/usr/share/astguiclient/LEADS_IN/'+'FINAL'+OUTPUTFILE)
	else:
		#Simply move OUTPUTFILE to the astguiclient/LEADS_IN location
		os.rename('FINAL'+OUTPUTFILE, '/usr/share/astguiclient/LEADS_IN/'+'FINAL'+OUTPUTFILE)

""" PERFORM A DE-DUPE BY CHECKING BOTH vicidial_list AND vicidial_list_archive TABLES"""
def systemArchiveDedupe(PHONENUMBER):
	duplicate = False
	db = MySQLdb.connect(user="cron", passwd="1234", host=OUTIP, db="asterisk")
	dc_conn = db.cursor()	#dupecheck (dc) database connection
	dc_conn.execute("SELECT COUNT(phone_number) FROM vicidial_list WHERE phone_number= %s", (PHONENUMBER))
	result = dc_conn.fetchone()
	systemDupe = result[0]
	dc_conn.execute("SELECT COUNT(phone_number) FROM vicidial_list_archive WHERE phone_number= %s", (PHONENUMBER))
	result = dc_conn.fetchone()
	archiveDupe = result[0]
	dc_conn.close()
	return (systemDupe, archiveDupe)

""" FINAL CHECK WHERE SYSTEM AND ARCHIVE DUPLICATE CHECKS ARE STARTED AND THE FINAL OUTPUT FILE IS CREATED"""
def finalCheck():
	reader = csv.DictReader(open(OUTPUTFILE, 'rb'), delimiter='|')	#input reader instance
	finalwriter = csv.writer(open('FINAL'+OUTPUTFILE, 'a'), delimiter='|')	#VICIDIAL standard format output
	rowWrites = 0         #output row counter
	systemDuplicate = 0	  #system vicidial_list duplicate counter	
	archiveDuplicate = 0  #vicidial_list_archive duplicate counter

	for row in reader:
		clean = True
	
		"""RUN SYSTEM/ARCHIVE DUPLICATE FUNCTION """
		systemDupe, archiveDupe = systemArchiveDedupe(row['PHONE'])
		if systemDupe > 0:		#If a result it returned from function increment the dup counter
			print(Fore.RED+" ---------> "+row['PHONE']+" IS ALREADY IN THE SYSTEM [vicidial_list] TABLE!")
			clean = False
			systemDuplicate += 1
		if archiveDupe > 0:
			print(Fore.RED+" ---------> "+row['PHONE']+" IS ALREADY IN THE ARCHIVE [vicidial_list_archive] TABLE!")
			clean = False
			archiveDuplicate += 1
		if clean == True:
			finalwriter.writerow(['','',str(LISTID),'1',row['PHONE'],'',row['FNAME'],'','','','','',row['CITY'].strip(),row['STATE'].strip(),'',row['ZIP'].strip(),'','','',row['ALT_PHONE'],'',row['CID_DID'],'','','','',''])		
			rowWrites += 1	#Increment rowWrites by 1

	print "RECORDS WITH DUPLICATE PHONE NUMBERS IN VICIDIAL SYSTEM : ",str(systemDuplicate)
	print "RECORDS WITH DUPLICATE PHONE NUMBERS IN ARCHIVE SYSTEM : ",str(archiveDuplicate)
	print "ROWS WRITTEN : ",str(rowWrites)
	print "\n"

""" MAIN FUNCTION TO START PROCESSING THE FILES"""
def cleanIt():
	db=MySQLdb.connect('127.0.0.1','cron','1234','asterisk')	#Use localhost for NPANXX related things
	conn = db.cursor()

	""" CREATE CID DID DICTS"""
	conn.execute("SELECT state, did FROM DID_CID_9000")
	result = conn.fetchall()
	camp9000CIDdict = {}	
	for row in result:
		camp9000CIDdict.update({row[0]:row[1]})
	conn.execute("SELECT state, did FROM DID_CID_9001")
	result = conn.fetchall()
	camp9001CIDdict = {}	
	for row in result:
		camp9001CIDdict.update({row[0]:row[1]})
		
	writer = csv.writer(open(OUTPUTFILE, 'a'), delimiter='|')	#VICIDIAL standard format output
	""" WRITE HEADER INTO OUTPUTFILE"""
	writer.writerow(['1','2','LIST_ID','COUNTRYCODE','PHONE','6','FNAME','8','9','10','11','12','CITY','STATE','15','ZIP','17','18','19','ALT_PHONE','21','CID_DID','22','23','24','25','26'])		

	badstateCount = 0     #non-callable state counter
	nophoneCount = 0      #missing phone counter
	invalNPANXXCount = 0  #failed NPANXX counter
	rowCount = 0          #read rows counter
	rowWrites = 0         #output row counter
	failedRow = 0         #failed row counter
	dupeCount = 0         #current list duplicate counter
	phoneList = []	#Phone number List to keep track of duplicates

	for filename in os.listdir(csvInFolder):
		if fnmatch.fnmatch(filename, '*csv'):	#if file is csv then process it
			fullFilename = os.path.join(csvInFolder,filename)	#join path to filename

			reader = csv.DictReader(open(fullFilename, 'rb'), delimiter=',')	#input reader instance

			""" REPORT WHAT FILE IS BEING PROCESSED """
			print "\n"
			print "\n"
			print(Fore.CYAN+"######################################################")
			print(Fore.WHITE+"###  Processing file: "+str(fullFilename))
			print(Fore.CYAN+"######################################################")
			print "\n"
			print "\n"
			time.sleep(1)	#Quick 1.5 sec pause for readability

			""" START LOOPING THROUGH INPUT READER FILE"""
			for row in reader:
				PHONEDIGITS = re.sub("\D","",row['PHONE'])	#Convert Phone number field to pure numeric digits
				ALTPHONEDIGITS = re.sub("\D","",row['ALT_PHONE'])	#Convert Alternate phone field to pure numeric digits
				clean = True	#Start out with Clean set to True
				rowCount += 1	#Increment the rowCount Counter
				badstate = 0	#non-callable state counter
				nophone = 0		#no phone counter
				invalNPANXX = 0	#invalid NPANXX counter

				""" CHECK FOR MISSING OR FORBIDDEN STATES """
				if row['STATE'].upper() in forbiddenStates or row['STATE'] == '':
					print(Fore.YELLOW+Style.BRIGHT+" ---------> "+row['STATE']+" IS LISTED AS UN-CALLABLE")
					clean = False
					badstateCount += 1	#Increment the Bad State Counter

				""" CHECK FOR PHONE NUMBER THAT ISN'T 10-DIGITS """
				if len(PHONEDIGITS) != 10:
					print(Fore.YELLOW+" ---------> "+PHONEDIGITS+" IS NOT 10-DIGITS, IT'S "+str(len(PHONEDIGITS)))
					clean = False
					nophoneCount += 1	#Increment the No Phone Counter

				""" IF PHONE NUMBER IS NOT MISSING OR LESS THAN 10-DIGITS PERFORM A NPANXX CHECK ON LOCAL MYSQL"""
				if len(PHONEDIGITS) == 10:
					nophone = 0
					phoneprefix = PHONEDIGITS[0:6]
					conn.execute("SELECT COUNT(*) FROM nanpa_codes WHERE npanxx = %s AND ocn !=''", (phoneprefix))
					result = conn.fetchone()
					
					""" IF NO RESULT COMES BACK FROM NPANXX TABLE THEN RECORD IS NOT CLEAN """
					if result[0] < 1:
						print(Fore.YELLOW+Style.BRIGHT+" ---------> "+PHONEDIGITS+" HAS FAILED NPA NXX CHECK!")
						clean = False
						invalNPANXXCount += 1	#Increment the Invalid NPANXX Counter

				""" CHECK IF PHONEDIGITS IS A DUPLICATE """
				if PHONEDIGITS in phoneList:
					print(Fore.RED+" ---------> "+PHONEDIGITS+" IS A DUPLICATE NUMBER IN THIS FILE!")
					clean = False
					dupeCount += 1	#Increment Duplicate Counter if PHONEDIGITS are in the phoneList already

				""" IF NOTHING FAILS THEN PROCEED TO LOOKUP PROPER CID AND THEN WRITE THE RECORD TO VICIDIAL-FORMAT OUTPUT FILE"""
				if clean == True:
					""" Lookup appropriate DID_CID table for campaign to populate CID info from """
					if CAMPID == '9000':
						if row['STATE'] in camp9000CIDdict:
							cid_did = camp9000CIDdict[row['STATE']]
						else:
							print(Fore.YELLOW+"NO CID DID FOR "+row['STATE']+" IN CAMPAIGN "+CAMPID)
							cid_did = ''
					if CAMPID == '9001':
						if row['STATE'] in camp9001CIDdict:
							cid_did = camp9001CIDdict[row['STATE']]
						else:
							print(Fore.YELLOW+"NO CID DID FOR "+row['STATE']+" IN CAMPAIGN "+CAMPID)
							cid_did = ''
					else:
						cid_did = ''
					print(Fore.GREEN+Style.BRIGHT+"   GOOD RECORD : "+row['FNAME']+"|"+row['CITY']+"|"+row['STATE']+"|"+row['ZIP']+"|"+PHONEDIGITS+"|"+ALTPHONEDIGITS+"|"+cid_did)		#Print out the variables 

					# Write the row out in the Vicidial standard format to the current working directory
					writer.writerow(['','',str(LISTID),'1',PHONEDIGITS,'',row['FNAME'],'','','','','',row['CITY'].strip(),row['STATE'].strip(),'',row['ZIP'].strip(),'','','',ALTPHONEDIGITS,'',cid_did,'','','','',''])		
					rowWrites += 1	#Increment rowWrites by 1
					phoneList.append(PHONEDIGITS)	#Add PHONEDIGITS to the phoneList
				
				""" IF CLEAN IS SET TO FALSE INCREMENT THE FAILED COUNT"""
				if clean == False:
					failedRow += 1

			""" DONE WITH THE FILE SO MOVE IT TO THE PROCESSED DIR"""
			os.rename('/root/leads/CSV_IN/'+filename, '/root/leads/CSV_IN/PROCESSED/'+filename)	#Move read/processed files to another DIR

	""" PRINT OUT SOME SUMMARY STATS BEFORE MOVING ON"""
	print "\n"
	print "\n"
	print "RECORDS WITH NON-CALLABLE STATE : ",str(badstateCount)
	print "RECORDS WITH MISSING PHONE NUMBERS : ",str(nophoneCount)
	print "RECORDS WITH INVALID NPA-NXX : ",str(invalNPANXXCount)
	print "RECORDS WITH DUPLICATE PHONE NUMBERS : ",str(dupeCount)
	print "\n"
	print "		TOTAL ROWS READ : [ "+str(rowCount)+" ]"
	print "		TOTAL ROWS WRITTEN : [ "+str(rowWrites)+" ]"
	print "		TOTAL FAILED ROWS : [ "+str(failedRow)+" ]"
	print "\n"
	print "\n"
	time.sleep(3)




	


cleanIt()	#Initial cleaner to get rid of bad states, intra-list dupes and missing or invalid phone numbers
finalCheck()	#Checks for dupes in the list and archive table and writes out a new outputfile
createActiveList()
transferToDestination()
passItToVicidial()
deleteFiles()
