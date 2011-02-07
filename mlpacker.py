#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
#ES: MLPacker: empaqueta el archivo de una lista de correo en una base de datos sqlite3
#EN: MLPacker: packs a mailing list archive onto a sqlite3 database
#
#ES: Por ahora es compatible con listas de correo estilo ezmlm
#EN: By now it is compatible with ezmlm mail-lists
#
# ES: Parámetros
# -D: directorio raíz donde el script tratará de encontrar archivos de listas de correo
# -d: directorio de donde leer el archivo
# -o: fichero de base de datos a crear (por defecto, email.db)
# -v: muestra información adicional
#
# EN: Arguments
# -D: root directory where the script will try to find mailist archives
# -d: directory from where to read the archive
# -o: database file to create (default, email.db)
# -v: be verbose
#
import sys, os, glob
import email
import getopt
import re
try:
	import sqlite3
except:
	from pysqlite2 import dbapi2 as sqlite3

majorVersion = "0"
minorVersion = "99"


Verbose = False
Directory = None
RecurseDirectory = None
DatabaseFile = None
Recurse = False

dbConn = None

def connectDB(dbFile):
	return sqlite3.connect(dbFile)

def disconnectDB(dbc):
	dbc.close()

def createTables(conn):
	c = conn.cursor()

	c.execute('''create table if not exists email
	(id INTEGER PRIMARY KEY AUTOINCREMENT,msgFrom VARCHAR(400),
	msgTo VARCHAR(400),msgCc VARCHAR(400), msgBcc VARCHAR(400),msgDate VARCHAR(400),msgSubject VARCHAR(400),msgID VARCHAR(320),msgInReplyTo VARCHAR(400),
	msgReferences VARCHAR(400),Message TEXT
	)''')

	c.execute('''create table if not exists mlindex
	(dir VARCHAR(5),id VARCHAR(5),
	key VARCHAR(100),msgSubject VARCHAR(400),date VARCHAR(30), msgID VARCHAR(320),msgSender VARCHAR(400)
	)''')

	conn.commit()
	c.close()

def insertMsg(dbc,mFrom,mTo,mCc,mBcc,mDate,mSubject,mID,mInReply,mRef,Message):
	if mFrom == None:
		mFrom = "<NO FROM>"

	if mTo == None:
		mTo = "<NO TO>"
	
	if mCc == None:
		mCc = "<NO CC>"
	
	if mBcc == None:
		mBcc = "<NO BCC>"
	
	if mDate == None:
		mDate = "<NO DATE>"
	
	if mSubject == None:
		mSubject = "<NO SUBJECT>"
	
	if mID == None:
		mID = "<NO ID>"
	
	if mInReply == None:
		mInReply = "<NO IN REPLY TO>"
	
	if mRef == None:
		mRef = "<NO REFERENCES>"
	
	if Message == None:
		Message = "<NO MESSAGE>"

	sql = "insert into email values (NULL,?,?,?,?,?,?,?,?,?,?)"
	dbc.execute(sql,(mFrom,mTo,mCc,mBcc,mDate,mSubject,mID,mInReply,mRef,Message) )

def getDirectories(d):
	"Get all directories where the archive/ dir exists, recursively"
	dirList = []
	for dir in os.listdir(d):
		if dir[0] != ".":
			if os.path.isdir( os.path.join(os.path.join(d,dir),"archive") ):
				dirList.append(dir)
	return dirList
#/getDirectories

def getFiles(dbConn,d):
	"Get all message files onto the directories recursively"
	for root, dirs, files in os.walk(d):
		for name in files:
			# if != index is a msg file
			if name != "index":
				msg = loadMsg(os.path.join(root,name))

				if Verbose == True:
					debugMsg(msg)

				prepareMsg(dbConn,msg)
			else:
				if Verbose == True:
					print "INDEX: [" + root + "]/[" + name + "]---[" + os.path.basename(root) + "]"

				# parse indes file and insert into the .db
				addIndex(dbConn,root)
	dbConn.commit()
#/getFiles

def loadMsg(f):
	fp = open(f, 'rb')
	msg = email.message_from_file(fp)
	fp.close()
	return msg
#/loadMsg

def addIndex(dbc,file):
	dir = os.path.basename(file)
	fileName = os.path.join(file,"index")

	f = open(fileName)
	while 1:
		l1 = f.readline()
		l2 = f.readline()
		if not l1 or not l2:
			break

		m = re.search('^(?P<id>[0-9]+):\s+(?P<key>\w+)\s(?P<subject>.*)', l1)
		id = m.group('id')
		key = m.group('key')
		msgSubject = m.group('subject')

		n = re.search('^\s+(?P<idate>.*);(?P<msgID>\S+)\s+(?P<sender>.*)', l2)
		date = n.group('idate')
		msgID = n.group('msgID')
		sender = n.group('sender')

		sql = "insert into mlindex values (?,?,?,?,?,?,?)"
		dbc.execute(sql,(dir,id,key,msgSubject,date,msgID,sender) )
		pass #/next
	f.close()
#/addIndex

def debugMsg(m):
	date = m.get('Date')
	mFrom = m.get("From")
	to = m.get("To")
	subject = m.get("Subject")
	messageID = m.get('Message-ID')
	charset = m.get_charset()
	inReply = m.get("In-Reply-To")
	references = m.get("References")

	print "-------------------------------------------------------------------------"
	if date != None:
		print "Date: " + date

	if mFrom != None:
		mFrom = mFrom.replace("\n ","")
		print "From: " + mFrom

	if to != None:
		to = to.replace("\n ","")
		print "To: " + to
	
	if subject != None:
		subject = subject.replace("\n ","")
		print "Subject: " + subject

	if messageID != None:
		messageID = messageID.replace("\n ","")
		print "Message-ID: " + messageID

	if charset != None:
		print "Charset: " + charset

	if inReply != None:
		inReply = inReply.replace("\n ","")
		print "In-Reply-To: " + inReply

	if references != None:
		references = references.replace("\n ","")
		print "References: " + references
#/debugMsg

def prepareMsg(dbConn,m):
	insertMsg(dbConn,m.get('From'),m.get('To'),m.get('Cc'),m.get('Bcc'),m.get('Date'),m.get('Subject'),m.get('Message-ID'),m.get('In-Reply-To'),m.get('References'),m.as_string())
#/prepareMsg

def showCopy():
	print "MLPacker v" + majorVersion + "." + minorVersion
	print "(c) 2011 - Román Ramírez <rramirez AT-NOSPAM rootedcon.es>\n"
#/showCopy

def mlpackerUsage():
	showCopy()
	print "./mlpacker [-v] -d directory with ezmlm archive [-o outfile sqlite3] [-h]\n"
	print "\t-v (--verbose)\t\tGive details"
	print "\t-d (--directory)\tezmlm archive directory"
	print "\t-D (--maindirectory)\tbase directory for vpopmail to get maillist dirs from it"
	print "\t-o (--output)\t\toutput sqlit3 file"
	print "\t-h (--help)\t\tthis help\n"
#/mlpackerUsage

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "D:d:o:vh", ["help","verbose", "maindirectory=", "directory=","output="])
	except getopt.GetoptError, err:
		# print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		usage()
		sys.exit(2)
	retDirectory = None
	recurseDirectory = None
	verbose = False
	doRecurse = False
	dbFile = "email.db"
	for o, p in opts:
		if o in ("-v","--verbose"):
			verbose = True
		elif o in ("-h", "--help"):
			mlpackerUsage()
			sys.exit(0)
		elif o in ("-d", "--directory"):
			if p:
				retDirectory = p
		elif o in ("-D", "--maindirectory"):
			if p:
				recurseDirectory = p
			doRecurse = True
		elif o in ("-o", "--output"):
			if p:
				dbFile = p
		else:
			assert False, "There are unknown options in the command line"

	if retDirectory == None and doRecurse == False:
		print "WARNING: mlpacker: We do need at least one parameter, mailing list archive directory (-d or -D to recurse)\n"
		mlpackerUsage()
		sys.exit(1)

	return retDirectory,dbFile,verbose,doRecurse,recurseDirectory
	#/for

if __name__ == "__main__":
	(Directory,DatabaseFile,Verbose,Recurse,RecurseDirectory) = main()
#/if main

if Verbose == True:
	print "Parsing directory ["+ Directory + "]"

if Recurse == False:
	#(1) Open sqlite3 database
	dbConn = connectDB(DatabaseFile)
	#(2) If tables exist next call won't do anything
	createTables(dbConn)
	#(3) Open and walk the directory adding messages to the db
	getFiles(dbConn,Directory)
	#(4) Close the DB
	disconnectDB(dbConn)
else:
	#(1) Open the main directory trying to find archive/ dirs inside
	for d in getDirectories(RecurseDirectory):
		#(2) Process bucle opening every mailist directory
		#(3) ... check at non-recurse step
		rdbConn = connectDB(d + ".db")
		createTables(rdbConn)
		getFiles(rdbConn,os.path.join(RecurseDirectory,d))
		disconnectDB(rdbConn)
