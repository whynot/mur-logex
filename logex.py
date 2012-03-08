import os
import glob
from dbfpy import dbf

stations = ("KMTI", "KLGL", "KMGR", "KSVC", "KWUT")
day = ''
month = ''
year = ''
log = []
print "Which station do you want to extract logs for?"
print "Please select a number:"
print "1: KMTI"
print "2: KLGL"
print "3: KMGR"
print "4: KSVC"
print "5: KWUT"
print
selection = raw_input("> ")
number = int(selection)
if number in range(1, 6):
    station = stations[number - 1]
    
    print "What date do you want to extract logs for?"
    print "Enter the date as: mm.dd.yyyy"
    selection = raw_input("> ")
    date = selection.split(".")
    month = date[0]
    day = date[1]
    year = date[2]
    date = month + day + year[2:4]

    # open all files that match the specified date and extract all log entries 
    # for the given station
    # F:\DAD\
    for filename in glob.glob(os.path.normpath('F:/DAD/Asplay/*%s.DBF' % date)):
        db = dbf.Dbf(filename)
        # print db
        for record in db:
        	if record['PLAYLIST'] == month + day + station:
        		log.append(record)
        db.close()

    if len(log) > 0:
        # sort log entries by actual play time
        log = sorted(log, key=lambda record: record[8])
        # print len(log)

        # Create new db and setup record fields
        db = dbf.Dbf(os.path.normpath('C:/IMPORT/asplay/%s%s.DBF' % (station, date)), new=True)
        db.addField(
            ("CUT", "C", 5),
            ("STATUS", "C", 1),
            ("TITLE", "C", 60),
            ("USER", "C", 8),
            ("PLAYER", "C", 2),
            ("DATE", "C", 8),
            ("SCHSTART", "C", 8),
            ("SCHDUR", "C", 8),
            ("ACTSTART", "C", 8),
            ("ACTDUR", "C", 8),
            ("ACTSTOP", "C", 8),
            ("TYPE", "C", 1),
            ("PLAYLIST", "C", 8),
            ("COMMENT", "C", 35),
            ("LINEID", "C", 10),
            ("ALTCUT", "C", 30),
            ("BOARDID", "C", 2),
            ("DEVICEID", "C", 3),
            ("GROUP", "C", 8),
            ("WASPLAYED", "C", 1),
        )
        # print db

        # loop through log entries and insert into ne db
        for record in log:
            rec = db.newRecord()
            rec["STATUS"] = record[1]
            rec["TITLE"] = record[2]
            rec["USER"] = record[3]
            rec["PLAYER"] = record[4]
            rec["DATE"] = record[5]
            rec["SCHSTART"] = record[6]
            rec["SCHDUR"] = record[7]
            rec["ACTSTART"] = record[8]
            rec["ACTDUR"] = record[9]
            rec["ACTSTOP"] = record[10]
            rec["TYPE"] = record[11]
            rec["PLAYLIST"] = record[12]
            rec["COMMENT"] = record[13]
            rec["LINEID"] = record[14]
            rec["ALTCUT"] = record[15]
            rec["BOARDID"] = record[16]
            rec["DEVICEID"] = record[17]
            rec["GROUP"] = record[18]
            rec["WASPLAYED"] = record[19]
            # If cut is played from a rotator then log the rotator cut number 
            # instead of the played cut
            if record[13].startswith('#R'):
                comment = record[13].split()
                rec["CUT"] = comment[1]
            else:
                rec["CUT"] = record[0]
            # Store record to database
            rec.store()
        db.close()
        # print db
        print "Finished! %s records were extracted for %s on %s/%s/%s" % (len(log), station, month, day, year)
    else:
        print "There are no log entries for %s on %s/%s/%s" % (station, month, day, year)
else:
    print "Sorry! That isn't one of the options."
    exit
    
# scan all logs for given day for specified station
# sort by actual play time
# replace played CUT with rotator CUT if applicable
# consolidate all log entries into new db
# save new db locally
