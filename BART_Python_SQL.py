# MSAN 691: Relational Databases
# Bart Project
# Devin Bowers, Davi Schumacher, Nimesh Sinha, Yu Tian
# September 7, 2017

# coding=utf-8
import psycopg2
import xlrd
import zipfile
import os
import datetime
import time


def ProcessBart(tmpDir, dataDir, SQLConn=None, schema='cls', table='bart'):

    # Delete all data structures created by this function in Database
    SQLCursor = SQLConn.cursor()
    SQLCursor.execute("DROP TABLE IF EXISTS %s.%s" % (schema, table))
    SQLConn.commit()

    os.system("rm %s/%s" % (tmpDir, "toLoad.csv"))

    # Unzip all files in dataDir and move to tmpDir
    data_path = os.path.abspath(dataDir) + "/"
    for f in os.listdir(dataDir):
        if f.endswith(".zip"):
            zf = zipfile.ZipFile(data_path+f, 'r')
            zf.extractall(path=tmpDir)
            zf.close()

    # Process each excel file
    numSheets = 3   # We only care about the first three sheets: Weekday, Saturday, Sunday
    tmp_path = os.path.abspath(tmpDir) + "/"
    data = []
    for root, dirnames, files in os.walk(tmp_path):
        for f in files:
            wb = xlrd.open_workbook(os.path.join(root, f))
            date_info = {}
            for i in range(numSheets):
                s = wb.sheet_by_index(i)
                ## Get date from first sheet, since other sheets reference the date cell in
                ## first sheet
                if i == 0:
                    date_cell = s.cell(0, 6)
                    date = datetime.datetime(*xlrd.xldate_as_tuple(date_cell.value, wb.datemode))
                    date_info["year"] = date.year
                    date_info["month"] = date.month
                day_type_cell = s.cell(0, 3)
                day_type = day_type_cell.value
                ## Some versions include "ADJUSTED" in day type.
                ## We remove those for consistency
                if len(day_type.split(" ")) == 2:
                    day_type = day_type.split(" ")[1]

                exits = []
                entries = []
                for x in s.col(0):
                    if x.ctype == 2:
                        exits.append(int(x.value))
                        entries.append(int(x.value))
                    if x.ctype == 1 and len(x.value) == 2:
                        exits.append(x.value)
                        entries.append(x.value)

                for x in range(2, len(exits)+2):
                    row = s.row(x)
                    for n in range(1, len(entries)+1):
                        val = row[n]
                        data.append((date_info['month'], date_info['year'],
                                    day_type, entries[n-1], exits[x-2],
                                    val.value))

    ## Load into CSV
    print len(data)
    csv_file = "toLoad.csv"
    with open(tmp_path+csv_file, 'wb') as f:
        for d in data:
            csv_line = "%i,%i,%s,%s,%s,%f\n" % d
            f.write(csv_line)

    ## Load into DB
    SQLCursor.execute("""
          CREATE TABLE %s.%s
          (
          mon int
          , yr int
          , daytype varchar(15)
          , start varchar(2)
          , term varchar(2)
          , riders float
          );""" % (schema, table))
    SQLCursor.execute("COPY %s.%s FROM '%s' CSV;" % (schema, table, tmp_path + csv_file))
    SQLConn.commit()

    print "DONE"


root = '/Users/Devin/Desktop/MSAN/691_RelationalDatabases/BART'
tmpDir = root + '/bartTemp'
dataDir = root + '/BART_DATA'

LCLconnR = psycopg2.connect("dbname=msan_691 host=localhost")

start = time.time()
ProcessBart(tmpDir, dataDir, SQLConn=LCLconnR)
end = time.time()
print end-start

