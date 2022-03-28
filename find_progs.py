import json
import mariadb
import sys
from datetime import date, datetime, timedelta
from urllib.request import urlopen
import cfg
import dateutil.parser

# Config
tothLength = 92
defaultCart = "200043"
scheduleApiUrl = "https://upload.cambridge105.co.uk/ScheduleApi"
dateToday = date.today()
dateTomorrow = dateToday + timedelta(days=1)
dateStr = dateTomorrow.strftime("%Y-%m-%d")
midnightTomorrow = datetime.combine(dateTomorrow, datetime.min.time())
unix_ms_midnight = (midnightTomorrow.timestamp() * 1000)
# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=cfg.dbUser,
        password=cfg.dbPassword,
        host=cfg.dbHost,
        port=cfg.dbPort,
        database=cfg.dbDatabase

    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

# ========== Functions ==================

# Converts schedule API to a dict of the next three schedule items indexed by time since midnight:
# In: {"PID":"brian-oreilly","Name":"Brian O'Reilly","StartTime":"2021-12-22 06:00:00","EndTime":"2021-12-22 07:00:00","Type":null},{"PID":"breakfast","Name":"Cambridge Breakfast with Julian & Lucy","StartTime":"2021-12-22 07:00:00","EndTime":"2021-12-22 09:30:00","Type":null}
# Out: {21599908: 'brian-oreilly|breakfast|alex-elbro', 25199908: 'breakfast|alex-elbro|neil-whiteside'...
def jsonToSchedule(jsonObj):
  numScheduleItems = len(jsonObj)
  print("ScheduleItems=" + str(numScheduleItems))
  scheduleObj = {}
  for i in range(numScheduleItems):
    # only get schedule items tomorrow
    if (jsonObj[i]['StartTime'][0:10] == dateStr):
      scheduleTripleStr = jsonObj[i]['PID']
      if (i+1 < numScheduleItems):
        scheduleTripleStr += "|" + jsonObj[(i+1)]['PID']
        if (i+2) < numScheduleItems:
          scheduleTripleStr += "|" + jsonObj[(i+2)]['PID']
      ms_since_midnight = convertDateTimeToMsSinceMidnight(jsonObj[i]['StartTime'])
      scheduleObj[ms_since_midnight] = scheduleTripleStr
  return scheduleObj

def convertDateTimeToMsSinceMidnight(dt_str):
  dt = dateutil.parser.isoparse(dt_str)
  unix = dt.timestamp() * 1000
  return int(unix - unix_ms_midnight - (tothLength*1000))

def getCartNumberForPromo(title):
  sqlq = "SELECT NUMBER FROM CART WHERE GROUP_NAME='TOTHPROMO' AND TITLE LIKE '" + title + "' LIMIT 1;"
  cur.execute(sqlq)
  row = cur.fetchone()
  if row is None:
    return False
  else:
    print ("Cart found for " + title + ": " + str(row[0]))
    return row[0]


def overwriteToth(cartNumber,progTime):
  sqlQuery = "UPDATE LOG_LINES SET CART_NUMBER='" + str(cartNumber) + "'  WHERE LOG_NAME='" + dateStr + "-TOTH' AND START_TIME='" + str(progTime) + "' AND CART_NUMBER='" + defaultCart + "' LIMIT 1;"
  print(sqlQuery)
  affected_rows = cur.execute(sqlQuery)
  if affected_rows is None:
    return False
  elif affected_rows > 0:
    return True
  return False

# Fetch schedule as JSON from API
scheduleResponse = urlopen(scheduleApiUrl)
jsonObj = json.loads(scheduleResponse.read())
scheduleObj = jsonToSchedule(jsonObj)

# Iterate the schedule object checking for TOTHPROMO carts matching the group of three items
for progTime in scheduleObj:
  nextThree = scheduleObj[progTime]
  nextCart = getCartNumberForPromo("[Next]"+nextThree)
  notNextCart = getCartNumberForPromo(nextThree)
  if nextCart != False:
    overwriteToth(nextCart, progTime)
  if notNextCart != False:
    overwriteToth (notNextCart, progTime)
    overwriteToth (notNextCart, (progTime - 3600000))
    overwriteToth (notNextCart, (progTime - 7200000))
    overwriteToth (notNextCart, (progTime - 10800000))

cur.close()
conn.close()
