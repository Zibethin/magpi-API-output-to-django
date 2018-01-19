import requests, sys, os, django, datetime, logging
from lxml import objectify
from time import gmtime, strftime

sys.path.append("")                         #Location of DJANGO folder HERE
os.environ["DJANGO_SETTINGS_MODULE"] = ""   #DJANGO MODULE NAME HERE
django.setup()
from data.models import report

#################### initialising log file

now = strftime("%Y-%m-%d %H:%M:%S", gmtime()) #for readable string
today = datetime.datetime.now()
logging.basicConfig(filename="/var/www/cron/data_processing.log", level=logging.INFO)
logging.info("Created on " + str(today) + "\n")

#################### getting data from API

try:
    print "Connecting to magpi for report..."
    data = {'username': 'someusername', 'accesstoken': '2222222', 'surveyid': '2222222'}
    r = requests.post("https://www.magpi.com/api/surveydata/", data=data, timeout=400)
except Exception:
    print "Connection to magpi survey using API has failed - please check data"
    logging.error("Failed to connect to the survey")
    sys.exit()

root = objectify.fromstring(r.content)
if "<error>" in r.content:
    print "Exiting script, data contains errors, check API connection"
    logging.error("Failed to connect to the survey: "+ r.content)
    sys.exit()

#backup code for when the API doesn't work
#r = open("test.txt","r")
#root = objectify.fromstring(r.read())

#################### transforming the data
data = []
# data2 = []
try:
    for survey in root['SurveyData']:
        test = {children1.tag: children1.text for children1 in survey.getchildren()}
        data.append(test)
except Exception:
    print "Exiting script, data contains errors"
    logging.error("The data format received from the API is not valid: "+ r.content)
    sys.exit()

################### cleaning database of all magpi data - only after all exceptions were raised if needed

reports = report.objects.all()
index = []
for r in reports:
    if r.magpi_id > 0:
        r.delete()
print "Report currently contains " + str(len(index)) + " entries."


#################### functions used to clean/save data

def toInt(var):
    if var == None:
        return 0         #assume that 'No data' === 0
    else:
        return int(var)

def notNull(var):
    if var == None:
        return ""
    else:
        return var

def processAndSaveData(row, indicator, num_patients, age):
    report = {}
    try:
        report['report_start'] = datetime.datetime.strptime(datetime.datetime.strptime(row["Date"], '%Y-%m-%d').strftime('%d/%m/%y'),'%d/%m/%y')
        report['report_end'] = report['report_start'] + datetime.timedelta(days=1)
    except Exception:        # Check for errors in date
        try:        #Note: DateStamp = Last Submit
            report['report_start'] = datetime.datetime.strptime(datetime.datetime.strptime(row["DateStamp"], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%y'),'%d/%m/%y')
            report['report_end'] = report['report_start'] + datetime.timedelta(days=1)
            print "Error processing report DATE, assumed date to be LAST SUBMIT:  " + str(row["Id"]) + " " + row["DateStamp"]
            logging.error("Error processing DATE for magpi uuid, assumed date to be LAST SUBMIT: " + str(row["Id"]) + " " +  row["DateStamp"])
        except Exception:    
            print "Error processing report DATE and LAST SUBMIT " + str(row["Id"])
            logging.error("Error processing DATE and LAST SUBMIT for magpi uuid: " + str(row["Id"]))
            return

    try:
        fake = report['id']
    except Exception:        #Check for errors in id
        print "Error processing id for " + str(row["Id"])
        logging.error("Error processing id for magpi uuid:" + str(row["Id"]))
        return

    report['indicator'] = indicator
    report['comments'] = notNull(row["Other"])
    report['magpi_id'] = row["Id"]
    report['date_updated'] = now
    #print(report)
    report = report(**report)
    report.save()


#################### saving data in reports

indicatorNames = {'indicator1': 'indicator 1', 'indicator2': 'Indicator 2'}

print "Processing dataset..."
i=0
for row in data:
    exists = 0
    for id in index:
        if str(id) == str(row['Id']):
            #print(str(id) + ' already in database - not added again')
            exists = 1
            break
    if exists == 0:
        #print("Added new row id:" + str(row['Id']))
        logging.info("Added new row id:" + str(row['Id']))

        number_people = toInt(row["people1"]) + toInt(row["people2"])
        processAndSaveData(row, indicatorNames['indicator2'], number_people, "all")
        processAndSaveData(row, indicatorNames['indicator2'], toInt(row["people1"]), "1")
        processAndSaveData(row, indicatorNames['indicator2'], toInt(row["people2"]), "2")

        i=i+10;

print 'Added ' + str(i) + ' entries to report from magpi file.'
