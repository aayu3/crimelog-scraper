#from tabula import convert_into
import googlemaps
import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup

import pandas as pd
cluster = MongoClient(os.environ.get("MONGODBURL"))
gmap_key = os.environ.get("GMAPKEY")

db = cluster["Crime-DB"]
collection = db["Crime-Data"]

#import re
#from urllib import request

'''response0 = request.urlopen(
    "https://police.illinois.edu/info/daily-crime-log/")


q = str(response0.read())
script_url = re.findall(r"https://blogs.illinois.edu/pc.*?\.js", q)[1]
print(script_url)
response1 = request.urlopen(script_url)
q1 = str(response1.read())
pdf_url = re.search(r"https://blogs.illinois.edu/files.*?\.pdf", q1)[0]
print(pdf_url)
response = request.urlopen(pdf_url)
webContent = response.read()
f = open('crime-log.pdf', 'wb')
f.write(webContent)
f.close()


convert_into("crime-log.pdf",
             "illinoisCrime.csv",
             output_format="csv",
             pages='all')
'''


# Create a DataFrame from the table data

# Specify the URL of the WordPress webpage
url = 'https://police.illinois.edu/info/daily-crime-log/'  # Replace with the actual URL

# Send a GET request and retrieve the HTML content
response = requests.get(url)
html_content = response.text

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

# Find the table element
table = soup.find('table')

# Extract the table rows and cells
rows = table.find_all('tr')

# Process the table data and create a list of lists
table_data = []
for row in rows:
    cells = row.find_all('td')
    row_data = [cell.get_text(strip=True) for cell in cells]
    table_data.append(row_data)

csvFile = pd.DataFrame(table_data, columns=['Number', 'Reported Date/Time', 'Occurred From Date/Time', 'Location', 'Description', 'Disposition'])
csvFile = csvFile.drop(csvFile.index[0])
csvFile['Date reported'] = csvFile['Reported Date/Time'].apply(lambda x: 'UNKNOWN' if 'UNKNOWN' in x else x.split(' ')[0])
csvFile['Time reported'] = csvFile['Reported Date/Time'].apply(lambda x: 'UNKNOWN' if 'UNKNOWN' in x else x.split(' ')[1])
csvFile['Date occurred'] = csvFile['Occurred From Date/Time'].apply(lambda x: 'UNKNOWN' if 'UNKNOWN' in x else x.split(' ')[0])
csvFile['Time occurred'] = csvFile['Occurred From Date/Time'].apply(lambda x: 'UNKNOWN' if 'UNKNOWN' in x else x.split(' ')[1])
gmaps = googlemaps.Client(gmap_key)

file = "illinoisCrime.csv"
location_bias_long = 88.2272
location_bias_lat = 40.1020
location_bias_radius = 500
locations_long_lat = []

csvFile["Longitude"] = np.nan
csvFile["Latitude"] = np.nan

for index, row in csvFile.iterrows():
    if row["Location"].split()[-1].lower() not in [
            "urbana", "champaign", "chicago", "il", "campus"
    ]:
        row["Location"] += " Urbana champaign"
    place_candidate = gmaps.find_place(
        row["Location"],
        'textquery',
        location_bias="circle:" + str(location_bias_radius) + "@" +
        str(location_bias_lat) + "," + str(location_bias_long))
    crime_desc = row["Description"].replace("‐", "-")
    crime_incident = row['Number'].replace("‐", "-")
    if place_candidate['status'] == "OK":
        if (row['Date reported']!="Date reported"):
            if (collection.find_one(filter={"CaseID":crime_incident,"Description":crime_desc}) is None):
                location = gmaps.reverse_geocode(
                    place_candidate['candidates'][0]['place_id'])
                formattedrow = {
                    "CaseID": crime_incident,
                    "DateReported": row['Date reported'],
                    "TimeReported": row['Time reported'],
                    "DateOccurred": row['Date occurred'],
                    "TimeOccurred": row['Time occurred'],
                    "Latitude": float(location[0]["geometry"]["location"]["lat"]),
                    "Longitude": float(location[0]["geometry"]["location"]["lng"]),
                    "StreetAddress": location[0]
                    ["formatted_address"],  # I could also use row["Location"], but I think this one is better
                    "Description": crime_desc,
                    "Disposition": row["Disposition"]
                }
                print("Added: %s:%s" % (crime_incident, crime_desc))
                collection.insert_one(formattedrow)
            else:
                db_check = collection.find_one(filter={"CaseID":row['Number'],"Description":row["Description"]})
                if (db_check != None):           
                    if db_check['Disposition']==row['Disposition']:
                        print("Skipped: %s:%s"%(row['Number'],row['Description']))
                    else:
                        collection.find_one_and_update(filter={"CaseID":row['Number'],"Description":row["Description"]},update={'$set':{'Disposition': row['Disposition']}})
                        print("Updated disposition for " + row['Number'] + " from " + db_check['Disposition'] + " to " + row['Disposition'])
        
