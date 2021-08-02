from tabula.io import convert_into
import googlemaps
import os
import pandas as pd
import numpy as np
from pymongo import MongoClient

cluster = MongoClient(os.environ.get("MONGODBURL"))

db = cluster["Crime-DB"]
collection = db["Crime-Data"]

import re
from urllib import request

response0 = request.urlopen(
    "https://police.illinois.edu/crime-reporting/daily-crime-log/")

q = str(response0.read())
script_url = re.search(r"https://illinois.edu/blog/pc.*?\.js", q)[0]
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

gmaps = googlemaps.Client(key=os.environ.get("MAPS_API_KEY"))

file = "illinoisCrime.csv"
location_bias_long = 88.2272
location_bias_lat = 40.1020
location_bias_radius = 500
locations_long_lat = []
csvFile = pd.read_csv(file, encoding="utf8")
csvFile["Longitude"] = np.nan
csvFile["Latitude"] = np.nan

for index, row in csvFile.iterrows():
    if row["General Location"].split()[-1].lower() not in [
            "urbana", "champaign", "chicago", "il"
    ]:
        row["General Location"] += " Urbana champaign"
    place_candidate = gmaps.find_place(
        row["General Location"],
        'textquery',
        location_bias="circle:" + str(location_bias_radius) + "@" +
        str(location_bias_lat) + "," + str(location_bias_long))
    crime_desc = row["Crime Description"].replace("‐", "-")
    crime_incident = row['Incident'].replace("‐", "-")
    if place_candidate['status'] == "OK":
        if collection.find_one(filter={
                "CaseID": crime_incident,
                "Description": crime_desc
        }) is None:
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
            print("Skipped: %s:%s" % (crime_incident, crime_desc))
