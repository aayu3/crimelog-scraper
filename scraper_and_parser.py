# from tabula import convert_into
import googlemaps
import os
import numpy as np
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
import pandas as pd
import datetime

cluster = MongoClient(os.environ.get("MONGODBURL"))
gmap_key = os.environ.get("GMAPKEY")

db = cluster["Crime-DB"]
collection = db["Crime-Data"]

def parse_datetime(input_string):
    # Split the input string by spaces
    date_time_parts = input_string.split()
    
    # Initialize date and time variables
    date_occurred = "UNKNOWN"
    time_occurred = "UNKNOWN"

    # Check each part of the string for date or time
    for part in date_time_parts:
        # Attempt to parse as a date
        try:
            parsed_date = parse(part)
            if parsed_date.time() == datetime.time(0):
                date_occurred = parsed_date.strftime("%m/%d/%Y")
            continue
        except ValueError:
            pass
        
        # Attempt to parse as a time
        try:
            parsed_time = parse(part).time()
            time_occurred = parsed_time.strftime("%H:%M:%S")
            continue
        except ValueError:
            pass

    return date_occurred, time_occurred

# Create a DataFrame from the table data

# Specify the URL of the WordPress webpage
url = "https://police.illinois.edu/info/daily-crime-log/"  # Replace with the actual URL

# Send a GET request and retrieve the HTML content
response = requests.get(url)
html_content = response.text

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, "html.parser")

# Find the table element
table = soup.find("table")

# Extract the table rows and cells
body = table.find("tbody")
rows = body.find_all("tr")

# Process the table data and create a list of lists
table_data = []
for row in rows:
    cells = row.find_all("td")
    row_data = [cell.get_text(strip=True) for cell in cells]
    table_data.append(row_data)

csvFile = pd.DataFrame(
    table_data,
    columns=[
        "Number",
        "Reported Date/Time",
        "Occurred From Date/Time",
        "Location",
        "Description",
        "Disposition",
    ],
)

csvFile["Date occurred"], csvFile["Time occurred"] = zip(*csvFile["Occurred From Date/Time"].apply(parse_datetime))
csvFile["Date occurred"].fillna("UNKNOWN", inplace=True)
csvFile["Time occurred"].fillna("UNKNOWN", inplace=True)

csvFile["Date reported"], csvFile["Time reported"] = zip(*csvFile["Reported Date/Time"].apply(parse_datetime))
csvFile["Date reported"].fillna("UNKNOWN", inplace=True)
csvFile["Time reported"].fillna("UNKNOWN", inplace=True)
# csvFile["Date reported"] = csvFile["Reported Date/Time"].apply(
#     lambda x: "UNKNOWN" if "UNKNOWN" in x else x.split(" ")[0]
# )
# csvFile["Time reported"] = csvFile["Reported Date/Time"].apply(
#     lambda x: "UNKNOWN" if "UNKNOWN" in x else x.split(" ")[1]
# )

# csvFile["Date occurred"] = csvFile["Occurred From Date/Time"].apply(
#     lambda x: "UNKNOWN" if "UNKNOWN" in x else x.split(" ")[0]
# )
# csvFile["Time occurred"] = csvFile["Occurred From Date/Time"].apply(
#     lambda x: "UNKNOWN" if "UNKNOWN" in x else x.split(" ")[1]
# )
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
        "urbana",
        "champaign",
        "chicago",
        "il",
        "campus",
    ]:
        row["Location"] += " Urbana champaign"

    crime_desc = row["Description"].replace("‐", "-")
    crime_incident = row["Number"].replace("‐", "-")
    case = collection.find_one(
        filter={
            "CaseID": crime_incident,
        }
    )
    if case is None:
        place_candidate = gmaps.find_place(
            row["Location"],
            "textquery",
            location_bias="circle:"
            + str(location_bias_radius)
            + "@"
            + str(location_bias_lat)
            + ","
            + str(location_bias_long),
        )
        if place_candidate["status"] == "OK":
            location = gmaps.reverse_geocode(
                place_candidate["candidates"][0]["place_id"]
            )
            formattedrow = {
                "CaseID": crime_incident,
                "DateReported": row["Date reported"],
                "TimeReported": row["Time reported"],
                "DateOccurred": row["Date occurred"],
                "TimeOccurred": row["Time occurred"],
                "Latitude": float(location[0]["geometry"]["location"]["lat"]),
                "Longitude": float(location[0]["geometry"]["location"]["lng"]),
                "StreetAddress": location[0][
                    "formatted_address"
                ],  # I could also use row["Location"], but I think this one is better
                "Description": crime_desc,
                "Disposition": row["Disposition"],
            }
            print("Added: %s:%s" % (crime_incident, crime_desc))
            collection.insert_one(formattedrow)
    else:
        if case["Description"] == crime_desc:
            if case["Disposition"] == row["Disposition"]:
                print("Skipped: %s:%s" % (row["Number"], row["Description"]))
            else:
                collection.find_one_and_update(
                    filter={
                        "CaseID": row["Number"],
                        "Description": row["Description"],
                    },
                    update={"$set": {"Disposition": row["Disposition"]}},
                )
                print(
                    "Updated disposition for "
                    + row["Number"]
                    + " from "
                    + case["Disposition"]
                    + " to "
                    + row["Disposition"]
                )


cluster.close()
