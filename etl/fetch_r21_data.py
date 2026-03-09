import requests
import datetime
import csv
import os
import xml.etree.ElementTree as ET

# ---- Config ----
CLIENT_NAME = "indl-r21-pipeline"
URL = "https://api.entur.io/realtime/v1/rest/estimated-timetable"

# ---- Hent data ----
headers = {
    "Client-Name": CLIENT_NAME
}

response = requests.get(URL, headers=headers)
response.raise_for_status()

# ---- Parse XML ----
root = ET.fromstring(response.content)

# ---- Forbered CSV ----
today = datetime.date.today().isoformat()
os.makedirs("data", exist_ok=True)
filename = f"data/{today}_R21.csv"

rows = []

# ---- Finn tog ----
for journey in root.iter():
    if journey.tag.endswith("EstimatedVehicleJourney"):
        line_ref = None
        delay = None
        aimed = None
        estimated = None

        for elem in journey.iter():
            tag = elem.tag.split("}")[-1]

            if tag == "LineRef":
                line_ref = elem.text

            if tag == "AimedDepartureTime":
                aimed = elem.text

            if tag == "ExpectedDepartureTime":
                estimated = elem.text

        # Filtrer R21
        if line_ref and "R21" in line_ref:
            rows.append([today, line_ref, aimed, estimated])

# ---- Skriv CSV ----
with open(filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["date", "line", "aimed_departure", "expected_departure"])
    writer.writerows(rows)

print(f"Saved {len(rows)} R21 records to {filename}")
