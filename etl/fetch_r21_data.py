import requests
import pandas as pd
from datetime import datetime
import os

# -----------------------------
# KONFIG
# -----------------------------
URL = "https://api.entur.io/realtime/v1/rest/et"
HEADERS = {
    "ET-Client-Name": "indl-r21-pipeline"
}

# -----------------------------
# HENT DATA FRA ENTUR
# -----------------------------
response = requests.get(URL, headers=HEADERS)

if response.status_code != 200:
    raise Exception(f"Feil ved henting av data: {response.status_code}")

data = response.json()

# -----------------------------
# PARSE DATA
# -----------------------------
records = []

try:
    deliveries = data["Siri"]["ServiceDelivery"]["EstimatedTimetableDelivery"]

    for delivery in deliveries:
        journeys = delivery.get("EstimatedJourneyVersionFrame", [])

        for frame in journeys:
            vehicle_journeys = frame.get("EstimatedVehicleJourney", [])

            for journey in vehicle_journeys:

                line_ref = journey.get("LineRef", {}).get("value", "")

                # Filtrer kun R21
                if "R21" not in line_ref:
                    continue

                journey_id = journey.get("FramedVehicleJourneyRef", {}).get("DatedVehicleJourneyRef", "")

                calls = journey.get("EstimatedCalls", {}).get("EstimatedCall", [])

                for call in calls:
                    aimed = call.get("AimedDepartureTime")
                    expected = call.get("ExpectedDepartureTime")
                    recorded = call.get("RecordedAtTime")

                    if aimed and expected:
                        delay_minutes = (
                            pd.to_datetime(expected) - pd.to_datetime(aimed)
                        ).total_seconds() / 60

                        records.append({
                            "DATE": aimed[:10],
                            "TRAIN": line_ref,
                            "AIMED_DEPARTURE": aimed,
                            "EXPECTED_DEPARTURE": expected,
                            "DELAY_MINUTES": round(delay_minutes, 2),
                            "RECORDED_AT": recorded,
                            "LOAD_TIMESTAMP": datetime.utcnow().isoformat()
                        })

except Exception as e:
    print("Parsing-feil:", e)

# -----------------------------
# LAG DATAFRAME
# -----------------------------
df = pd.DataFrame(records)

if df.empty:
    print("Ingen R21-data funnet")
    exit()

# -----------------------------
# LAGRE CSV
# -----------------------------
today = datetime.utcnow().strftime("%Y-%m-%d")

output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

file_path = f"{output_dir}/{today}_R21.csv"

df.to_csv(file_path, index=False)

print(f"Fil lagret: {file_path}")
