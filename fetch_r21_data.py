import os
import datetime
import requests
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# -----------------------------
# HENT DATA FRA ENTUR
# -----------------------------
URL = "https://api.entur.io/realtime/v1/rest/et"

HEADERS = {
    "ET-Client-Name": "github-r21-pipeline"
}

response = requests.get(URL, headers=HEADERS)

if response.status_code != 200:
    raise Exception(f"API error: {response.status_code}")

data = response.json()

records = []

deliveries = data.get("Siri", {}).get("ServiceDelivery", {}).get("EstimatedTimetableDelivery", [])

for delivery in deliveries:
    frames = delivery.get("EstimatedJourneyVersionFrame", [])

    for frame in frames:
        journeys = frame.get("EstimatedVehicleJourney", [])

        for journey in journeys:

            line = journey.get("LineRef", {}).get("value", "")

            # Kun R21
            if "R21" not in line:
                continue

            journey_id = journey.get("FramedVehicleJourneyRef", {}).get("DatedVehicleJourneyRef", "")

            calls = journey.get("EstimatedCalls", {}).get("EstimatedCall", [])

            for call in calls:
                aimed = call.get("AimedDepartureTime")
                expected = call.get("ExpectedDepartureTime")

                if aimed and expected:
                    delay = (pd.to_datetime(expected) - pd.to_datetime(aimed)).total_seconds() / 60

                    records.append((
                        aimed[:10],          # DATE
                        journey_id,         # TRAIN
                        round(delay, 2)     # DELAY
                    ))

# fallback hvis ingen data
today = datetime.date.today().isoformat()

if not records:
    records = [(today, "NO_DATA", 0)]

# -----------------------------
# SNOWFLAKE AUTENTISERING
# -----------------------------
private_key = serialization.load_pem_private_key(
    os.environ["SNOWFLAKE_PRIVATE_KEY"].encode(),
    password=None,
    backend=default_backend()
)

pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# -----------------------------
# KOBLE TIL SNOWFLAKE
# -----------------------------
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    private_key=pkb,
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"]
)

cs = conn.cursor()

# -----------------------------
# LAG TABELL HVIS IKKE FINNES
# -----------------------------
cs.execute("""
CREATE TABLE IF NOT EXISTS R21_GITHUB_STAGE (
    DATE DATE,
    TRAIN STRING,
    DELAY_MINUTES NUMBER
)
""")

# -----------------------------
# LAST INN DATA
# -----------------------------
cs.executemany(
    "INSERT INTO R21_GITHUB_STAGE VALUES (%s,%s,%s)",
    records
)

cs.close()
conn.close()

print(f"Lastet {len(records)} rader til Snowflake")
