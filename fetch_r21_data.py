import os
import datetime
import requests
import xml.etree.ElementTree as ET
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# -----------------------------
# HENT DATA FRA ENTUR (XML)
# -----------------------------
URL = "https://api.entur.io/realtime/v1/rest/et"

HEADERS = {
    "ET-Client-Name": "github-r21-pipeline"
}

response = requests.get(URL, headers=HEADERS)

print("Status:", response.status_code)

if response.status_code != 200:
    raise Exception(f"API error: {response.status_code}")

root = ET.fromstring(response.content)

ns = {"siri": "http://www.siri.org.uk/siri"}

records = []

for journey in root.findall(".//siri:EstimatedVehicleJourney", ns):

    line_ref = journey.find("siri:LineRef", ns)
    if line_ref is None:
        continue

    line = line_ref.text

    if "R21" not in line:
        continue

    journey_ref = journey.find(".//siri:DatedVehicleJourneyRef", ns)
    journey_id = journey_ref.text if journey_ref is not None else "UNKNOWN"

    calls = journey.findall(".//siri:EstimatedCall", ns)

    for call in calls:
        aimed = call.find("siri:AimedDepartureTime", ns)
        expected = call.find("siri:ExpectedDepartureTime", ns)

        if aimed is None or expected is None:
            continue

        aimed_time = aimed.text
        expected_time = expected.text

        delay = (
            datetime.datetime.fromisoformat(expected_time.replace("Z", "+00:00")) -
            datetime.datetime.fromisoformat(aimed_time.replace("Z", "+00:00"))
        ).total_seconds() / 60

        records.append((
            aimed_time[:10],
            journey_id,
            round(delay, 2)
        ))

# fallback
today = datetime.date.today().isoformat()
if not records:
    records = [(today, "NO_DATA", 0)]

print("Antall records:", len(records))

# -----------------------------
# SNOWFLAKE AUTH (ROBUST FIX)
# -----------------------------
private_key_raw = os.environ.get("SNOWFLAKE_PRIVATE_KEY")

if not private_key_raw:
    raise Exception("SNOWFLAKE_PRIVATE_KEY mangler i GitHub Secrets")

# 🔥 håndter både riktig og feil formatering
if "\\n" in private_key_raw:
    private_key_fixed = private_key_raw.replace("\\n", "\n")
else:
    private_key_fixed = private_key_raw

private_key = serialization.load_pem_private_key(
    private_key_fixed.encode(),
    password=None,
)

pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# -----------------------------
# CONNECT TO SNOWFLAKE
# -----------------------------
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    private_key=pkb,
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"]
)

cs = conn.cursor()

# 🔥 sett context eksplisitt
cs.execute("""
CREATE TABLE IF NOT EXISTS TRAIN_DELAY_DB.RAW.R21_GITHUB_STAGE (
    DATE DATE,
    TRAIN STRING,
    DELAY_MINUTES NUMBER
)
""")

cs.executemany(
    "INSERT INTO TRAIN_DELAY_DB.RAW.R21_GITHUB_STAGE VALUES (%s,%s,%s)",
    records
)

cs.close()
conn.close()

print(f"Lastet {len(records)} rader til Snowflake")
