import os
import datetime
import requests
import xml.etree.ElementTree as ET
import snowflake.connector
from cryptography.hazmat.primitives import serialization
import time

# -----------------------------
# HENT DATA FRA ENTUR (XML) MED RETRY
# -----------------------------
URL = "https://api.entur.io/realtime/v1/rest/et"

HEADERS = {
    "ET-Client-Name": "github-r21-pipeline"
}

MAX_RETRIES = 3

for attempt in range(MAX_RETRIES):
    try:
        response = requests.get(URL, headers=HEADERS, timeout=30)

        print("Status:", response.status_code)

        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code}")

        break  # SUCCESS

    except Exception as e:
        print(f"Forsøk {attempt+1} feilet: {e}")

        if attempt == MAX_RETRIES - 1:
            raise Exception("API feilet etter flere forsøk")

        time.sleep(5)

# -----------------------------
# PARSE XML
# -----------------------------
root = ET.fromstring(response.content)

ns = {"siri": "http://www.siri.org.uk/siri"}

records = []

now_utc = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
load_time = now_utc.isoformat()

# -----------------------------
# FILTERKRITERIER
# -----------------------------
VALID_LINE = "VYG:Line:R21"

VALID_STOPS = {
    "NSR:Quay:845", "NSR:Quay:843",   # Kambo
    "NSR:Quay:250", "NSR:Quay:248",
    "NSR:Quay:247", "NSR:Quay:246"    # Lysaker
}

# -----------------------------
# PARSE + FILTER
# -----------------------------
for journey in root.findall(".//siri:EstimatedVehicleJourney", ns):

    line_ref = journey.find("siri:LineRef", ns)
    if line_ref is None:
        continue

    line = line_ref.text

    # 1️⃣ Kun R21 tog
    if VALID_LINE not in line:
        continue

    journey_ref = journey.find(".//siri:DatedVehicleJourneyRef", ns)
    journey_id = journey_ref.text if journey_ref is not None else "UNKNOWN"

    calls = journey.findall(".//siri:EstimatedCall", ns)

    for call in calls:
        aimed = call.find("siri:AimedDepartureTime", ns)
        expected = call.find("siri:ExpectedDepartureTime", ns)
        stop = call.find("siri:StopPointRef", ns)

        if aimed is None or expected is None or stop is None:
            continue

        stop_id = stop.text

        # 2️⃣ Kun dine stopp
        if stop_id not in VALID_STOPS:
            continue

        aimed_time = aimed.text
        expected_time = expected.text

        aimed_dt = datetime.datetime.fromisoformat(aimed_time.replace("Z", "+00:00"))
        expected_dt = datetime.datetime.fromisoformat(expected_time.replace("Z", "+00:00"))

        # 3️⃣ Kun siste 60 min før avgang
        diff_minutes = (aimed_dt - now_utc).total_seconds() / 60

        if diff_minutes < 0 or diff_minutes > 60:
            continue

        delay = (expected_dt - aimed_dt).total_seconds() / 60

        records.append((
            aimed_dt.date().isoformat(),
            line,
            journey_id,
            stop_id,
            round(delay, 2),
            load_time
        ))

# fallback
today = datetime.date.today().isoformat()
if not records:
    records = [(today, "NO_DATA", "NO_JOURNEY", "UNKNOWN", 0, load_time)]

print("Antall records etter filtrering:", len(records))

# -----------------------------
# SNOWFLAKE AUTH
# -----------------------------
private_key_raw = os.environ.get("SNOWFLAKE_PRIVATE_KEY")

if not private_key_raw:
    raise Exception("SNOWFLAKE_PRIVATE_KEY mangler")

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
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database="TRAIN_DELAY_DB",
    schema="RAW",
    role="SYSADMIN"
)

cs = conn.cursor()

# -----------------------------
# INSERT (BATCH)
# -----------------------------
BATCH_SIZE = 10000

for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i + BATCH_SIZE]

    cs.executemany(
        "INSERT INTO TRAIN_DELAY_DB.RAW.R21_GITHUB_STAGE "
        '("DATE", "LINE", "JOURNEY_ID", "STOP_ID", "DELAY_MINUTES", "LOAD_TIMESTAMP") '
        "VALUES (%s,%s,%s,%s,%s,%s)",
        batch
    )

    print(f"Lastet batch {i//BATCH_SIZE + 1}: {len(batch)} rader")

cs.close()
conn.close()

print(f"Lastet totalt {len(records)} rader til Snowflake")
