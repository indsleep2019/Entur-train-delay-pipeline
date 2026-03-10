import os
import requests
import datetime
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# ---------- ENTUR ----------
CLIENT_NAME = "indl-r21-pipeline"
URL = "https://api.entur.io/realtime/v1/rest/estimated-timetable"

headers = {"Client-Name": CLIENT_NAME}
response = requests.get(URL, headers=headers)
response.raise_for_status()

# TODO: Bytt til ekte parsing senere
today = datetime.date.today().isoformat()
rows = [
    (today, "R21", 5),
    (today, "R21", 0),
]

# ---------- SNOWFLAKE AUTH ----------
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

# ---------- CONNECT ----------
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    private_key=pkb,
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"]
)

cs = conn.cursor()

# ---------- TABLE ----------
cs.execute("""
CREATE TABLE IF NOT EXISTS R21_GITHUB_STAGE (
    DATE DATE,
    LINE STRING,
    DELAY_MINUTES NUMBER
)
""")

# ---------- INSERT ----------
cs.executemany(
    "INSERT INTO R21_GITHUB_STAGE (DATE, LINE, DELAY_MINUTES) VALUES (%s,%s,%s)",
    rows
)

cs.close()
conn.close()

print("Loaded data into Snowflake")
