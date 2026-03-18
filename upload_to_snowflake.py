import snowflake.connector
import os
import glob

files = glob.glob("data/*.csv")

if not files:
    raise Exception("Ingen filer funnet")

latest_file = max(files, key=os.path.getctime)

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"]
)

cursor = conn.cursor()

cursor.execute(f"PUT file://{latest_file} @%R21_RAW AUTO_COMPRESS=TRUE")

cursor.execute("""
COPY INTO R21_RAW
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
""")

cursor.close()
conn.close()

print("Lastet:", latest_file)
