import datetime
import csv
import os

today = datetime.date.today().isoformat()
os.makedirs("data", exist_ok=True)

filename = f"data/{today}_R21.csv"

with open(filename, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["date","train","delay_minutes"])
    writer.writerow([today,"R21_0800",5])
    writer.writerow([today,"R21_1504",0])

print(f"Created {filename}")
