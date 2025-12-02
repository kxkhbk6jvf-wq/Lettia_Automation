from datetime import datetime

tests = ["25-9-14", "25-8-24", "2025/08/24", "2025-08-24 07:45:10"]

for t in tests:
    try:
        print(t, datetime.strptime(t, "%y-%m-%d").date())
    except:
        print(t, "FAILED")

