import pandas as pd

df = pd.read_csv("RTO_vehicle_codes.csv")
df["RegNo"] = df["RegNo"].str.upper()

vehicle_number = input("Enter Vehicle Number: ").upper()
vehicle_number = ''.join(filter(str.isalnum, vehicle_number))

rto_code = vehicle_number[:4]

result = df[df["RegNo"] == rto_code]

if not result.empty:
    city = result.iloc[0]["Place"]
    state = result.iloc[0]["State"]

    print("\nVehicle Details")
    print("---------------")
    print("Vehicle Number:", vehicle_number)
    print("RTO Code:", rto_code)
    print("City:", city)
    print("State:", state)
else:
    print("Vehicle location not found in database")