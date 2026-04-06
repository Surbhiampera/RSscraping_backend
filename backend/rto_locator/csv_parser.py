import pandas as pd
import re

df = pd.read_csv("RTO_clean.csv")

def convert_code(code):
    if pd.isna(code):
        return None
    
    code = str(code)

    # Remove numeric prefix (like 35, 28 etc.)
    code = re.sub(r'^\d+', '', code)

    # Extract letters + digits
    m = re.match(r'([A-Z]{2})(\d+)', code)
    if not m:
        return None
    
    state = m.group(1)
    office = m.group(2)

    # Take first 2 digits and pad if needed
    office = office[:2].zfill(2)

    return state + office


df["vehicle_rto"] = df["code"].apply(convert_code)

# Keep rows where conversion worked
df = df.dropna(subset=["vehicle_rto"])

# Keep first entry per vehicle RTO
df = df.drop_duplicates(subset=["vehicle_rto"])

# Include state_code in final output
final_df = df[["vehicle_rto", "city", "state", "state_code"]]
final_df.columns = ["RegNo", "Place", "State", "StateCode"]

final_df.to_csv("RTO_vehicle_codes.csv", index=False)

print("Final dataset rows:", len(final_df))
