import pandas as pd

df = pd.read_csv("cc32d3e2-7ea3-4b6b-94ab-85e57f6a0a3a.csv")

# Fix: cast state_code to str as well
df["RTO_CODE"] = df["state_code"].astype(str) + df["office_code"].astype(str).str.zfill(2)

# Select only required columns
clean_df = df[["RTO_CODE", "office_name", "state_name", "state_code"]]

# Remove duplicates
clean_df = clean_df.drop_duplicates()

# Rename columns
clean_df.columns = ["code", "city", "state", "state_code"]

# Save new CSV
clean_df.to_csv("RTO_clean.csv", index=False)
print("Clean RTO dataset created!")
