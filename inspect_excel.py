import pandas as pd

f2024 = r'Data/Total Votes Results_3675bf50-5eeb-4407-b538-c04b909ccd08.xlsx'
f2022 = r'Data/Total Votes Results_7ceee513-d4c4-4b68-9397-68a73d22dc4f.xlsx'

# Check 2024 County Results
df24 = pd.read_excel(f2024, sheet_name='County Results')
print(f"2024 County Results - Total rows: {len(df24)}")
print(f"Unique County values (first 5): {list(df24['County'].unique()[:5])}")
print(df24[df24['Party'].notna()].head(5).to_string())
print()

# Check 2022 Total Votes sheet
df22_tv = pd.read_excel(f2022, sheet_name='Total Votes')
print(f"2022 Total Votes sheet - Total rows: {len(df22_tv)}")
print(df22_tv.head(5).to_string())
print()

# Unique Ballot Names (to understand format)
df22 = pd.read_excel(f2022, sheet_name='Precinct Results')
print("Sample Ballot Names:")
print(df22[df22['Party'].notna()]['Ballot Name'].unique()[:15])
