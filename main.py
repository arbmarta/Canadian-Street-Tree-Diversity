# This code processes the street tree inventories into one merged dataframe, cleans the botanical names, and adds species, genus, and family

import os
import pandas as pd
import numpy as np

## -------------------------------------------- IMPORT AND MERGE INVENTORIES -------------------------------------------
#region

# Cities that use species codes
cities_with_species_codes = ['Mississauga', 'Toronto', 'Ottawa', 'Moncton', 'Halifax']

print("🔄 Loading cities with species codes...")

# Initialize an empty list to hold the data
data_frames = []

# Loop and import cities with species codes
for city in cities_with_species_codes:
    print(f"  📥 Importing: {city}")
    file_name = fr'Inventories\{city}.csv'

    if os.path.exists(file_name):
        df = pd.read_csv(file_name)
        df['City'] = city  # Ensure City column is added if not already present
        data_frames.append(df)
    else:
        print(f"  ⚠️ {city} file does not exist.")

if len(data_frames) == 0:
    raise ValueError("❌ No cities XLSX files loaded... Ensure they have been placed in Inventories/ directory.")

print("✅ Finished loading species-coded cities.")

# Concatenate all DataFrames
master_df = pd.concat(data_frames, ignore_index=True)

# Import compiled species codes
print("🔄 Importing species codes lookup table...")
tree_codes_df = pd.read_csv('Non-Inventory Datasets/Tree Codes/Compiled Tree Codes.csv')

# Standardize case for join
tree_codes_df['Code'] = tree_codes_df['Code'].str.lower()
master_df['Botanical Name'] = master_df['Botanical Name'].str.lower()

print("🔄 Replacing species codes with botanical names...")
# Merge to replace codes with botanical names
merged_df = master_df.merge(
    tree_codes_df[['City', 'Code', 'Botanical Name']],
    left_on=['City', 'Botanical Name'],
    right_on=['City', 'Code'],
    how='left',
    suffixes=('', '_corrected')
)

# Use corrected Botanical Name if available
merged_df['Botanical Name'] = merged_df['Botanical Name_corrected'].combine_first(merged_df['Botanical Name'])

# Drop helper columns
master_df = merged_df.drop(columns=['Code', 'Botanical Name_corrected'])

print("✅ Botanical names updated from species codes.\n")

# All other cities (without species codes)
cities_without_species_codes = [
    'Victoria', 'Vancouver', 'New Westminster', 'Maple Ridge', 'Kelowna', 'Calgary',
    'Edmonton', 'Strathcona County', 'Lethbridge', 'Regina', 'Winnipeg', 'Windsor',
    'Waterloo', 'Kitchener', 'Guelph', 'Burlington', 'Welland', 'St. Catharines',
    'Niagara Falls', 'Ajax', 'Whitby', 'Peterborough', 'Kingston', 'Montreal', 'Longueuil',
    'Quebec City', 'Fredericton'
]

print("🔄 Loading cities without species codes...")

# Initialize a new list for these cities
additional_data_frames = []

# Loop through and import each city's file
for city in cities_without_species_codes:
    print(f"  📥 Importing: {city}")
    file_name = fr'Inventories\{city}.csv'

    if os.path.exists(file_name):
        df = pd.read_csv(file_name)
        df['City'] = city  # Ensure the City column is present
        additional_data_frames.append(df)
    else:
        print(f"  ⚠️ {city} file does not exist.")

# Concatenate and append to existing master_df
if additional_data_frames:
    print("🔗 Appending non-species-coded cities to master DataFrame...")
    other_df = pd.concat(additional_data_frames, ignore_index=True)
    master_df = pd.concat([master_df, other_df], ignore_index=True)
    print("✅ All cities successfully combined.")
else:
    print("⚠️ No additional cities loaded from cities_without_species_codes.")

# endregion

## ------------------------------------- REMOVE ROWS WITH MISSING DAUIDs AND CTUIDs ------------------------------------
#region

# Processing status
print("\n🔍 Checking master inventory for missing DAUID and CTUID...")

# Count the number of trees in the master inventory before processing
initial_row_count = master_df.shape[0]
print(f"Initial number of rows before removal: {initial_row_count}")

# Remove rows where both DAUID and CTUID are missing
master_df = master_df[~(master_df["DAUID"].isna() & master_df["CTUID"].isna())]

# Report how many rows were removed
num_rows_removed = initial_row_count - master_df.shape[0]
print(f"Number of trees where both DAUID and CTUID were missing: {num_rows_removed}")

# Identify rows where CTUID is missing
missing_ctuid = master_df[master_df["CTUID"].isna()]
unique_dauids = missing_ctuid["DAUID"].dropna().unique()
num_missing_ctuid = len(missing_ctuid)

print(f"Number of trees where CTUID is missing: {num_missing_ctuid}")
print("Unique DAUIDs where CTUID is missing:")
print(", ".join(map(str, unique_dauids)))  # comma-separated single-line list

# Reference data to fill in missing CTUID and City based on DAUID
data_dict_df = pd.DataFrame({
    'DAUID': [59150883, 59150891, 59153562, 59170272, 35240431, 35100286, 35201466, 35204675, 35204821, 35205067,
              35370553, 24580007, 24662985, 24662707, 24662951, 24662821, 24662885, 24660984, 24663395, 24230066,
              13100304, 13070131, 12090576, 59154073],
    'CTUID': [9330045.01, 9330025, 9330059.08, 9350001, 5370204, 5210100.01, 5350003, 5350210.04, 5350012.01, 5350200.01,
              5590043.02, 4620886.03, 4620288, 4620276, 4620585.01, 4620290.05, 4620290.09, 4620322.03, 4620390, 4210310,
              3200009, 3050006, 2050112, 9330202.01],
    'City': ['Vancouver', 'Vancouver', 'Vancouver', 'Victoria', 'Burlington', 'Kingston', 'Toronto', 'Toronto', 'Toronto', 'Toronto',
             'Windsor', 'Longueuil', 'Montreal', 'Montreal', 'Montreal', 'Montreal', 'Montreal', 'Montreal', 'Montreal', 'Quebec City',
             'Fredericton', 'Moncton', 'Halifax', 'New Westminster']
})

# Merge with the lookup DataFrame on DAUID
master_df = master_df.merge(data_dict_df, on='DAUID', how='left', suffixes=('', '_dict'))

# Fill missing CTUID and City values from the dictionary
master_df['CTUID'] = master_df['CTUID'].combine_first(master_df['CTUID_dict'])
master_df['City'] = master_df['City'].combine_first(master_df['City_dict'])
master_df['City'] = master_df['City'].str.strip().str.lower()

# Drop helper columns from the dictionary
master_df = master_df.drop(columns=['CTUID_dict', 'City_dict'])

print("✅ CTUID and City values updated using DAUID reference table.")

# Final check: count remaining rows with missing CTUID
num_missing_ctuid_after = master_df["CTUID"].isna().sum()
print(f"📉 Number of rows where CTUID is still missing after filling: {num_missing_ctuid_after:,}\n")

#endregion

## ------------------------------------------- CORRECT BOTANICAL NAME COLUMN -------------------------------------------
#region

# Count the number of unique botanical names
num_unique_botanical = master_df['Botanical Name'].nunique()
print(f"Number of unique botanical names before species cleaning: {num_unique_botanical}")

# Lowercase and strip
master_df['Botanical Name'] = master_df['Botanical Name'].str.lower().str.strip()

# Clean and standardize Botanical Name
pattern_replacements = {
    '\u00A0': '',         # non-breaking space
    ' x ': ' ',           # hybrid indicator
    ' × ': ' ',           # another hybrid form
    "'": '',              # remove single quotes
    '"': '',              # remove double quotes
    "sp.": 'spp.',        # normalize sp. to spp.
    "species": 'spp.'     # normalize "species" too
}

for pattern, repl in pattern_replacements.items():
    master_df['Botanical Name'] = master_df['Botanical Name'].str.replace(pattern, repl, regex=False)

# Replace empty strings and fill NA
master_df['Botanical Name'] = master_df['Botanical Name'].replace('', pd.NA).fillna('missing')

# Remove any non-living trees
print("🪵 Removing non-living tree records...")
master_df = master_df[~master_df["Botanical Name"].isin(["dead", "stump", "stump spp.", "stump for", "shrub",
                                                         "shrubs", "vine", "vines", "hedge", "wildlife snag",
                                                         "vacant"])]

# Count the number of unique botanical names
num_unique_botanical = master_df['Botanical Name'].nunique()
print(f"✅ Unique botanical names (post-cleaning): {num_unique_botanical:,}\n")

#endregion

## ------------------------------------------- ADD SPECIES AND GENUS COLUMNS -------------------------------------------
#region

print("🌱 Extracting species and genus...")

# Simplify to the species level (i.e., remove cultivar names)
def simplify_name(name):
    if pd.isna(name):
        return name
    parts = name.strip().split()

    # If name starts with 'x', skip it
    if parts[0].lower() == "x" and len(parts) >= 3:
        parts = parts[1:]

    if len(parts) == 1:
        return f"{parts[0]} spp."
    elif len(parts) >= 3 and parts[1].lower() == "x":
        return f"{parts[0]} {parts[2]}"
    elif len(parts) >= 3 and parts[1].lower() in ["var", "var."]:
        return f"{parts[0]} {parts[2]}"
    else:
        return f"{parts[0]} {parts[1]}"

master_df["Species"] = master_df["Botanical Name"].apply(simplify_name)
master_df["Species"] = master_df["Species"].str.replace(r'\b(sp|spp|ssp)\b\.?$', 'spp.', regex=True)

# Apply corrections from lookup table
print("🔁 Applying species corrections...\n")

# Load and clean the corrections table
corrections_df = pd.read_csv('Non-Inventory Datasets/Species Corrections.csv')
corrections_df['Species'] = corrections_df['Species'].str.strip().str.lower()
corrections_df['Correction'] = corrections_df['Correction'].str.strip()

# Ensure Species column is lowercase for consistent mapping
master_df['Species'] = master_df['Species'].str.strip().str.lower()

# Create mapping dictionary
corrections_dict = dict(zip(corrections_df['Species'], corrections_df['Correction']))

# Apply corrections using map (faster than replace)
master_df['Species'] = master_df['Species'].map(corrections_dict).fillna(master_df['Species'])

# Extract Genus (handle hybrids that start with × or x)
master_df["Genus"] = master_df["Species"].apply(
    lambda x: x.split()[1] if x.startswith(("× ", "x ")) else x.split()[0]
).str.lower()

#endregion

## ------------------------------------------ PRINT LIST OF SPECIES AND GENERA -----------------------------------------
#region

# Load accepted nomenclature list
accepted_nomenclature_df = pd.read_csv('Non-Inventory Datasets/Reviewed Nomenclature.csv')
reviewed_genera = set(accepted_nomenclature_df['Genus'].dropna().str.lower().unique())

# Identify genera not in the reviewed list
missing_genera = sorted(set(master_df['Genus'].dropna().unique()) - reviewed_genera)

if missing_genera:
    print("🚨 Genera in master_df but not found in reviewed list:")
    for genus in missing_genera:
        print(genus)
else:
    print("✅ All genera are present in the reviewed dataset.")

# Load reviewed species list
reviewed_species = set(accepted_nomenclature_df['Species'].dropna().str.lower().unique())

# Identify species not in the reviewed list
missing_species = sorted(set(master_df['Species'].dropna().unique()) - reviewed_species)

if missing_species:
    print("\n🚨 Species in master_df but not found in reviewed list:")
    for species in missing_species:
        print(species)
else:
    print("\n✅ All species are present in the reviewed dataset.")

#endregion

## ------------------------------------------------- ADD FAMILY COLUMN -------------------------------------------------
#region

print("\n🔍 Merging genus-to-family lookup...")

# Import data dictionary of genus and families
families_df = pd.read_csv('Non-Inventory Datasets/Families.csv')
master_df = master_df.merge(families_df[['Genus', 'Family']], on='Genus', how='left')

# Print genera with missing families
print("⚠️ Genera with missing families (by city):")
missing_families = (
    master_df[
        master_df['Family'].isna() &
        ~master_df['Genus'].isin(['missing', 'unknown'])
    ]
    .groupby(['Genus', 'City'])
    .size()
    .reset_index(name='Count')
)
print(missing_families)

#endregion

## ---------------------------------- REPORT THE NUMBER OF MISSING OR UNKNOWN SPECIES ----------------------------------
#region

# Ensure all cities are represented in the result
all_cities = master_df['City'].unique()

# Report the number of missing and unknown species
missing_unknown_counts = (
    master_df[master_df['Species'].isin(['missing', 'unknown spp.'])]
    .groupby('City')['Species']
    .value_counts()
    .unstack(fill_value=0)
).reindex(all_cities, fill_value=0).sort_index()

print(missing_unknown_counts)

# Drop the missing and unknown species from the master_df
master_df = master_df[~master_df['Species'].isin(['missing', 'unknown spp.'])]

# Compute total tree count per city
total_per_city = master_df.groupby('City').size().reindex(all_cities, fill_value=0)

# Compute percentage table
percent_missing_unknown = (missing_unknown_counts.T / total_per_city).T.fillna(0).round(4) * 100

print("\n📊 Percent of trees with missing or unknown species per city:")
print(percent_missing_unknown)

#endregion

## ----------------------------------- PRINT UNIQUE SPECIES, GENUS, AND FAMILY COUNTS ----------------------------------
#region

print("\n📊 Final summary of unique taxa:")

num_species = master_df['Species'].nunique(dropna=True)
num_genera = master_df['Genus'].nunique(dropna=True)
num_families = master_df['Family'].nunique(dropna=True)

print(f"🧬 Unique species: {num_species:,}")
print(f"🌿 Unique genera: {num_genera:,}")
print(f"🌳 Unique families: {num_families:,}")

#endregion

## --------------------------------------------- SAVE THE DATASET TO A CSV ---------------------------------------------
#region

# Print the number of rows
print(f"🔢 Number of rows in master_df: {len(master_df):,}")

# Save the result
print("\n💾 Saving final master DataFrame to 'Master DF.csv'...")
master_df.to_csv('Master df.csv', index=False)
print("✅ File saved.\n")

#endregion
