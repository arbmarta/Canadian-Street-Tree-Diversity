import pykew.powo as powo
from pykew.powo_terms import Name, Filters
import pandas as pd

# Import the species checker dataset
master_df = pd.read_csv('Non-Inventory Datasets/Reviewed Nomenclature.csv')

## -------------------------------------- GENERATE THE SPECIES NATIVE STATUS LIST --------------------------------------
#region

# List of Canadian provincial codes (TDWG Level 3)
provincial_codes = ['MAN', 'ONT', 'SAS', 'ABT', 'BRC', 'QUE', 'NBR', 'NSC']

# Load, merge, filter, and process
df_pivot = (
    pd.read_csv('Non-Inventory Datasets/Tree Nativity and Families/Distribution.csv', sep="|", quoting=3,
                encoding="utf-8", keep_default_na=False, low_memory=False)
    .merge(
        pd.read_csv('Non-Inventory Datasets/Tree Nativity and Families/Names.csv', sep="|", quoting=3,
                    encoding="utf-8", keep_default_na=False, low_memory=False),
        on='plant_name_id'
    )
    .query("area_code_l3 in @provincial_codes")
    .loc[:, ['plant_name_id', 'taxon_rank', 'family', 'genus_hybrid', 'genus', 'species_hybrid', 'species',
             'taxon_name', 'area_code_l3', 'introduced']]
    .rename(columns={'area_code_l3': 'Province', 'taxon_name': 'Botanical Name'})
    .pivot_table(index=['Botanical Name', 'family'], columns='Province', values='introduced', fill_value=2)
    .reset_index()
)

# Rename TDWG province codes to full province names
tdwg_to_province = {
    'BRC': 'British Columbia',
    'ABT': 'Alberta',
    'SAS': 'Saskatchewan',
    'MAN': 'Manitoba',
    'ONT': 'Ontario',
    'QUE': 'Quebec',
    'NSC': 'Nova Scotia',
    'NBR': 'New Brunswick'
}
df_pivot.rename(columns=tdwg_to_province, inplace=True)

# Convert 'introduced' to Native: 1 = native, 0 = not native
province_cols = list(tdwg_to_province.values())

# Replace introduced: 0 → 1 (native), everything else (1, 2, NaN) → 0 (not native)
df_pivot[province_cols] = df_pivot[province_cols].applymap(lambda x: 1 if x == 0 else 0)

# Clean and standardize 'Botanical Name' column
df_pivot['Botanical Name'] = (
    df_pivot['Botanical Name']
    .str.lower()
    .str.strip()
    .str.replace(" × ", " x ", regex=False)
    .str.replace("'", "", regex=False)
    .str.replace("xxxx ", "", regex=False)
    .str.replace("Ã—", "", regex=False)
    .str.replace("Ã", "", regex=False)
    .apply(lambda name: name + " spp." if isinstance(name, str) and len(name.strip().split()) == 1 else name)
)

# Create duplicate entries without the ' x '
hybrid_rows = df_pivot[df_pivot['Botanical Name'].str.contains(" x ", na=False)].copy()
hybrid_rows['Botanical Name'] = hybrid_rows['Botanical Name'].str.replace(" x ", " ", regex=False)

# Append the modified duplicates back to the original DataFrame
df_pivot = pd.concat([df_pivot, hybrid_rows], ignore_index=True)

# Save to a csv
df_pivot.to_csv('Non-Inventory Datasets/Tree Nativity and Families/Distribution Data.csv', index=False)
print("💾 Species distribution data saved as 'Distribution Data.csv'")

print(df_pivot)

#endregion

## ------------------------------------------- CHECK THE SPELLING OF SPECIES -------------------------------------------
#region

# Extract Genus and Species portions from 'Botanical Name'
master_df[['Genus Portion', 'Species Portion']] = master_df['Species'].str.split(n=1, expand=True)

# Drop rows with missing genus or missing/unspecified species
master_df = master_df.dropna(subset=['Genus Portion', 'Species Portion'])
master_df = master_df[master_df['Species Portion'].str.lower() != 'spp.']

# List to hold unmatched species
unmatched_species = []

# Total for progress tracking
total = len(master_df)

# Loop through each row to query POWO with Genus and Species
for i, row in master_df.iterrows():
    genus = row['Genus Portion']
    species = row['Species Portion']
    print(f"🔎 Checking {i + 1} of {total}: {genus} {species}")
    try:
        results = list(powo.search({Name.genus: genus, Name.species: species}, filters=[Filters.species]))

        found_valid = False
        for r in results:
            if r.get('rank', '').lower() == 'species' and r.get('accepted', False):
                found_valid = True
                break

        if not found_valid:
            unmatched_species.append(f"{genus} {species}")

    except Exception as e:
        unmatched_species.append(f"{genus} {species} (error)")

# Print list of unmatched species
if unmatched_species:
    print(f"\n🚨 {len(unmatched_species)} of {total} species not identified or accepted in POWO:")
    for name in unmatched_species:
        print(name)
else:
    print(f"\n✅ All {total} species successfully identified and accepted in POWO.")

#endregion

## ------------------------------------------- CHECK THE SPELLING OF SPECIES -------------------------------------------
#region

# Extract Genus and Species portions from 'Botanical Name'
master_df[['Genus Portion', 'Species Portion']] = master_df['Species'].str.split(n=1, expand=True)

# Drop rows with missing genus or missing/unspecified species
master_df = master_df.dropna(subset=['Genus Portion', 'Species Portion'])
master_df = master_df[master_df['Species Portion'].str.lower() != 'spp.']

# Loop through each row to query POWO with Genus and Species
for i, row in master_df.iterrows():
    genus = row['Genus Portion']
    species = row['Species Portion']
    print(f"Checking {i + 1} of {len(master_df)}: {genus} {species}")
    try:
        results = list(powo.search({Name.genus: genus, Name.species: species}, filters=[Filters.species]))

        found_valid = False
        for r in results:
            if r.get('rank', '').lower() == 'species' and r.get('accepted', False):
                found_valid = True
                break

        if not found_valid:
            print(f"  ❌ Not found or not accepted in POWO: {genus} {species}")

    except Exception as e:
        print(f"  ⚠️ Error with species '{genus} {species}': {e}")

#endregion

## -------------------------------------------- CHECK THE SPELLING OF GENERA -------------------------------------------
#region

# Ensure genus names are unique and non-null
unique_genera = master_df['Genus'].dropna().unique()

# Loop through each genus
for genus in unique_genera:
    print(f"Searching genus: {genus}")
    try:
        results = list(powo.search({Name.genus: genus}, filters=[Filters.genera]))
        found_valid = False
        for r in results:
            if r.get('rank', '').lower() == 'genus' and r.get('accepted', False):
                found_valid = True
                break
        if not found_valid:
            print(f"  ❌ No accepted genus-level result for: {genus}")
    except Exception as e:
        print(f"  ⚠️ Error with genus '{genus}': {e}")

#endregion

## ------------------------------------------- CHECK THE SPELLING OF FAMILIES ------------------------------------------
#region

# Ensure family names are unique and non-null
unique_families = master_df['Family'].dropna().unique()

# Loop through each family
for family in unique_families:
    print(f"🔍 Searching family: {family}")
    try:
        results = powo.search({Name.family: family}, filters=[Filters.families])
        for r in results:
            if r.get('rank', '').lower() == 'family' and r.get('accepted', False):
                print(f"✅ Confirmed family: {r.get('name')}")
                break  # Exit the loop after first valid match
        else:
            print(f"❌ No accepted 'family' rank result for: {family}")
    except Exception as e:
        print(f"⚠️ Error with family '{family}': {e}")

#endregion
