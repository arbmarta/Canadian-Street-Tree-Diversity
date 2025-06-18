import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import seaborn as sns

# Import the dataframe
master_df = pd.read_csv('Master df.csv', low_memory=False)
print(f"🔢 Number of rows in master_df: {len(master_df):,}")

## ----------------------------------------- IDENTIFY TREES IN DOWNTOWN AREAS ------------------------------------------
#region

# Cities with delineated downtown areas
cities_with_downtown_areas = ['burlington', 'calgary', 'edmonton', 'fredericton', 'guelph', 'halifax', 'kelowna',
                              'kingston', 'kitchener', 'lethbridge', 'longueuil', 'mississauga', 'moncton',
                              'montreal', 'ottawa', 'peterborough', 'quebec city', 'regina', 'st. catharines',
                              'toronto', 'vancouver', 'victoria', 'waterloo', 'winnipeg', 'windsor']

# Import the downtown areas file
downtown_areas_df = pd.read_csv('Non-Inventory Datasets/Downtown Areas.csv')

# Set the city column to lowercase
downtown_areas_df['City'] = downtown_areas_df['City'].str.lower()

# Merge datasets based on City and DAUID
master_df = master_df.merge(downtown_areas_df, on=['City', 'DAUID'], how='left')

# Fill NaN values in 'City Area' with 'Not Downtown'
master_df['City Area'] = master_df['City Area'].fillna('Not Downtown')

# Print the results of the merge
counts = master_df['City Area'].value_counts()
print(f"\n🏙️ Downtown: {counts.get('Downtown', 0):,}")
print(f"🏡 Not Downtown: {counts.get('Not Downtown', 0):,}")

#endregion

## ------------------------------------- IDENTIFY PROVINCE, ECOZONE, AND CITY SIZE -------------------------------------
#region

# Import the location index file
location_index_df = pd.read_csv('Non-Inventory Datasets/Location Index.csv')

# Set the city column to lowercase
location_index_df['City'] = location_index_df['City'].str.lower()

# Merge datasets based on City and DAUID
master_df = master_df.merge(location_index_df, on=['City'], how='left')

# Count of unique cities per Province
print("\n🏞️ Cities per Province:")
print(master_df.groupby('Province')['City'].nunique().sort_values(ascending=False))

# Count of unique cities per Ecozone
print("\n🌎 Cities per Ecozone:")
print(master_df.groupby('Ecozone')['City'].nunique().sort_values(ascending=False))

#endregion

## ---------------------------------------- IDENTIFY WHETHER A SPECIES IS NATIVE ---------------------------------------
#region

# Import the distribution data file
distribution_data_df = pd.read_csv('Non-Inventory Datasets/Tree Nativity and Families/Distribution Data.csv')
distribution_data_df = distribution_data_df.drop(columns='family')

# Lowercase and strip
distribution_data_df['Botanical Name'] = distribution_data_df['Botanical Name'].str.lower().str.strip()

# Sort by Botanical Name
distribution_data_df = distribution_data_df.sort_values('Botanical Name').reset_index(drop=True)

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
    distribution_data_df['Botanical Name'] = distribution_data_df['Botanical Name'].str.replace(pattern, repl, regex=False)

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

distribution_data_df["Species"] = distribution_data_df["Botanical Name"].apply(simplify_name)
distribution_data_df["Species"] = distribution_data_df["Species"].str.replace(r'\b(sp|spp|ssp)\b\.?$', 'spp.', regex=True)

# Apply corrections from lookup table
print("\n🔁 Applying species corrections...\n")

# Load and clean the corrections table
corrections_df = pd.read_csv('Non-Inventory Datasets/Species Corrections.csv')
corrections_df['Species'] = corrections_df['Species'].str.strip().str.lower()
corrections_df['Correction'] = corrections_df['Correction'].str.strip()

# Ensure Species column is lowercase for consistent mapping
distribution_data_df['Species'] = distribution_data_df['Species'].str.strip().str.lower()

# Create mapping dictionary
corrections_dict = dict(zip(corrections_df['Species'], corrections_df['Correction']))

# Apply corrections using map (faster than replace)
distribution_data_df['Species'] = distribution_data_df['Species'].map(corrections_dict).fillna(distribution_data_df['Species'])
distribution_data_df = distribution_data_df.drop(columns='Botanical Name')

# Make sure Province column names match those in distribution_data_df
native_lookup = distribution_data_df.set_index('Species')

# Prepare for merge: convert wide format to long format for merging
native_lookup_reset = native_lookup.reset_index().melt(id_vars='Species', var_name='Province', value_name='Native')

# Drop duplicates — keep the first occurrence (which will be the canonical species due to prior sorting)
native_lookup_reset = native_lookup_reset.drop_duplicates(subset=['Species', 'Province'], keep='first')

# Lowercase and strip for merge compatibility
master_df['Species'] = master_df['Species'].str.lower().str.strip()
master_df['Province'] = master_df['Province'].str.lower().str.strip()
native_lookup_reset['Province'] = native_lookup_reset['Province'].str.lower().str.strip()

# Merge to assign nativity
master_df = master_df.merge(native_lookup_reset, on=['Species', 'Province'], how='left')

# Manual overrides: explicitly assign Native = 0 (native) or 1 (not native)
manual_overrides = [
    ('populus x ontariensis', ['alberta', 'saskatchewan', 'manitoba', 'ontario', 'quebec'], 1),
    ("crataegus 'vaughn'", ['ontario'], 1),
    ('fraxinus americana', ['ontario', 'quebec', 'new brunswick', 'nova scotia', 'prince edward island'], 1),
    ('amelanchier x lamarckii', ['ontario'], 1),
    ('acer x freemanii', ['ontario'], 1),
]

# Create override DataFrame
manual_rows = []
for species, provinces, native_value in manual_overrides:
    for province in provinces:
        manual_rows.append({
            'Species': species.lower().strip(),
            'Province': province.lower().strip(),
            'Native_Override': native_value
        })

manual_df = pd.DataFrame(manual_rows)

# Merge and override Native values
master_df = master_df.merge(manual_df, on=['Species', 'Province'], how='left')
master_df['Native'] = master_df['Native_Override'].combine_first(master_df['Native'])
master_df = master_df.drop(columns='Native_Override')

# Set any remaining NaN values to 0 (non-native)
master_df['Native'] = master_df['Native'].fillna(0).astype(int)

#endregion

## ---------------------------------------- CALCULATE THE NUMBER OF NATIVE TREES ---------------------------------------
#region

# Confirm only 0s and 1s exist
unique_values = master_df['Native'].unique()
print(f"✅ Unique values in 'Native': {unique_values}\n")

# Group and calculate
native_counts = master_df.groupby('City')['Native'].sum().astype(int)
total_counts = master_df.groupby('City').size()
native_proportions = (native_counts / total_counts * 100).round(2)

# Combine into summary
native_summary = pd.DataFrame({
    'Native Tree Count': native_counts,
    'Proportion Native (%)': native_proportions
}).reset_index()

pd.set_option('display.max_columns', None)
print("🌳 Summary of native trees per city:")
print(native_summary)

# Filter to only the 25 cities with downtown areas
filtered_df = master_df[master_df['City'].str.lower().isin(cities_with_downtown_areas)]

# Group and summarize
summary_native_count = (
    filtered_df
    .groupby(['City', 'City Area'])
    .agg(
        Total_Trees=('Native', 'count'),
        Native_Trees=('Native', lambda x: (x == 1).sum())
    )
    .reset_index()
)

# Add the proportion column (in percent, rounded to 2 decimals)
summary_native_count['Proportion Native (%)'] = (summary_native_count['Native_Trees'] /
                                                 summary_native_count['Total_Trees'] * 100).round(2)

# Sort for neat output
summary_native_count = summary_native_count.sort_values(['City', 'City Area'])

# Display the summary
print("\n🏙️ Summary of native trees per downtown areas:")
print(summary_native_count)

#endregion

## --------------------------------------- CALCULATE DIVERSITY INDICES - NATIONAL --------------------------------------
#region

def compute_diversity_from_counts(counts):
    proportions = counts / counts.sum()
    nonzero = proportions[proportions > 0]
    shannon = round(-np.sum(nonzero * np.log(nonzero)), 2)
    gini_simpson = round(1 - np.sum(proportions ** 2), 2)
    richness = int((counts > 0).sum())
    pielou = round(shannon / np.log(richness), 2) if richness > 1 else 0
    effective_richness = int(round(np.exp(shannon)))

    return shannon, gini_simpson, pielou, richness, effective_richness

diversity_data = []

for level in ['Family', 'Genus', 'Species']:
    grouped = master_df.groupby(['City', level]).size().unstack(fill_value=0)

    for city, counts in grouped.iterrows():
        shannon, gini_simpson, pielou, richness, effective_richness = compute_diversity_from_counts(counts.values)
        diversity_data.append({
            'City': city,
            f'{level} Shannon': shannon,
            f'{level} Gini-Simpson': gini_simpson,
            f'{level} Pielou': pielou,
            f'{level} Richness': richness,
            f'{level} Effective Richness': effective_richness
        })

# Combine into a final DataFrame
diversity_df = pd.DataFrame(diversity_data)

# Pivot to wide format by setting 'City' as index
diversity_wide = diversity_df.groupby('City').agg('first')  # use .agg('mean') to combine if needed

diversity_wide.reset_index(inplace=True)

# Check for missing values
if diversity_wide.isnull().values.any():
    print("\n⚠️ Warning: 'Diversity Indices' contains missing values.")
else:
    print("\n✅ All diversity index columns are complete (no missing values).")

# Save to CSV
diversity_wide.to_csv('Diversity Indices.csv', index=False)
print("💾 Diversity indices saved to 'Diversity Indices.csv'")

#endregion

## -------------------------------------- CALCULATE RELATIVE ABUNDANCE - NATIONAL --------------------------------------
#region

# Create dictionary frame
relative_abundance_nationally = {}

# Total number of trees
total_trees = len(master_df)

# Calculate the relative abundance of the ten most common taxa nationally
for col in ['Species', 'Genus', 'Family']:
    print(f"\n🌳 Summary Table for Top 10 {col} Nationally:")

    # Compute overall top 10
    counts = master_df[col].value_counts().head(10)
    rel_abundance = (counts / total_trees * 100).round(2)
    top_list = counts.index.tolist()

    # Create summary dataframe
    summary = pd.DataFrame({
        col: counts.index,
        'Count': counts.values,
        'Relative Abundance (%)': rel_abundance.values
    })

    # Get top 10 of that taxon per city
    top10_per_city = (
        master_df.groupby(['City', col])
        .size()
        .reset_index(name='City Count')
        .sort_values(['City', 'City Count'], ascending=[True, False])
        .groupby('City')
        .head(10)
    )

    # Count number of cities each top national taxon appears in
    city_counts = (
        top10_per_city[top10_per_city[col].isin(top_list)]
        .groupby(col)['City']
        .nunique()
        .reset_index(name='City Count (n)')
    )

    # Merge with summary and fill missing with 0
    summary = summary.merge(city_counts, on=col, how='left').fillna({'City Count (n)': 0})
    print(summary.to_string(index=False))

    # Save result
    relative_abundance_nationally[col] = summary

#endregion

## -------------------------------- CALCULATE RELATIVE ABUNDANCE - ECOZONE AND CITY SIZE -------------------------------
#region

# Create dictionary frames
relative_abundance_by_ecozone = {}
relative_abundance_by_city_size = {}

# Calculate the relative abundance of the five most common taxa per ecozone
for col in ['Species', 'Genus', 'Family']:
    print(f"\n🌳 Summary Table for Top 5 {col} by Ecozone:")

    total_per_ecozone = master_df.groupby('Ecozone').size().rename('Total')

    abundance = (
        master_df.groupby(['Ecozone', col])
        .size()
        .reset_index(name='Count')
        .merge(total_per_ecozone, on='Ecozone')
    )
    abundance['Relative Abundance (%)'] = (abundance['Count'] / abundance['Total'] * 100).round(2)

    per_city_counts = (
        master_df.groupby(['City', 'Ecozone', col])
        .size()
        .reset_index(name='City Count')
    )

    top5_per_city = (
        per_city_counts
        .sort_values(['City', 'City Count'], ascending=[True, False])
        .groupby('City')
        .head(5)
    )

    top5_counts = (
        top5_per_city
        .groupby(['Ecozone', col])['City']
        .nunique()
        .reset_index(name='Count (n)')
    )

    summary = (
        abundance
        .merge(top5_counts, on=['Ecozone', col], how='left')
        .fillna({'Count (n)': 0})
        .sort_values(['Ecozone', 'Relative Abundance (%)'], ascending=[True, False])
    )

    summary = summary.rename(columns={col: col.capitalize()})
    summary = summary[['Ecozone', col.capitalize(), 'Relative Abundance (%)', 'Count (n)']]

    relative_abundance_by_ecozone[col] = summary

    # Group preview
    print(summary.groupby('Ecozone').head(5))  # top 5 per ecozone

# Calculate the relative abundance of the five most common taxa per city size
for col in ['Species', 'Genus', 'Family']:
    print(f"\n🌳 Summary Table for Top 5 {col} by City Size:")

    total_per_city_size = master_df.groupby('City Size').size().rename('Total')

    abundance = (
        master_df.groupby(['City Size', col])
        .size()
        .reset_index(name='Count')
        .merge(total_per_city_size, on='City Size')
    )
    abundance['Relative Abundance (%)'] = (abundance['Count'] / abundance['Total'] * 100).round(2)

    per_city_counts = (
        master_df.groupby(['City', 'City Size', col])
        .size()
        .reset_index(name='City Count')
    )

    top5_per_city = (
        per_city_counts
        .sort_values(['City', 'City Count'], ascending=[True, False])
        .groupby('City')
        .head(5)
    )

    top5_counts = (
        top5_per_city
        .groupby(['City Size', col])['City']
        .nunique()
        .reset_index(name='Count (n)')
    )

    summary = (
        abundance
        .merge(top5_counts, on=['City Size', col], how='left')
        .fillna({'Count (n)': 0})
        .sort_values(['City Size', 'Relative Abundance (%)'], ascending=[True, False])
    )

    summary = summary.rename(columns={col: col.capitalize()})
    summary = summary[['City Size', col.capitalize(), 'Relative Abundance (%)', 'Count (n)']]

    relative_abundance_by_city_size[col] = summary

    # Group preview
    print(summary.groupby('City Size').head(5))  # top 5 per city size

#endregion

## -------------------------------------- CALCULATE RELATIVE ABUNDANCE - PER CITY --------------------------------------
#region

# Filter just the cities with downtown areas
all_df = master_df.copy()

# Container for results
top_taxa_by_city = {}

for col in ['Species', 'Genus', 'Family']:
    print(f"\n🌆 Top {col.lower()} by city:")

    # Group by city + city area + taxon and count
    group_counts = (
        all_df
        .groupby(['City', col])
        .size()
        .reset_index(name='Count')
    )

    # Total trees per city-area to calculate relative abundance
    totals = (
        all_df
        .groupby(['City'])
        .size()
        .reset_index(name='Total')
    )

    # Merge and calculate relative abundance
    merged = group_counts.merge(totals, on=['City'])
    merged['Relative Abundance (%)'] = (merged['Count'] / merged['Total'] * 100).round(2)

    # Get top 1 taxon per City + City Area
    top1 = (
        merged
        .sort_values(['City', 'Relative Abundance (%)'], ascending=[True, False])
        .groupby(['City'])
        .head(1)
    )

    # Rename columns for clarity
    top1 = top1.rename(columns={col: col.capitalize()})
    top1 = top1[['City', col.capitalize(), 'Count', 'Relative Abundance (%)']]

    print(top1.head(50))
    top_taxa_by_city[col] = top1

#endregion

## --------------------------- CALCULATE RELATIVE ABUNDANCE - DOWNTOWN AND NON-DOWNTOWN AREAS --------------------------
#region

# Filter just the cities with downtown areas
downtown_df = master_df[master_df['City'].str.lower().isin(cities_with_downtown_areas)]

# Container for results
top_taxa_by_area = {}

for col in ['Species', 'Genus', 'Family']:
    print(f"\n🌆 Top {col} in Downtown vs Not Downtown areas:")

    # Group by city + city area + taxon and count
    group_counts = (
        downtown_df
        .groupby(['City', 'City Area', col])
        .size()
        .reset_index(name='Count')
    )

    # Total trees per city-area to calculate relative abundance
    totals = (
        downtown_df
        .groupby(['City', 'City Area'])
        .size()
        .reset_index(name='Total')
    )

    # Merge and calculate relative abundance
    merged = group_counts.merge(totals, on=['City', 'City Area'])
    merged['Relative Abundance (%)'] = (merged['Count'] / merged['Total'] * 100).round(2)

    # Get top 1 taxon per City + City Area
    top1 = (
        merged
        .sort_values(['City', 'City Area', 'Relative Abundance (%)'], ascending=[True, True, False])
        .groupby(['City', 'City Area'])
        .head(1)
    )

    # Rename columns for clarity
    top1 = top1.rename(columns={col: col.capitalize()})
    top1 = top1[['City', 'City Area', col.capitalize(), 'Count', 'Relative Abundance (%)']]

    print(top1.head(50))
    top_taxa_by_area[col] = top1

#endregion

## --------------------------- CALCULATE DIVERSITY INDICES - DOWNTOWN AND NON-DOWNTOWN AREAS ---------------------------
#region

# Standardize casing
master_df['City'] = master_df['City'].str.strip().str.lower()
master_df['City Area'] = master_df['City Area'].str.strip()

# Filter for cities with both Downtown and Not Downtown
df_filtered = master_df[master_df['City'].isin(cities_with_downtown_areas)]

# Initialize list to store results
downtown_diversity_data = []

for level in ['Family', 'Genus', 'Species']:
    grouped = df_filtered.groupby(['City', 'City Area', level]).size().unstack(fill_value=0)

    for (city, area), counts in grouped.iterrows():
        shannon, gini_simpson, pielou, richness, effective_richness = compute_diversity_from_counts(counts.values)
        downtown_diversity_data.append({
            'City': city,
            'City Area': area,
            f'{level} Shannon': shannon,
            f'{level} Gini-Simpson': gini_simpson,
            f'{level} Pielou': pielou,
            f'{level} Richness': richness,
            f'{level} Effective Richness': effective_richness
        })

# Combine into final DataFrame
downtown_diversity_df = pd.DataFrame(downtown_diversity_data)

# Reshape for easy viewing
downtown_diversity_wide = downtown_diversity_df.sort_values(['City', 'City Area']).reset_index(drop=True)

# Check for missing values
if downtown_diversity_wide.isnull().values.any():
    print("\n⚠️ Warning: 'Diversity Indices' contains missing values.")
else:
    print("\n✅ All diversity index columns are complete (no missing values).")

# Save results
downtown_diversity_wide.to_csv('Diversity Indices by Downtown Status.csv', index=False)
print("💾 Diversity indices saved to 'Diversity Indices by Downtown Status.csv'")

#endregion

print(f"🔢 Number of rows in master_df: {len(master_df):,}")

## --------------------------------------------------- FIGURE SET-UP ---------------------------------------------------
#region

# Custom ecozone color palette
ecozone_colors = {
    'Pacific Maritime': '#009DAE',    # Deep Forest Green
    'Montane Cordillera': '#A9A9A9', # Mountain Gray
    'Prairie': '#DFAF2C',            # Amber / Tawny Brown
    'Mixedwood Plain': '#D02E2E',    # Light Maple Red
    'Atlantic Maritime': '#0F52BA'   # Sapphire Blue
}

#endregion

## -------------------------------------------- FIGURE 3: RELATIVE ABUNDANCE -------------------------------------------
#region

# Capitalize first letter of Genus and Species
for level in ['Species', 'Genus']:
    relative_abundance_nationally[level][level] = relative_abundance_nationally[level][level].str.capitalize()

# Combine national top 10 summaries into a single dataframe
plot_data = []
for level in ['Species', 'Genus', 'Family']:  # Order: Species → Genus → Family
    df = relative_abundance_nationally[level]
    for _, row in df.iterrows():
        plot_data.append({
            "Taxon": row[level],
            "Mean (%)": row["Relative Abundance (%)"],
            "Cities (N)": int(row["City Count (n)"]),
            "Taxonomic Level": level
        })

plot_df = pd.DataFrame(plot_data)

# Define custom colors
levels_colors = {'Species': '#009DAE', 'Genus': '#DFAF2C', 'Family': '#D02E2E'}

# Assign sort order to ensure Species is on top
tax_level_order = {'Species': 0, 'Genus': 1, 'Family': 2}
plot_df['Level Rank'] = plot_df['Taxonomic Level'].map(tax_level_order)

# Sort by Level Rank and Mean (%)
plot_df = plot_df.sort_values(by=["Level Rank", "Mean (%)"], ascending=[True, False]).reset_index(drop=True)

# Begin plotting
fig, ax = plt.subplots(figsize=(10, 8))
plt.subplots_adjust(left=0.3)

# Draw bars and text
for i, (taxon, value, cities, level) in enumerate(zip(
    plot_df["Taxon"], plot_df["Mean (%)"], plot_df["Cities (N)"], plot_df["Taxonomic Level"]
)):
    ax.barh(i, value, color=levels_colors.get(level, "skyblue"))
    ax.text(value + 0.5, i, f"{value:.1f}% ({cities})", va='center', color="black", fontsize=12)

# Y-tick labels
ax.set_yticks(range(len(plot_df)))
ax.set_yticklabels(plot_df["Taxon"], fontsize=13)
ax.set_ylim(-0.8, len(plot_df) - 0.2)

# Italicize species and genus labels
for i, level in enumerate(plot_df["Taxonomic Level"]):
    ax.get_yticklabels()[i].set_fontstyle('italic' if level in ['Species', 'Genus'] else 'normal')

# Secondary y-axis with taxonomic group labels
midpoints = []
labels = []
for level in ['Species', 'Genus', 'Family']:
    indices = plot_df[plot_df['Taxonomic Level'] == level].index
    if not indices.empty:
        midpoint = (min(indices) + max(indices)) / 2
        midpoints.append(midpoint)
        labels.append(level)

secax = ax.secondary_yaxis('right')
secax.set_yticks(midpoints)
secax.set_yticklabels(labels, rotation=-90, ha='center', va='center', fontweight='bold', fontsize=13)
secax.tick_params(length=0, pad=7)

# Final formatting
ax.set_xlim(0, 30)
ax.set_xlabel("Proportion of Street Tree Population (%)", fontweight='bold', fontsize=14)
ax.tick_params(axis='x', labelsize=12)
ax.invert_yaxis()

# Save and display
plt.tight_layout()
plt.savefig("Figure 3 - Relative Abundance.png", dpi=900, bbox_inches='tight')
plt.show()

#endregion

## ---------------------------------------- FIGURE 4: DIVERSITY INDICES RESULTS ----------------------------------------
#region

# Merge diversity data with location data
figure_3_df = pd.merge(diversity_wide, location_index_df, on='City', how='left')

# Keep only the relevant columns
columns_to_keep = [
    'City',
    'Species Shannon',
    'Species Gini-Simpson',
    'Species Pielou',
    'Species Richness',
    'Species Effective Richness',
    'Ecozone',
    'Longitude'
]
figure_3_df = figure_3_df[columns_to_keep]

# Sort by longitude for geographic left-to-right order
figure_3_df = figure_3_df.sort_values('Longitude')

# Capitalize city names
figure_3_df['City'] = figure_3_df['City'].str.title()

# Set city order for plotting
figure_3_df['City'] = pd.Categorical(figure_3_df['City'], categories=figure_3_df['City'][::-1], ordered=True)

# Map ecozone colors
figure_3_df['Color'] = figure_3_df['Ecozone'].map(ecozone_colors)

fig, ax1 = plt.subplots(figsize=(14, 6))
ax2 = ax1.twinx()

# Bar plot for Species Richness
bars = ax2.bar(figure_3_df['City'], figure_3_df['Species Richness'], color=figure_3_df['Color'], alpha=0.6)
ax2.set_ylabel('Species Richness', fontweight='bold', fontsize=14)

# Add Effective Richness labels above bars
offsets = {'Welland': 10, 'Regina': 19}
for bar, city, eff_rich in zip(bars, figure_3_df['City'], figure_3_df['Species Effective Richness']):
    y_offset = offsets.get(city, 1)
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + y_offset,
             f'({int(round(eff_rich))})', ha='center', va='bottom', fontsize=11)

# Diversity index scatter plots
ax1.scatter(figure_3_df['City'], figure_3_df['Species Shannon'],
            label='Shannon-Wiener Diversity Index', s=64)
ax1.scatter(figure_3_df['City'], figure_3_df['Species Gini-Simpson'],
            label='Gini-Simpson Diversity Index', color='black', marker='s', s=64)
ax1.scatter(figure_3_df['City'], figure_3_df['Species Pielou'],
            label='Pielou’s Index of Evenness', color='green', marker='D', s=64)

ax1.set_ylabel('Diversity and Evenness Indices', fontweight='bold', fontsize=14)
ax1.yaxis.set_major_locator(MultipleLocator(0.5))

# Plot layering
ax1.set_zorder(2)
ax2.set_zorder(1)
ax1.patch.set_visible(False)

# Axis formatting
ax1.tick_params(axis='x', labelsize=12)
ax1.tick_params(axis='y', labelsize=12)
ax2.tick_params(axis='y', labelsize=12)

ax1.set_xticks(range(len(figure_3_df['City'])))
ax1.set_xticklabels(figure_3_df['City'], rotation=90, fontsize=12)
ax1.set_xlim(len(figure_3_df['City']) - 0.5, -0.5)
ax1.set_xlabel('')
ax2.set_ylim(0, 395)

plt.tight_layout()
fig.savefig("Figure 4.png", dpi=900, bbox_inches='tight')
plt.show()

#endregion

## ---------------------------------------- FIGURE 5: NATIVE SPECIES DOMINANCE -----------------------------------------
#region

# Merge Ecozone and Longitude info directly from master_df
city_info = master_df[['City', 'Ecozone', 'Longitude']].drop_duplicates()
native_summary = native_summary.merge(city_info, on='City', how='left')

# Capitalize City names
native_summary['City'] = native_summary['City'].str.title()

# Sort by Longitude (west to east)
native_summary = native_summary.sort_values('Longitude', ascending=False)

# Set figure size
plt.figure(figsize=(10, 8))

# Plot bar chart
barplot = sns.barplot(
    data=native_summary,
    x='Proportion Native (%)',
    y='City',
    hue='Ecozone',
    dodge=False,
    palette=ecozone_colors,
    order=native_summary['City'],
    errorbar=None
)

plt.xlim(0, 100)
plt.ylim(len(native_summary) - 0.2, -0.8)

# Add text labels to bars
for container in barplot.containers:
    for bar in container:
        width = bar.get_width()
        if width > 0:
            plt.text(
                width + 1,
                bar.get_y() + bar.get_height() / 2,
                f'{width:.1f}%',
                ha='left',
                va='center',
                fontsize=10
            )

# Format axes and legend
plt.xlabel('Proportion of Street Trees that are Native Species (%)', fontsize=14, fontweight='bold')
plt.gca().set_ylabel(None)
plt.xticks(fontsize=14)
plt.yticks(fontsize=12)
plt.legend(
    title='Ecozone',
    loc='upper right',
    bbox_to_anchor=(0.995, 0.995),
    frameon=True,
    fontsize=11,
    title_fontsize=12
)

# Save and show
plt.tight_layout()
plt.savefig("Figure 5 - Native Species Dominance.png", dpi=900, bbox_inches='tight')
plt.show()

#endregion
