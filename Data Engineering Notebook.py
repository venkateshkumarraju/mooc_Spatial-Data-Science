import arcgis
import pandas
import os
import arcpy

import pandas as pd

##### Import needed modules

# The CSV file is in the same folder as this notebook; providing the CSV file name is sufficient.
elections_data_path = "E:\mooc\Spatial Data Science\DataEngineering_and_Visualization\countypres_2000-2020.csv"
elections_complete_df=pd.read_csv(elections_data_path, dtype={"county_fips":object})
elections_complete_df

rename_cols = {
    "state_po": "state_abbr",
    "county_fips": "FIPS",
    "party": "pol_identity"
}
elections_complete_df.rename(columns=rename_cols, inplace=True)
elections_complete_df.head()

# Find records with null values.
elections_complete_df.query("FIPS.isnull()")

# Add the FIPS code for Washington, DC.
elections_complete_df.loc[elections_complete_df['state_abbr'] == 'DC', 'FIPS'] = '11001'

# Determine the total number of records in the table.
rowcount = elections_complete_df.shape[0]

# Determine the total number of votes cast across all counties in all elections.
votecount = elections_complete_df["candidatevotes"].sum()

# Determine how many votes are associated with null FIPS values.
null_fips_df = elections_complete_df.query("FIPS.isnull()")
null_fips_rowcount = null_fips_df.shape[0]
null_fips_votecount = null_fips_df["candidatevotes"].sum()

# Calculate the percentage of the data affected by null records.
percentage_null_fips_rows = round((null_fips_rowcount / rowcount) * 100, 3)
percentage_null_fips_votes = round((null_fips_votecount / votecount) * 100, 3)

# Use a print statement to report this information.
print(f"There were {null_fips_rowcount} records with null FIPS values in the data, representing {null_fips_votecount} votes.")
print(f"This amounts to {percentage_null_fips_rows}% of the total records and {percentage_null_fips_votes}% of the total votes.")

# Remove records with null FIPS values.
elections_df = elections_complete_df.query("FIPS.notnull()")

# Filter records by political identity for Democrats and Republicans.
elections_df = elections_df.query("pol_identity in ['DEMOCRAT', 'REPUBLICAN']")

# Group records for each candidate by county and year.
candidate_group = elections_df.groupby(['FIPS', 'county_name', 'state', 'candidate', 'year', 'pol_identity', 'totalvotes'])

# Sum each candidate's votes in each county, for each election year.
candidate_votes = candidate_group['candidatevotes'].sum()

# Remove the multi-index created by the group-by and sum, returning a new data frame.
candidate_votes_df = candidate_votes.reset_index()

# Check the output.
candidate_votes_df.head()

# Pivot the data frame.
# The index "locks" the fields that remain the same for each county.
# The columns determine which values will become new fields; in this case, the two values in the pol_identity column become two new fields.
# The values column determines which values will be reported for each of the new fields.
elections_pivot_df = candidate_votes_df.pivot(
    index=['year', 'FIPS', 'county_name', 'state', 'totalvotes'], 
    columns=['pol_identity'], 
    values=['candidatevotes']
)

# Check the output.
elections_pivot_df.head()

# Remove the multi-index since you no longer need these fields to be "locked" for the pivot.
elections_pivot_df.columns = elections_pivot_df.columns.get_level_values(1).rename(None)
elections_pivot_df.reset_index(inplace=True)

# Rename columns to better reflect their new meaning.
elections_pivot_df.rename(columns={"DEMOCRAT": "votes_dem","REPUBLICAN": "votes_gop"}, inplace=True)

# Check the output.
elections_pivot_df.head()

# Create a dictionary of CSV files for each election year.
cvap_paths = {
    2020: "E:\mooc\Spatial Data Science\DataEngineering_and_Visualization\CountyCVAP_2017-2021.csv",
    2016: "E:\mooc\Spatial Data Science\DataEngineering_and_Visualization\CountyCVAP_2014-2018.csv",
    2012: "E:\mooc\Spatial Data Science\DataEngineering_and_Visualization\CountyCVAP_2010-2014.csv",
    2008: "E:\mooc\Spatial Data Science\DataEngineering_and_Visualization\CountyCVAP_2006-2010.csv"
}

# Create a dictionary of data frames for each election year.
cvap_dfs = {year:pd.read_csv(path, encoding="latin-1") for year, path in cvap_paths.items()}

# Display the first three rows of each data frame to check the output.
for df in cvap_dfs.values():
    display(df.head(3))

# Create a new dictionary to hold the processed dataframes.
cvap_processed_dfs = {}

for year, df in cvap_dfs.items():
    
    # Set column formatting to lowercase.
    df.columns = df.columns.str.lower()
    
    # Add a year value.
    df['year'] = year
    
    # Include only rows of total counts.
    df = df.query("lntitle == 'Total'")

    # Include only necessary columns.
    df = df[['year', 'geoid','geoname','cvap_est']]

    # Extract FIPS from the geoid value.
    df['FIPS'] = df['geoid'].str[-5:]
    
    # Add processed data frames to the dictionary.
    cvap_processed_dfs[year] = df
    
# Concatenate the data frames.
cvap_df = pd.concat(cvap_processed_dfs.values())

# Check the output.
cvap_df

voting_info_df = pd.merge(elections_pivot_df, cvap_df, left_on=['FIPS', 'year'], right_on=['FIPS', 'year'], how='left')

# Check the output.
voting_info_df

voting_info_df = voting_info_df.query("year >= 2008")

# Check the output.
voting_info_df

voting_info_df.query("cvap_est.isnull()")

voting_info_df = voting_info_df.query("state != 'ALASKA'")

voting_info_df.query("cvap_est.isnull()")

voting_info_df.query("FIPS in ['29095', '36000', '51019', '51515']")

# Because the counties to fix are in different states, each group of a county and its associated city can be defined by year and state.
county_groups = voting_info_df.query("FIPS in ['29095', '36000', '51019', '51515']").groupby(['year', 'state'])
summed_votes = county_groups.sum()

# Match index values for summed data with original data.
summed_votes.index = voting_info_df.query("FIPS in ['29095', '51019']").sort_values('year').index

# Check the output.
summed_votes

# Update county records with new summed values.
voting_info_df.loc[summed_votes.index, summed_votes.columns] = summed_votes

# Eliminate Kansas City and Bedford records.
voting_info_df = voting_info_df.query("FIPS not in ['36000', '51515']")

# Check the output.
voting_info_df.query("FIPS in ['29095', '36000', '51019', '51515']")

# Verify that no record in the output data frame has a null cvap_est value.
voting_info_df.query("cvap_est.isnull()")

# Calculate voters who did not choose the Democratic or Republican party.
voting_info_df['votes_other'] = voting_info_df['totalvotes'] - (voting_info_df['votes_dem'] + voting_info_df['votes_gop'])

# Calculate voter share attributes.
voting_info_df['voter_share_major_party'] = (voting_info_df['votes_dem'] + voting_info_df['votes_gop']) / voting_info_df['totalvotes']
voting_info_df['voter_share_dem'] = voting_info_df['votes_dem'] / voting_info_df['totalvotes']
voting_info_df['voter_share_gop'] = voting_info_df['votes_gop'] / voting_info_df['totalvotes']
voting_info_df['voter_share_other'] = voting_info_df['votes_other'] / voting_info_df['totalvotes']

# Calculate raw difference attributes.
voting_info_df['rawdiff_dem_vs_gop'] = voting_info_df['votes_dem'] - voting_info_df['votes_gop']
voting_info_df['rawdiff_gop_vs_dem'] = voting_info_df['votes_gop'] - voting_info_df['votes_dem']
voting_info_df['rawdiff_dem_vs_other'] = voting_info_df['votes_dem'] - voting_info_df['votes_other']
voting_info_df['rawdiff_gop_vs_other'] = voting_info_df['votes_gop'] - voting_info_df['votes_other']
voting_info_df['rawdiff_other_vs_dem'] = voting_info_df['votes_other'] - voting_info_df['votes_dem']
voting_info_df['rawdiff_other_vs_gop'] = voting_info_df['votes_other'] - voting_info_df['votes_gop']

# Calculate percentage difference attributes.
voting_info_df['pctdiff_dem_vs_gop'] = (voting_info_df['votes_dem'] - voting_info_df['votes_gop']) / voting_info_df['totalvotes']
voting_info_df['pctdiff_gop_vs_dem'] = (voting_info_df['votes_gop'] - voting_info_df['votes_dem']) / voting_info_df['totalvotes']
voting_info_df['pctdiff_dem_vs_other'] = (voting_info_df['votes_dem'] - voting_info_df['votes_other']) / voting_info_df['totalvotes']
voting_info_df['pctdiff_gop_vs_other'] = (voting_info_df['votes_gop'] - voting_info_df['votes_other']) / voting_info_df['totalvotes']
voting_info_df['pctdiff_other_vs_dem'] = (voting_info_df['votes_other'] - voting_info_df['votes_dem']) / voting_info_df['totalvotes']
voting_info_df['pctdiff_other_vs_gop'] = (voting_info_df['votes_other'] - voting_info_df['votes_gop']) / voting_info_df['totalvotes']

# Calculate voter turnout attributes.
voting_info_df['voter_turnout'] = voting_info_df['totalvotes'] / voting_info_df['cvap_est']
voting_info_df['voter_turnout_majparty'] = (voting_info_df['votes_dem']+voting_info_df['votes_gop']) / voting_info_df['cvap_est']
voting_info_df['voter_turnout_dem'] = voting_info_df['votes_dem'] / voting_info_df['cvap_est']
voting_info_df['voter_turnout_gop'] = voting_info_df['votes_gop'] / voting_info_df['cvap_est']
voting_info_df['voter_turnout_other'] = voting_info_df['votes_other'] / voting_info_df['cvap_est']

# Determine the winning political party.
def return_winning_party(total_votes_dem, total_votes_gop, total_votes_other):
    if total_votes_dem > total_votes_gop and total_votes_dem > total_votes_other:
        return "Democratic Party"
    elif total_votes_gop > total_votes_dem and total_votes_gop > total_votes_other:
        return "Republican Party"
    elif total_votes_other > total_votes_dem and total_votes_other > total_votes_gop:
        return "Other Party"
    
voting_info_df["Winning Party"] = voting_info_df.apply(
    lambda x: return_winning_party(
        x.votes_dem, 
        x.votes_gop,
        x.votes_other
    ), axis=1)

# Check the output.
voting_info_df.head()

voting_info_df['voter_turnout'].describe()

# Perform query for voter turnout above 100%.
turnout_over_1_df = voting_info_df.query('voter_turnout > 1')[['FIPS','county_name','state','year','voter_turnout','totalvotes','cvap_est']].sort_values(['FIPS', 'year'])
turnout_over_1_df

# Adjust values.
voting_info_df.loc[voting_info_df['voter_turnout'] > 1, 'voter_turnout'] = 1

# Describe data distribution.
voting_info_df['voter_turnout'].describe()

voting_info_pivot_df = voting_info_df.pivot(
    index=['FIPS'], 
    columns=['year'], 
    values=['totalvotes', 'cvap_est', 'voter_turnout', 'voter_turnout_dem', 'voter_turnout_gop', 'pctdiff_dem_vs_gop', 'rawdiff_dem_vs_gop', 'Winning Party'])

voting_info_pivot_df

voting_info_pivot_df[voting_info_pivot_df.isna().any(axis=1)]

# Make a copy of the data frame.
voting_info_fix_df = voting_info_df.copy()

# Fix the FIPS code for Oglala Lakota County.
voting_info_fix_df.loc[voting_info_fix_df['FIPS'] == '46113', 'FIPS'] = '46102'

# Pivot the table so that there is a single entry for each county.
voting_info_pivot_df = voting_info_fix_df.pivot(
    index=['FIPS'], 
    columns=['year'], 
    values=['totalvotes', 'cvap_est', 'voter_turnout', 'voter_turnout_dem', 'voter_turnout_gop', 'pctdiff_dem_vs_gop', 'rawdiff_dem_vs_gop', 'Winning Party'])

# Check the output to ensure there are no null values in any field.
voting_info_pivot_df[voting_info_pivot_df.isna().any(axis=1)]

voting_info_pivot_df.columns = [f"{a}_{b}" for a, b in voting_info_pivot_df.columns]
voting_info_pivot_df = voting_info_pivot_df.reset_index()
voting_info_pivot_df

# The relevant layer is available to the public, so you can connect to ArcGIS Online anonymously.
gis = arcgis.gis.GIS()

# Get the USA Census counties layer from ArcGIS Living Atlas. 
item = gis.content.get('14c5450526a8430298b2fa74da12c2f4')

# Convert the layer to a spatially enabled data frame.
counties_sdf = pd.DataFrame.spatial.from_layer(item.layers[0])
counties_sdf.head()

# Join the voting information dataframe with the counties geometry.
geo_sdf = pd.merge(counties_sdf, voting_info_pivot_df, left_on='FIPS', right_on='FIPS', how='right')
geo_sdf.head()

geo_sdf.query("SHAPE.isnull()")

# Create variables that represent the ArcGIS Pro project and map.
aprx = arcpy.mp.ArcGISProject("CURRENT")
m = aprx.listMaps('Data Engineering')[0]

# Create a variable that represents the default file geodatabase.
arcpy.env.workspace = aprx.defaultGeodatabase
arcpy.env.addOutputsToMap = False

arcpy.env.workspace

# Create a feature class for the 2020 presidential election.
out_fc_name = "county_elections_pres"
out_fc_path = os.path.join(arcpy.env.workspace, out_fc_name)
out_fc = geo_sdf.spatial.to_featureclass(out_fc_path)
out_fc
