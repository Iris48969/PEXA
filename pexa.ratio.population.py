import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Connect to database
connection = pyodbc.connect(
    'Driver={SQL Server};'
    'Server=localhost;'
    'Database=forecasts;'
    'UID=sa;'
    'PWD=MBS_project_2024'
)

# Create cursor
cursor = connection.cursor()

# Execute the SQL query for population data
cursor.execute("""
    SELECT 
        [ASGS_2016] as ASGSCode,
        [ERPYear] as Year, 
        SUM([Number]) as Population 
    FROM 
        [forecasts].[dbo].[ERP] 
    GROUP BY 
        ASGS_2016, ERPYear 
    ORDER BY 
        ASGS_2016
""")

# Fetch all data
pop_data = cursor.fetchall()
# Convert to DataFrame directly
pop_df = pd.DataFrame([tuple(row) for row in pop_data], columns=['ASGSCode', 'Year', 'Pop_Number'])
print(pop_df.head(10))

cursor.execute("""
    SELECT 
        [ASGSCode],
        [ERPYear]as Year,
        SUM([Number]) as HouseholdCount 
    FROM 
        [forecasts].[dbo].[Households]
    WHERE 
        HhKey = 19 
    GROUP BY 
        ASGSCode, HhKey, ERPYear 
    ORDER BY 
        ASGSCode
""")
household_data = cursor.fetchall()
# Convert to DataFrame directly
household_df = pd.DataFrame([tuple(row) for row in household_data], columns=['ASGSCode', 'Year', 'Household_Number'])
print(household_df.head(10))

# Merge the two DataFrames on ASGSCode and Year
merged_df = pd.merge(pop_df, household_df, on=['ASGSCode', 'Year'], how='inner')
print(merged_df[:5])

# Calculate the ratio of Population to HouseholdCount
merged_df['Ratio'] = merged_df['Pop_Number'] / merged_df['Household_Number']

# Filter the data where the ratio is greater than 5 or less than 1
outliers_df = merged_df[(merged_df['Ratio'] > 5) | (merged_df['Ratio'] < 1)]

# Print the ASGSCode and Year that need to be checked
print("ASGSCode and Year that need to be checked (Ratio > 5 or Ratio < 1):")
print(outliers_df[['ASGSCode', 'Year', 'Ratio']])

unique_outlier_asgs_codes = outliers_df['ASGSCode'].unique()
number_ASGSCode = len(unique_outlier_asgs_codes)
# Print the unique ASGSCode values that need to be checked
print("Unique ASGSCode values that need to be checked (Ratio > 5 or Ratio < 1):")
print(unique_outlier_asgs_codes)
print(number_ASGSCode)

##########################################################################
import matplotlib.pyplot as plt

# Filter the data for the specific region
region_code = 41001110
region_df = merged_df[merged_df['ASGSCode'] == region_code]

# Ensure the Year column is an integer and sort by Year
region_df['Year'] = region_df['Year'].astype(int)
region_df = region_df.sort_values(by='Year', ascending=True)

# Print the data for verification
print(region_df[['Year', 'Ratio']])

# Plot the data with enhanced aesthetics
plt.figure(figsize=(12, 6))
plt.plot(region_df['Year'], region_df['Ratio'], marker='o', linestyle='-', color='#1f77b4', linewidth=2, markersize=8, markerfacecolor='orange', markeredgewidth=2)

# Add horizontal lines at y=1 and y=5
plt.axhline(y=1, color='red', linestyle='--', linewidth=2, label='Check Threshold = 1')
plt.axhline(y=5, color='green', linestyle='--', linewidth=2, label='Check Threshold = 5')

# Customize the title and labels
plt.title(f'Population to Household Ratio for Region {region_code}', fontsize=16, fontweight='bold')
plt.xlabel('Year', fontsize=14)
plt.ylabel('Ratio', fontsize=14)

# Customize the grid
plt.grid(True, linestyle='--', alpha=0.7)

# Set x-ticks to show every year, and ensure they are labeled correctly
plt.xticks(region_df['Year'], rotation=45, fontsize=12)

# Customize the y-ticks
plt.yticks(fontsize=12)

# Add a legend
plt.legend(loc='best', fontsize=12)

# Adjust the layout to prevent clipping of tick-labels
plt.tight_layout()

# Show the plot
plt.show()