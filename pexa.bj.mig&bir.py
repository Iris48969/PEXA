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
    SELECT [ASGS_2016] as ASGSCode,
           [ERPYear] as Year,
           SUM(Number) as Number
    FROM [forecasts].[dbo].[ERP]
    WHERE LEN([ASGSCode]) = 8
    GROUP BY [ASGSCode], [ERPYear]
    ORDER BY [ASGSCode], [ERPYear]
""")

# Fetch all data
pop_data = cursor.fetchall()

# Convert to DataFrame directly
pop_df = pd.DataFrame([tuple(row) for row in pop_data], columns=['ASGSCode', 'Year', 'Number'])
pop_df['Population_Change'] = pop_df.groupby('ASGSCode')['Number'].diff()
print(pop_df.head(10))

# Migration data
cursor.execute("""  
  SELECT [ASGSCode],
         [MigrationType],
         [Year],
         SUM([Number]) as Number 
  FROM [forecasts].[dbo].[Migration]
  WHERE LEN([ASGSCode]) = 8
  GROUP BY [ASGSCode],[MigrationType],[Year]
  ORDER BY [ASGSCode],[Year]
""")

# Fetch all migration data
mig_data = cursor.fetchall()
mig_df = pd.DataFrame([tuple(row) for row in mig_data], columns=['ASGSCode', 'MigrationType','Year', 'Number'])
print(mig_df.head(10))

arrivals = mig_df[mig_df['MigrationType'].str.contains('Arrival')]
departures = mig_df[mig_df['MigrationType'] == 'Departure']

# Group by ASGSCode and Year, and sum the Number column
arrivals_sum = arrivals.groupby(['ASGSCode', 'Year'])['Number'].sum().reset_index()
departures_sum = departures.groupby(['ASGSCode', 'Year'])['Number'].sum().reset_index()

# Merge the two DataFrames on ASGSCode and Year
net_migration = pd.merge(arrivals_sum, departures_sum, on=['ASGSCode', 'Year'], how='outer', suffixes=('_Arrival', '_Departure'))

net_migration['Net_Migration'] = net_migration['Number_Arrival'] - net_migration['Number_Departure']

# Display the resulting DataFrame
print(net_migration.head(10))

# Birth and Death data
cursor.execute("""  
  SELECT [ASGSCode],
         [Year],
         SUM(Number) as Birth_Number
  FROM [forecasts].[dbo].[Births]
  WHERE LEN([ASGSCode]) = 8
  GROUP BY [ASGSCode],[Year]
  ORDER BY [ASGSCode],[Year]
""")

# Fetch all birth data
birth_data = cursor.fetchall()
birth_df = pd.DataFrame([tuple(row) for row in birth_data], columns=['ASGSCode','Year', 'Birth_Number'])
print(birth_df.head(10))

cursor.execute("""  
  SELECT [ASGSCode],
         [Year],
         SUM([Number]) as Death_Number
  FROM [forecasts].[dbo].[Deaths]
  WHERE LEN([ASGSCode]) = 8
  GROUP BY [ASGSCode],[Year]
  ORDER BY [ASGSCode],[Year]
""")
# Fetch all death data
death_data = cursor.fetchall()
death_df = pd.DataFrame([tuple(row) for row in death_data], columns=['ASGSCode','Year', 'Death_Number'])
print(death_df.head(10))

# Merge birth and death data on ASGSCode and Year
birth_death_df = pd.merge(birth_df, death_df, on=['ASGSCode', 'Year'], how='outer')

# Calculate the net births (Births - Deaths)
birth_death_df['Net_Births'] = birth_death_df['Birth_Number'] - birth_death_df['Death_Number']

# Display the resulting DataFrame
print(birth_death_df.head(10))


final_df = pd.merge(
    pop_df[['ASGSCode', 'Year', 'Population_Change']],
    net_migration[['ASGSCode', 'Year', 'Net_Migration']], 
    on=['ASGSCode', 'Year'], 
    how='outer'
)

# Now merge with the birth and death data
final_df = pd.merge(
    final_df,
    birth_death_df[['ASGSCode', 'Year', 'Net_Births']], 
    on=['ASGSCode', 'Year'], 
    how='outer'
)

# Calculate the expected population change
final_df['Expected_Population'] = final_df['Net_Migration'] + final_df['Net_Births']
final_df = final_df.dropna(subset=['Population_Change', 'Net_Migration', 'Net_Births', 'Expected_Population'])

# Calculate the difference between the actual number and the expected population
final_df['Difference'] = final_df['Population_Change'] - final_df['Expected_Population']

# Display the resulting DataFrame
print(final_df.head(10))

# Optionally, filter and display only the rows where there's a difference
differences_df = final_df[final_df['Difference'] != 0]
print(differences_df)

Q1 = final_df['Difference'].quantile(0.25)
Q3 = final_df['Difference'].quantile(0.75)
IQR = Q3 - Q1

# Define outliers as values outside 1.5 times the IQR
outlier_threshold_lower = Q1 - 1.5 * IQR
outlier_threshold_upper = Q3 + 1.5 * IQR

# Filter out the outliers
outliers = final_df[(final_df['Difference'] < outlier_threshold_lower) | (final_df['Difference'] > outlier_threshold_upper)]

outlier_asgs_codes = outliers['ASGSCode'].unique()

# Display the unique ASGSCode values that are outliers
print("Outlier ASGSCode values:")
print(outlier_asgs_codes)
print[len(outlier_asgs_codes)]

# Calculate the number of unique outlier ASGSCode values
num_outlier_asgs_codes = len(outlier_asgs_codes)

# Print the number of unique outlier ASGSCode values
print("Number of unique outlier ASGSCode values:")
print(num_outlier_asgs_codes)