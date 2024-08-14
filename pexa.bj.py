
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to database
connection = pyodbc.connect('Driver={SQL Server};'
                            'Server=localhost;'
                            'Database=forecasts;'
                            'UID=sa;'
                            'PWD=MBS_project_2024')


# Create cursor
cursor = connection.cursor()

'''
# Query database Example
cursor.execute("SELECT ASGSCode FROM AreasAsgs where parent = 401 and ASGS = 2021")
for row in cursor.fetchall():
    print(row.ASGSCode)
'''


# Fetching population data
cursor.execute("""
    SELECT 
        [ASGS_2016] as ASGSCode,
        [ERPYear], 
        SUM([Number]) as Population 
    FROM 
        [forecasts].[dbo].[ERP] 
    GROUP BY 
        ASGS_2016, ERPYear 
    ORDER BY 
        ERPYear
""")

population_data = cursor.fetchall()
#print(population_data[:5])
#population_data = population_data[:5]
population_data =np.array(population_data)
#print(population_data)
population_df = pd.DataFrame(population_data, columns=['ASGSCode', 'ERPYear', 'Population'])
print(population_df[:5])


# Fetching household data
cursor.execute("""
    SELECT TOP (3000) 
        [ASGSCode],
        [ERPYear],
        SUM([Number]) as HouseholdCount 
    FROM 
        [forecasts].[dbo].[Households]
    WHERE 
        HhKey = 19 
    GROUP BY 
        ASGSCode, HhKey, ERPYear 
    ORDER BY 
        ERPYear, ASGSCode
""")
household_data = cursor.fetchall()
#print(household_data[:5])
household_data = np.array(household_data)
#print(household_data)
household_df = pd.DataFrame(household_data, columns=["ASGSCode", "ERPYear", "HouseholdCount"])
print(household_df[:5])


# Merging the dataframes
merged_df = pd.merge(population_df, household_df, left_on=['ERPYear', 'ASGSCode'], right_on=['ERPYear', 'ASGSCode'])

# Calculating the percentage of population in households
merged_df['Percentage'] = (merged_df['HouseholdCount'] / merged_df['Population']) * 100
print(merged_df["Percentage"])
summary = merged_df["Percentage"].describe()
print(summary)


# draw box plot, and every outlier should have a table 

# Step 1: Draw the box plot
plt.figure(figsize=(10, 6))
sns.boxplot(x=merged_df["Percentage"], showfliers=True)
plt.title("Box Plot of Percentage with Outliers")
plt.xlabel("Percentage")
plt.show()

# Step 2: Extract outliers
# Calculate the IQR (Interquartile Range)
Q1 = merged_df["Percentage"].quantile(0.25)
Q3 = merged_df["Percentage"].quantile(0.75)
IQR = Q3 - Q1

# Determine the bounds for outliers
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Filter the outliers
outliers_df = merged_df[(merged_df["Percentage"] < lower_bound) | (merged_df["Percentage"] > upper_bound)]
print(outliers_df[5:])

# Step 3: Create a table with region code, ERPYear, and percentage
outliers_table = outliers_df[["ASGSCode", "ERPYear", "Percentage"]]
print(outliers_table[:5])

###################################################################################################################################################
###################################################################################################################################################

# Max/Min Population 
cursor.execute("""
    WITH BaseData AS (
    SELECT 
        ASGS_2016,
        ERPYear,
        SUM(Number) AS Number
    FROM 
        [forecasts].[dbo].[ERP]
    GROUP BY 
        ASGS_2016,
        ERPYear
)

    SELECT 
        ASGS_2016,
        ERPYear,
        Number,
        -- Add a flag to indicate if Number is less than 2000 or greater than 2500
        CASE 
            WHEN Number > 2000 THEN 'Greater than 2000'
            WHEN Number > 2500 THEN 'Greater than 2500'
            ELSE 'Within Range'
        END AS NumberFlag
    FROM 
        BaseData
    ORDER BY 
        ERPYear, ASGS_2016;
""")

# Fetch and print the results
number_data = cursor.fetchall()
number_data = np.array(number_data)
number_pd = pd.DataFrame(number_data)
print(number_pd[:5])







####################################################################################################################################################
# Close connection
connection.close()
