import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Function to connect to the database
def connect_to_database():
    return pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=forecasts;'
                          'UID=sa;'
                          'PWD=MBS_project_2024')

# Function to fetch population data
def fetch_population_data(cursor):
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
    population_data = np.array(population_data)
    return pd.DataFrame(population_data, columns=['ASGSCode', 'ERPYear', 'Population'])

# Function to fetch household data
def fetch_household_data(cursor):
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
    household_data = np.array(household_data)
    return pd.DataFrame(household_data, columns=["ASGSCode", "ERPYear", "HouseholdCount"])

# Function to merge population and household data and calculate percentage
def calculate_percentage(merged_df):
    merged_df['Percentage'] = (merged_df['HouseholdCount'] / merged_df['Population']) * 100
    return merged_df

# Function to detect outliers
def detect_outliers(merged_df):
    Q1 = merged_df["Percentage"].quantile(0.25)
    Q3 = merged_df["Percentage"].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers_df = merged_df[(merged_df["Percentage"] < lower_bound) | (merged_df["Percentage"] > upper_bound)]
    return outliers_df

# Function to calculate population growth rate
def calculate_growth_rate(number_pd, periods):
    result = []
    for period_start, period_end in periods:
        period_data = number_pd[(number_pd['ERPYear'] == period_start) | (number_pd['ERPYear'] == period_end)]
        for region in period_data['ASGSCode'].unique():
            region_data = period_data[period_data['ASGSCode'] == region]
            if len(region_data) == 2:
                pop_start = region_data[region_data['ERPYear'] == period_start]['Population'].values[0]
                pop_end = region_data[region_data['ERPYear'] == period_end]['Population'].values[0]
                growth_rate = ((pop_end - pop_start) / pop_start) * 100
                if growth_rate > 25:
                    result.append([region, f'{period_start}-{period_end}', pop_start, pop_end, growth_rate])
    return pd.DataFrame(result, columns=['Region', 'Period', 'Population Start', 'Population End', 'Growth Rate'])

# Function to close the database connection
def close_connection(connection):
    connection.close()

# Main function
def main():
    # Connect to the database
    connection = connect_to_database()
    cursor = connection.cursor()

    # Fetch data
    population_df = fetch_population_data(cursor)
    household_df = fetch_household_data(cursor)

    # Merge dataframes and calculate percentage
    merged_df = pd.merge(population_df, household_df, left_on=['ERPYear', 'ASGSCode'], right_on=['ERPYear', 'ASGSCode'])
    merged_df = calculate_percentage(merged_df)
    
    # Summary and outliers detection
    summary = merged_df["Percentage"].describe()
    print(summary)

    # Detect outliersss
    outliers_df = detect_outliers(merged_df)
    outliers_table = outliers_df[["ASGSCode", "ERPYear", "Percentage"]]
    print(outliers_table)

    # Plotting box plot with outliers
    plt.figure(figsize=(10, 6))
    sns.boxplot(x=merged_df["Percentage"], showfliers=True)
    plt.title("Box Plot of Percentage with Outliers")
    plt.xlabel("Percentage")
    plt.show()

    # Define periods for growth rate calculation
    periods = [(2022, 2026), (2027, 2031), (2032, 2036), (2037, 2041), (2042, 2046)]

    # Calculate growth rate
    result_df = calculate_growth_rate(population_df, periods)
    print(result_df)
    
    # Close the database connection
    close_connection(connection)

# Run the main function
if __name__ == "__main__":
    main()
