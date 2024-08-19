import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Database connection
def connect_to_database():
    return pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=forecasts;'
                          'UID=sa;'
                          'PWD=MBS_project_2024')

# Function to execute and fetch query results into DataFrame
def fetch_data(query, connection):
    """
    Execute a SQL query and fetch the results into a Pandas DataFrame.

    Parameters:
    - query (str): The SQL query to execute.
    - connection (object): The database connection object.

    Returns:
    - pd.DataFrame: DataFrame containing the query results.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description] if cursor.description else []
        if not columns:
            # Handle case where there are no columns (e.g., empty result set)
            return pd.DataFrame()
        return pd.DataFrame.from_records(rows, columns=columns)
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

# Data Quality Check function
def run_data_quality_check(query, check_name, connection):
    """
    Run a data quality check based on a SQL query and print the result.

    Parameters:
    - query (str): The SQL query to execute.
    - check_name (str): The name of the data quality check.
    - connection (object): The database connection object.

    Returns:
    - bool: True if the data quality check passed, False otherwise.
    """
    try:
        df = fetch_data(query, connection)
        # Check if the DataFrame is empty or if the first value is 0
        if df.empty or (df.iloc[0, 0] == 0 if not df.empty else False):
            print(f"\nData Quality Checking for '{check_name}' PASSED.")
            return True
        else:
            print(f"\nData Quality Checking for '{check_name}' FAILED. First few rows are below.")
            print(df.head())  # Print only the first few rows for readability
            return False
    except Exception as e:
        print(f"Error running data quality check '{check_name}': {e}")
        return False

# Define Queries
queries = {
    "missing_lat_long": """
    SELECT ASGS AS Year, RegionType,
        CASE 
            WHEN Latitude IS NULL THEN 'Missing Latitude'
            ELSE 'Latitude Present'
        END AS Latitude_Status,
        CASE 
            WHEN Longitude IS NULL THEN 'Missing Longitude'
            ELSE 'Longitude Present'
        END AS Longitude_Status,
        COUNT(*) AS Count
    FROM AreasAsgs
    GROUP BY ASGS, RegionType,
        CASE 
            WHEN Latitude IS NULL THEN 'Missing Latitude'
            ELSE 'Latitude Present'
        END,
        CASE 
            WHEN Longitude IS NULL THEN 'Missing Longitude'
            ELSE 'Longitude Present'
        END
    ORDER BY Year
    """,
    
    "negative_values_sa2": """
    SELECT TOP 10 a.ASGS_2016, b.RegionType, b.Parent, a.AgeKey, a.SexKey, a.ERPYear, 
           SUM(a.Number) AS Population, 
           (SELECT COUNT(*) FROM ERP AS a
            INNER JOIN AreasAsgs AS b ON a.ASGS_2016 = b.ASGSCode
            WHERE a.Number < 0 AND b.RegionType IN ('SA2') AND b.ASGS = 2016) AS Total_Negative_Count_SA2s
    FROM ERP AS a
    INNER JOIN AreasAsgs AS b ON a.ASGS_2016 = b.ASGSCode
    WHERE a.Number < 0 AND b.RegionType IN ('SA2') AND b.ASGS = 2016
    GROUP BY a.ERPYear, a.ASGS_2016, b.RegionType, b.Parent, a.AgeKey, a.SexKey
    ORDER BY a.ERPYear
    """,

    "negative_values_fa": """
    SELECT TOP 10 a.ASGS_2016, b.RegionType, b.Parent, a.AgeKey, a.SexKey, a.ERPYear, 
           SUM(a.Number) AS Population, 
           (SELECT COUNT(*) FROM ERP AS a
            INNER JOIN AreasAsgs AS b ON a.ASGS_2016 = b.ASGSCode
            WHERE a.Number < 0 AND b.RegionType IN ('FA') AND b.ASGS = 2016) AS Total_Negative_Count_FAs
    FROM ERP AS a
    INNER JOIN AreasAsgs AS b ON a.ASGS_2016 = b.ASGSCode
    WHERE a.Number < 0 AND b.RegionType IN ('FA') AND b.ASGS = 2016
    GROUP BY a.ERPYear, a.ASGS_2016, b.RegionType, b.Parent, a.AgeKey, a.SexKey
    ORDER BY a.ERPYear
    """,

    "sa2_consistency_households": """
    SELECT COUNT(*) FROM Households h
    LEFT JOIN AreasAsgs a ON h.ASGSCode = a.ASGSCode
    WHERE a.ASGSCode IS NULL AND h.HhKey = 19
    """,

    "sa2_consistency_erp": """
    SELECT COUNT(*) FROM ERP e
    LEFT JOIN AreasAsgs a ON e.ASGS_2016 = a.ASGSCode
    WHERE a.ASGSCode IS NULL AND a.RegionType IN ('SA2', 'FA')
    """,

    "sumfa_equal_sa2_check": """
    WITH SA2_Population AS (
        SELECT a.RegionType, a.ASGSCode AS SA2_Code, a.Parent AS Parent_SA2_Code, SUM(b.Number) AS SA2_Population
        FROM AreasAsgs AS a
        INNER JOIN ERP AS b ON b.ASGS_2016 = a.ASGSCode
        WHERE a.RegionType = 'SA2' AND a.ASGS = 2016
        GROUP BY a.RegionType, a.Parent, a.ASGSCode
    ),
    FA_Population AS (
        SELECT a.RegionType, a.ASGSCode AS FA_Code, a.Parent AS Parent_SA2_Code, SUM(b.Number) AS Total_Population_FA
        FROM AreasAsgs AS a
        INNER JOIN ERP AS b ON b.ASGS_2016 = a.ASGSCode
        WHERE a.RegionType = 'FA' AND a.ASGS = 2016
        GROUP BY a.RegionType, a.ASGSCode, a.Parent
    )
    SELECT sa2.SA2_Code, sa2.SA2_Population, COALESCE(SUM(fa.Total_Population_FA), 0) AS Total_FA_Population,
           CASE 
               WHEN sa2.SA2_Population = COALESCE(SUM(fa.Total_Population_FA), 0) THEN 'Match'
               WHEN sa2.SA2_Population > 0 AND COALESCE(SUM(fa.Total_Population_FA), 0) = 0 THEN 'Mismatch - FA Population Zero'
               WHEN sa2.SA2_Population = 0 AND COALESCE(SUM(fa.Total_Population_FA), 0) = 0 THEN 'Match - Both Zero'
               ELSE 'Mismatch'
           END AS Population_Comparison
    FROM SA2_Population AS sa2
    LEFT JOIN FA_Population AS fa ON sa2.SA2_Code = fa.Parent_SA2_Code
    GROUP BY sa2.SA2_Code, sa2.SA2_Population
    ORDER BY sa2.SA2_Code;
    """,

    "sa2_population_over_25years": """
    SELECT a.RegionType, b.ERPYear, SUM(b.Number) AS SA2_Population
    FROM AreasAsgs AS a
    INNER JOIN ERP AS b ON b.ASGS_2016 = a.ASGSCode
    WHERE a.RegionType = 'SA2' AND a.ASGS = 2016
    GROUP BY a.RegionType, b.ERPYear 
    ORDER BY a.RegionType, b.ERPYear
    """,

    "ERP": """
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
    """,

    "Household_number": """
    SELECT 
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
    """
}

# Execute data quality checks
def perform_data_quality_checks(connection):
    """
    Perform a series of data quality checks using predefined SQL queries.

    Parameters:
    - connection (object): The database connection object.
    """
    checks = [
        ("missing_lat_long", "Missing Latitude/Longitude"),
        ("negative_values_sa2", "Negative Population in SA2s"),
        ("negative_values_fa", "Negative Population in FAs"),
        ("sa2_consistency_households", "SA2 in Households Code Consistency"),
        ("sa2_consistency_erp", "SA2 and FA in ERP Code Consistency"),
        ("sumfa_equal_sa2_check", "Population in SA2 equals total population in FA")
    ]
    
    for query_key, check_name in checks:
        print(f"Running data quality check: {check_name}")
        run_data_quality_check(queries[query_key], check_name, connection)
        
# Perform outlier detection on SA2 data
def analyze_sa2_population(connection):
    df_population = fetch_data(queries["sa2_population_over_25years"], connection)
    
    df_population['Previous_Population'] = df_population['SA2_Population'].shift(1)
    df_population['Population_Change'] = df_population['SA2_Population'] - df_population['Previous_Population']
    df_population['Percentage_Change'] = (df_population['Population_Change'] / df_population['Previous_Population']) * 100

    df_population = df_population.dropna(subset=['Percentage_Change'])

    mean_change = df_population['Percentage_Change'].mean()
    std_dev_change = df_population['Percentage_Change'].std()

    df_population['Anomaly'] = df_population['Percentage_Change'].apply(
        lambda x: 'Anomaly' if abs(x - mean_change) > 2 * std_dev_change else 'Normal'
    )

    print("\nOutliers Detected in Percent_Change in SA2 over 25 years :")
    print(df_population[df_population['Anomaly'] == 'Anomaly'])
    print("\nIn 2023 and 2024, the percentage change in the population of SA2 exceeded the expected value, considering the mean change of", mean_change, "\n")
    
    # Plotting yearly population change
    plt.figure(figsize=(14, 7))
    sns.lineplot(data=df_population, x='ERPYear', y='Percentage_Change', hue='Anomaly', marker='o')
    plt.axhline(mean_change, color='red', linestyle='--', label=f'Mean Change: {mean_change:.2f}')
    plt.title('Yearly Percentage Change in SA2 population')
    plt.xlabel('Year')
    plt.ylabel('Population Percentage Change')
    plt.legend(title='Anomaly Status')
    plt.grid(True)
    plt.show()

# Calculates household population percentages and identifies outliers
def household_ERP_ratio(connection):
    population_data = fetch_data(queries["ERP"], connection)
    household_data = fetch_data(queries["Household_number"], connection)
    
    population_df = pd.DataFrame(population_data, columns=['ASGSCode', 'ERPYear', 'Population'])
    print(population_df[:5])

    household_df = pd.DataFrame(household_data, columns=["ASGSCode", "ERPYear", "HouseholdCount"])
    print(household_df[:5])

    # Merging the dataframes
    merged_df = pd.merge(population_df, household_df, left_on=['ERPYear', 'ASGSCode'], right_on=['ERPYear', 'ASGSCode'])

    # Calculating the percentage of population in households
    merged_df['Percentage'] = (merged_df['HouseholdCount'] / merged_df['Population']) * 100
    print(merged_df["Percentage"])
    summary = merged_df["Percentage"].describe()
    print(summary)

    # Draw box plot, and every outlier should have a table 
    plt.figure(figsize=(10, 6))
    sns.boxplot(x=merged_df["Percentage"], showfliers=True)
    plt.title("Box Plot of Percentage with Outliers")
    plt.xlabel("Percentage")
    plt.show()

    # Extract outliers
    Q1 = merged_df["Percentage"].quantile(0.25)
    Q3 = merged_df["Percentage"].quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    outliers_df = merged_df[(merged_df["Percentage"] < lower_bound) | (merged_df["Percentage"] > upper_bound)]
    print(outliers_df[:5])

    outliers_table = outliers_df[["ASGSCode", "ERPYear", "Percentage"]]
    number_of_rows = len(outliers_table)
    print(f"Number of outliers: {number_of_rows}")
    print(outliers_table[:5])

# Calculates 5-year population growth rates and identifies regions with a growth rate exceeding 25%
def growth_rate_per_5_years(connection):
    number = fetch_data(queries["ERP"], connection)
    number_pd = pd.DataFrame(number, columns=['ASGSCode','ERPYear','Population'])
    periods = [(2022, 2026), (2027, 2031), (2032, 2036), (2037, 2041), (2042, 2046)]

    # Initialize a list to store the results
    result = []

    for period_start, period_end in periods:
        # Filter the data for the current period
        period_data = number_pd[(number_pd['ERPYear'] == period_start) | (number_pd['ERPYear'] == period_end)]
        
        # Ensure that we have both start and end years data for the calculation
        for region in period_data['ASGSCode'].unique():
            region_data = period_data[period_data['ASGSCode'] == region]
            
            if len(region_data) == 2:
                pop_start = region_data[region_data['ERPYear'] == period_start]['Population'].values[0]
                pop_end = region_data[region_data['ERPYear'] == period_end]['Population'].values[0]
                
                # Calculate the growth rate
                growth_rate = ((pop_end - pop_start) / pop_start) * 100
                
                if growth_rate > 25:
                    result.append([region, f'{period_start}-{period_end}', pop_start, pop_end, growth_rate])

    # Convert the result to a DataFrame
    result_df = pd.DataFrame(result, columns=['Region', 'Period', 'Population Start', 'Population End', 'Growth Rate'])

    # Display the new table and count of regions exceeding the growth rate threshold
    print(result_df)
    count_number = len(result_df)
    print(f"Number of regions with growth rate greater than 25%: {count_number}")

# Main function
def main():
    connection = None
    try:
        connection = connect_to_database()
        perform_data_quality_checks(connection)
        analyze_sa2_population(connection)
        household_ERP_ratio(connection)
        growth_rate_per_5_years(connection)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if connection is not None:
            connection.close()
            print("Database connection closed.")

# Execute the main function
if __name__ == "__main__":
    main()
