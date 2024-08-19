import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description] if cursor.description else []
            if not columns:
                # Handle case where there are no columns (e.g., empty result set)
                return pd.DataFrame()
            return pd.DataFrame.from_records(rows, columns=columns)
    except Exception as e:
        # Log or print the exception message
        print(f"An error occurred: {e}")
        # Optionally, you might want to raise the exception to handle it higher up
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
    "missing_lat_long" : """
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
GROUP BY ASGS,RegionType,
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
    """
}

# Execute data quality checks
def perform_data_quality_checks(connection):
    """
    Perform a series of data quality checks using predefined SQL queries.

    Parameters:
    - connection (object): The database connection object.
    """
    queries = {
        "missing_lat_long": "YOUR_QUERY_HERE",
        "negative_values_sa2": "YOUR_QUERY_HERE",
        "negative_values_fa": "YOUR_QUERY_HERE",
        "sa2_consistency_households": "YOUR_QUERY_HERE",
        "sa2_consistency_erp": "YOUR_QUERY_HERE",
        "sumfa_equal_sa2_check": "YOUR_QUERY_HERE",
    }
    
    # Define the checks and their corresponding names
    checks = [
        ("missing_lat_long", "Missing Latitude/Longitude"),
        ("negative_values_sa2", "Negative Population in SA2s"),
        ("negative_values_fa", "Negative Population in FAs"),
        ("sa2_consistency_households", "SA2 in Households Code Consistency"),
        ("sa2_consistency_erp", "SA2 and FA in ERP Code Consistency"),
        ("sumfa_equal_sa2_check", "Population in SA2 equals total population in FA")
    ]
    
    # Perform each data quality check
    for query_key, check_name in checks:
        print(f"Running data quality check: {check_name}")
        run_data_quality_check(queries[query_key], check_name, connection)
        
# Perform outlier detection on SA2 data
def analyze_sa2_population():
    df_population = fetch_data(queries["sa2_population_over_25years"])
    
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
    print("\n In 2023 and 2024, the percentage change in the population of SA2 exceeded the expected value, considering the mean change of", mean_change, "\n")
    
    # Plotting yearly population change
    plt.figure(figsize=(14, 7))
    # Plot the absolute population change over the years
    sns.lineplot(data=df_population, x='ERPYear', y='Percentage_Change', hue='Anomaly', marker='o')
    plt.axhline(mean_change, color='red', linestyle='--', label=f'Mean Change: {mean_change:.2f}')

    plt.title('Yearly Percentage Change in SA2 population')
    plt.xlabel('Year')
    plt.ylabel('Population Percentage Change')
    plt.legend(title='Anomaly Status')
    plt.grid(True)
    plt.show()
# Main execution
connection = connect_to_database()
perform_data_quality_checks()
analyze_sa2_population()
connection.close()
