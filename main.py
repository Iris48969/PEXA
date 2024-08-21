import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def connect_to_database():
    """
    Establish a connection to the SQL Server database.

    Returns:
    - connection (pyodbc.Connection): A connection object to interact with the database.
    """
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=localhost;'
                              'Database=forecasts;'
                              'UID=sa;'
                              'PWD=MBS_project_2024')
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def fetch_data(query, connection):
    """
    Execute a SQL query and fetch the results into a Pandas DataFrame.

    Parameters:
    - query (str): The SQL query to execute.
    - connection (pyodbc.Connection): The database connection object.

    Returns:
    - pd.DataFrame: DataFrame containing the query results.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description] if cursor.description else []
            if not columns:
                return pd.DataFrame()
            return pd.DataFrame.from_records(rows, columns=columns)
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def check_missing_lat_long(df_areas_asgs):
    """
    Check for missing latitude and longitude values in the AreasAsgs DataFrame.

    Parameters:
    - df_areas_asgs (pd.DataFrame): DataFrame containing AreasAsgs data.

    This function groups the data by 'ASGS' and 'RegionType', checks if latitude 
    or longitude values are missing, and prints the Year, RegionType, and count of missing values.
    """
    df_filtered = df_areas_asgs[df_areas_asgs['RegionType'].isin(['SA2', 'FA'])].copy()
    df_filtered.loc[:, 'Missing_Latitude'] = df_filtered['Latitude'].isnull().astype(int)
    df_filtered.loc[:, 'Missing_Longitude'] = df_filtered['Longitude'].isnull().astype(int)

    df_missing = df_filtered.groupby(['ASGS', 'RegionType']).agg(
        Total_Missing=('Missing_Latitude', 'sum')
    ).reset_index()

    print("\nMissing Latitude/Longitude Check (SA2 and FA regions):")
    print(df_missing)


def check_negative_values(df_erp, df_areas_asgs):
    """
    Check for records with negative population values in the SA2 and FA region types.

    Parameters:
    - df_erp (pd.DataFrame): DataFrame containing ERP data.
    - df_areas_asgs (pd.DataFrame): DataFrame containing AreasAsgs data.

    This function calculates the total number of negative population values in the 
    SA2 and FA region types and prints these counts along with the corresponding ASGSCodes.
    """
    df_merged = df_erp.merge(df_areas_asgs, left_on='ASGS_2016', right_on='ASGSCode')
    df_negative = df_merged[df_merged['Number'] < 0]
    df_filtered = df_negative[df_negative['RegionType'].isin(['SA2', 'FA'])]

    for region in ['SA2', 'FA']:
        df_region = df_filtered[df_filtered['RegionType'] == region]
        total_negatives = df_region.shape[0]
        asgs_codes = df_region['ASGSCode'].unique()

        print(f"\nTotal Negative Values in {region}: {total_negatives}")
        print(f"ASGSCodes with Negative Values in {region}:")
        print(asgs_codes)

def check_sa2_consistency_households(df_erp, df_areas_asgs):
    """
    Check for consistency of ASGS codes between the erp DataFrame and AreasAsgs DataFrame.

    Parameters:
    - df_erp (pd.DataFrame): DataFrame containing population data.
    - df_areas_asgs (pd.DataFrame): DataFrame containing AreasAsgs data.

    This function identifies records in the erp data that do not have a matching
    entry in the AreasAsgs data, indicating potential inconsistencies in the SA2 codes.
    """

    df_erp_missing = df_erp.merge(df_areas_asgs, left_on='ASGSCode', right_on='ASGSCode', how='left', indicator=True)
    df_erp_missing = df_erp_missing[df_erp_missing['_merge'] == 'left_only']
    if df_erp_missing.empty:
        print("\nSA2 in ERP Code Consistency Check: Passed")
    else:
        print("\nSA2 in ERP Code Consistency Check:")
        print(df_erp_missing)

def check_sa2outliers(df_erp, df_areas_asgs):
    """
    Checks for outliers in the growth rate for each SA2 region 
    for each ERPYear using IQR. Only outliers in growth rate are reported.

    Parameters:
    - df_erp (DataFrame): The ERP data.
    - df_areas_asgs (DataFrame): The AreasAsgs data.

    This function merges ERP and AreasAsgs to get relevant population data, calculates the growth rates 
    of each SA2 region, and detects outliers in growth rate using IQR.
    """
    df_population = df_erp.merge(df_areas_asgs, left_on='ASGS_2016', right_on='ASGSCode')
    df_population = df_population[df_population['RegionType'] == 'SA2']
    df_population = df_population.groupby(['ASGSCode', 'ERPYear'])['Number'].sum().reset_index()
    df_population.rename(columns={'Number': 'Population'}, inplace=True)
    df_population['Population'] = df_population['Population'].fillna(0)
    df_population.replace([float('inf'), -float('inf')], 0, inplace=True)
    df_population.sort_values(by=['ASGSCode', 'ERPYear'], inplace=True)
    df_population['GrowthRate'] = df_population.groupby('ASGSCode')['Population'].pct_change() * 100
    df_population['GrowthRate'] = df_population['GrowthRate'].fillna(0)
    df_population.replace([float('inf'), -float('inf')], 0, inplace=True)

    # Function to detect outliers using IQR for GrowthRate
    def detect_growth_rate_outliers(group):
        if group['GrowthRate'].dropna().empty:
            return pd.DataFrame()  
        
        Q1_growth = group['GrowthRate'].quantile(0.25)
        Q3_growth = group['GrowthRate'].quantile(0.75)
        IQR_growth = Q3_growth - Q1_growth
        lower_bound_growth = Q1_growth - 1.5 * IQR_growth
        upper_bound_growth = Q3_growth + 1.5 * IQR_growth

        outliers_growth = group[(group['GrowthRate'] < lower_bound_growth) | (group['GrowthRate'] > upper_bound_growth)]

        if not outliers_growth.empty:
            print(f"Outliers detected in GrowthRate for ASGSCode {group.name[0]} (ERPYear {group.name[1]}):")
            print(outliers_growth[['ASGSCode', 'ERPYear', 'GrowthRate']])
        
        return outliers_growth[['ASGSCode', 'ERPYear', 'GrowthRate']]

    # Apply the function to each group
    #outliers = df_population.groupby(['ASGSCode', 'ERPYear'], group_keys=False).apply(detect_growth_rate_outliers).reset_index(drop=True)
    grouped = df_population.groupby(['ASGSCode', 'ERPYear'])
    outliers = grouped.apply(lambda x: detect_growth_rate_outliers(x)).reset_index(drop=True)

    # Check if outliers DataFrame is empty
    if outliers.empty:
        print("\nSA2 Growth Rate Outlier Check: Passed")
    else:
        print("\nSA2 Growth Rate Outlier Check:")
        print(outliers)

def main():
    """
    Main function to execute the data quality checks and analysis.

    This function establishes a connection to the database, fetches necessary data,
    performs data quality checks, analyzes the SA2 population, and then closes the
    database connection.
    """
    connection = connect_to_database()

    # Fetch tables
    df_areas_asgs = fetch_data("SELECT * FROM AreasAsgs", connection)
    df_erp = fetch_data("SELECT * FROM ERP", connection)
    df_households = fetch_data("SELECT * FROM Households", connection)

    # Perform checks
    check_missing_lat_long(df_areas_asgs)
    check_negative_values(df_erp, df_areas_asgs)
    check_sa2_consistency_households(df_households, df_areas_asgs)
    check_sa2outliers(df_erp, df_areas_asgs)

    connection.close()

# Run the main function
if __name__ == "__main__":
    main()
