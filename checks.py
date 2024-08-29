import pandas as pd
import os
import logging

def household_check(conn):
    """
    The purpose of this function is to identify abnormal spike/drop of population forecast
    in a timeseries format.

    input: connection
    output: list of list which contains [region code, region type, description]
    """
    try:
        # getting queries
        population_query = open(os.path.abspath("SQL_Queries/population_number.sql"), 'r').read()
        household_query = open(os.path.abspath("SQL_Queries/Household_type.sql"), 'r').read()
        logging.info("Query file opened")

        # execute queries
        cursor = conn.cursor()
        cursor.execute(population_query)
        pop_data = cursor.fetchall()
        pop_df = pd.DataFrame(pop_data, columns=['ASGSCode', 'Year', 'Pop_Number'])

        cursor.execute(household_query)
        household_data = cursor.fetchall()
        household_df = pd.DataFrame(household_data, columns=['ASGSCode', 'Year', 'Household_Number'])
        logging.info("Query data returned")

        # Call the check function and return the results
        return check_population_household_ratio(pop_df, household_df)

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return []

def check_population_household_ratio(pop_df, household_df):
    """
    This function identifies outliers in the ratio of population to household count 
    for different regions and years. It applies specific filtering conditions based 
    on the population threshold.

    Parameters:
    - pop_df: DataFrame containing population data
    - household_df: DataFrame containing household data

    Returns:
    - outliers_df: A DataFrame containing the ASGSCode, Year, and Ratio for outliers.
    - unique_outlier_asgs_codes: A list of unique ASGSCode values that need to be checked.
    """

    # Merge the two DataFrames on ASGSCode and Year
    merged_df = pd.merge(pop_df, household_df, on=['ASGSCode', 'Year'], how='inner')

    # Calculate the ratio of Population to HouseholdCount
    merged_df['Ratio'] = merged_df['Pop_Number'] / merged_df['Household_Number']
    low_population_threshold = 100

    # Apply the filtering conditions
    outliers_df = merged_df[
        ((merged_df['Pop_Number'] < low_population_threshold) & (merged_df['Ratio'] > 2)) |
        ((merged_df['Pop_Number'] >= low_population_threshold) & ((merged_df['Ratio'] > 5) | (merged_df['Ratio'] < 1)))
    ]

    # Get unique ASGSCode values that need to be checked
    unique_outlier_asgs_codes = outliers_df['ASGSCode'].unique()

    # Print the result for debugging purposes
    print("ASGSCode and Year that need to be checked (Ratio > 5 or Ratio < 1 for low population):")
    print(outliers_df[['ASGSCode', 'Year', 'Ratio']])
    print("Unique ASGSCode values that need to be checked:")
    print(unique_outlier_asgs_codes)
    print(f"Total unique ASGSCode values: {len(unique_outlier_asgs_codes)}")

    return outliers_df, unique_outlier_asgs_codes




