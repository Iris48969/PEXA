import os
import csv
import logging
import psycopg2  # type: ignore
import datetime
import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set up logging
my_date = datetime.datetime.now().strftime('%Y%m%d')
logFile = os.path.join(os.path.abspath("" + my_date + ".log"))  
logging.basicConfig(
    filename=logFile,
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)

def connect_to_database():
    '''
    This function creates a connection to the database  
    '''
    try:
        logging.info("Connecting to SQL Server database...")
        conn = pyodbc.connect(
            'Driver={SQL Server};'
            'Server=localhost;'
            'Database=forecasts;'
            'UID=sa;'
            'PWD=MBS_project_2024'
        )
        logging.info("Successfully connected to SQL Server database.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to SQL Server database: {e}")
        raise

def fetch_data_with_checks(conn):
    '''
    This function loads the SQL file and reads it and stores it in dataframe
    '''
    with open(os.path.abspath('C:/Users/Shrvan/Downloads/A-lab/pyforecasts/pyforecasts/SQL_file.sql'), 'r') as file:
            query = file.read()
    try:
        logging.info("Fetching data from SQL Server...")
        df = pd.read_sql(query, conn)
        logging.info("Data fetched successfully.")
        return df
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise

def perform_negative_checks(df):
    '''
    This function checks for Negative values in tables Births, Deaths, ERP
    '''
    try:
        logging.info("Performing negative checks...")
        negative_births = df[(df['DataType'] == 'Births') & (df['Total'] < -10) & (df['RegionType'] == 'FA')]
        if negative_births.empty:
            print("\n****************** CHECK 1: No Negative Births Values Found.")
        else:
            print(f"\n****************** CHECK 1: Checking for Negative values in Births Table")
            print(f"\nFound {negative_births.shape[0]} rows with Negative Births")
            print(negative_births['ASGSCode'].unique())
    
        negative_deaths = df[(df['DataType'] == 'Deaths') & (df['Total'] < -10) & (df['RegionType'] == 'FA')]
        if negative_deaths.empty:
            print("\n****************** CHECK 2: No Negative Deaths Values Found.")
        else:
            print(f"\n****************** CHECK 2: Checking for Negative values in Deaths Table")
            print(f"\nFound {negative_deaths.shape[0]} rows with Negative Deaths")
            print(negative_deaths['ASGSCode'].unique())
    
        negative_erp = df[(df['DataType'] == 'ERP') & (df['Total'] < -10)]
        if negative_erp.empty:
            print("\n****************** CHECK 3: No Negative ERP Values Found.")
        else:
            print(f"\n****************** CHECK 3:  Checking for Negative values in ERP Table ")
            print(f"\nFound {negative_erp.shape[0]} rows with Negative ERP")
            print(negative_erp['ASGSCode'].unique())
    except Exception as e:
        logging.error(f"Error performing negative checks: {e}")
        raise

def perform_sanity_checks(df):
    '''
    This function performs checks zero values, missing values, duplicate values in Births, Deaths, ERP
    '''
    try:
        logging.info("Performing sanity checks...")
        zero_births = df[(df['DataType'] == 'Births') & (df['Total'] == 0)]
        zero_deaths = df[(df['DataType'] == 'Deaths') & (df['Total'] == 0)]
        zero_population = df[(df['DataType'] == 'ERP') & (df['Total'] == 0)]
        print("\n****************** CHECK 4: Checking for Zero Values")
        print(f"Count of rows with Zero Births: {zero_births.shape[0]}")
        print(f"Count of rows with Zero Deaths: {zero_deaths.shape[0]}")
        print(f"Count of rows with Zero Population: {zero_population.shape[0]}")

        if not zero_births.empty:
            print("\nUnique ASGS Codes with Zero Births:")
            print(zero_births['ASGSCode'].unique())
        
        if not zero_deaths.empty:
            print("\nUnique ASGS Codes with Zero Deaths:")
            print(zero_deaths['ASGSCode'].unique())
        
        if not zero_population.empty:
            print("\nUnique ASGS Codes with Zero Population:")
            print(zero_population['ASGSCode'].unique())

        # Check for missing values
        missing_values = df.isnull().sum()
        if missing_values.any():
            print("\n****************** CHECK 5: Missing Values detected in the following columns:")
            print(missing_values[missing_values > 0])
        else:
            print("\n****************** CHECK 5: No missing values detected in the dataset.")

        # Check for duplicate records
        duplicates = df[df.duplicated()]
        if not duplicates.empty:
            print(f"\n****************** CHECK 6: Count of Duplicate Records: {duplicates.shape[0]}")
        else:
            print("\n****************** CHECK 6: No duplicate entries found.")
    except Exception as e:
        logging.error(f"Error performing sanity checks: {e}")
        raise

def perform_outlier_checks(df):
    '''
    This function performs outlier detection in ERP table at SA2 and FA levels.
    '''
    try:
        logging.info("Performing outlier checks...")
        # Filter for ERP data at both SA2 and FA levels
        df = df[(df['DataType'] == 'ERP') & (df['RegionType'].isin(['SA2', 'FA']))]
        # Separate the data by RegionType
        results = {}
        for region_type in df['RegionType'].unique():
            region_df = df[df['RegionType'] == region_type]
            # Pivot the DataFrame to get wide format
            wide_df = region_df.pivot_table(index='ASGSCode', columns='Year', values='Total')
            wide_df = wide_df.dropna()
            # Calculate the population growth rate in percentage
            growth_rates = wide_df.pct_change(axis=1) * 100
            growth_rates = growth_rates.dropna()
            # Flatten the growth rates for outlier detection
            flat_growth_rates = growth_rates.stack().reset_index(name='GrowthRate')

            Q1 = flat_growth_rates['GrowthRate'].quantile(0.25)
            Q3 = flat_growth_rates['GrowthRate'].quantile(0.75)
            IQR = Q3 - Q1

            outliers = flat_growth_rates[(flat_growth_rates['GrowthRate'] < (Q1 - 1.5 * IQR)) |
                                         (flat_growth_rates['GrowthRate'] > (Q3 + 1.5 * IQR))]

            results[region_type] = {
                'num_rows': len(region_df),
                'num_outliers': len(outliers),
                'outliers': outliers[['ASGSCode', 'Year', 'GrowthRate']] if not outliers.empty else 'No outliers found.'
            }
        print("\n****************** CHECK 7: OUTLIER CHECK******************")
        for region_type, result in results.items():
            print(f"\nRegionType: {region_type}")
            print(f"Number of rows: {result['num_rows']}")
            print(f"Number of outliers found: {result['num_outliers']}")
            if result['num_outliers'] != 'No outliers found.':
                print("Outliers Details:")
                print(result['outliers'])
            else:
                print(result['outliers'])

    except Exception as e:
        logging.error(f"Error performing outlier checks: {e}")
        raise

def main():
    try:
        sql_server_conn = connect_to_database()
        df = fetch_data_with_checks(sql_server_conn)
        perform_negative_checks(df)
        perform_sanity_checks(df)
        perform_outlier_checks(df)
        sql_server_conn.close()
        logging.info("Database connections closed.")
    
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
