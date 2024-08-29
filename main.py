import os
import ruptures as rpt
import logging
import psycopg2  # type: ignore
import datetime
import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import IsolationForest  # type: ignore
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import pairwise_distances
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.stats.outliers_influence import summary_table
from scipy.stats import zscore

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

def perform_negative_checks(conn):
    '''
    This function checks for Negative values in tables Births, Deaths, ERP
    '''
    query_file_path = 'C:/Users/Shrvan/Downloads/A-lab/pyforecasts/pyforecasts/SQL_file.sql'
    try:
        logging.info("Fetching data from SQL Server...")
        with open(query_file_path, 'r') as file:
            query = file.read()
        df = pd.read_sql(query, conn)
        logging.info("Data fetched successfully.")
        logging.info("Performing negative checks...")
        
        negative_births = df[(df['DataType'] == 'Births') & (df['Total'] < -10) & (df['RegionType'] == 'FA')]
        negative_deaths = df[(df['DataType'] == 'Deaths') & (df['Total'] < -10) & (df['RegionType'] == 'FA')]
        negative_erp = df[(df['DataType'] == 'ERP') & (df['Total'] < -10)]

        return {
            'negative_births': negative_births,
            'negative_deaths': negative_deaths,
            'negative_erp': negative_erp
        }

    except Exception as e:
        logging.error(f"Error performing negative checks: {e}")
        raise

def perform_sanity_checks(conn):
    '''
    This function performs checks for zero values, missing values, duplicate values in Births, Deaths, ERP
    '''
    query_file_path = 'C:/Users/Shrvan/Downloads/A-lab/pyforecasts/pyforecasts/SQL_file.sql'
    try:
        logging.info("Fetching data from SQL Server...")
        with open(query_file_path, 'r') as file:
            query = file.read()
        df = pd.read_sql(query, conn)
        logging.info("Data fetched successfully.")
        logging.info("Performing sanity checks...")
        
        # Check for zero values in Births, Deaths, and Population
        zero_births = df[(df['DataType'] == 'Births') & (df['Total'] == 0)]
        zero_deaths = df[(df['DataType'] == 'Deaths') & (df['Total'] == 0)]
        zero_population = df[(df['DataType'] == 'ERP') & (df['Total'] == 0)]

        # Check for missing values
        missing_values = df.isnull().sum()

        # Check for duplicate records
        duplicates = df[df.duplicated()]

        return {
            'zero_births': zero_births,
            'zero_deaths': zero_deaths,
            'zero_population': zero_population,
            'missing_values': missing_values,
            'duplicates': duplicates
        }

    except Exception as e:
        logging.error(f"Error performing sanity checks: {e}")
        raise

def perform_outlier_checks(conn):
    '''
    This function performs outlier detection in ERP table at SA2 and FA levels.
    '''
    query_file_path = 'C:/Users/Shrvan/Downloads/A-lab/pyforecasts/pyforecasts/SQL_file.sql'
    try:
        logging.info("Fetching data from SQL Server...")
        with open(query_file_path, 'r') as file:
            query = file.read()
        df = pd.read_sql(query, conn)
        logging.info("Data fetched successfully.")
        logging.info("Performing outlier checks...")

        # Filter for ERP data at both SA2 and FA levels
        df = df[(df['DataType'] == 'ERP') & (df['RegionType'].isin(['SA2', 'FA']))]

        # Separate the data by RegionType
        results = {}
        for region_type in df['RegionType'].unique():
            # Filter data for the current RegionType
            region_df = df[df['RegionType'] == region_type]

            # Pivot the DataFrame to get wide format
            wide_df = region_df.pivot_table(index='ASGSCode', columns='Year', values='Total')

            # Drop rows with NA values
            wide_df = wide_df.dropna()

            # Calculate the population growth rate in percentage
            growth_rates = wide_df.pct_change(axis=1) * 100
            
            # Drop NA values in growth rates
            growth_rates = growth_rates.dropna()

            # Flatten the growth rates for outlier detection
            flat_growth_rates = growth_rates.stack().reset_index(name='GrowthRate')

            # Identify outliers using IQR method
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

        return results

    except Exception as e:
        logging.error(f"Error performing outlier checks: {e}")
        raise

def generate_report(negative_checks, sanity_checks, outlier_checks):
    '''
    This function generates a single DataFrame with all results from the checks.
    '''
    rows = []

    # Negative Checks
    for check_type, df in [('Negative Births', negative_checks['negative_births']),
                            ('Negative Deaths', negative_checks['negative_deaths']),
                            ('Negative ERP', negative_checks['negative_erp'])]:
        if not df.empty:
            grouped = df.groupby('RegionType')['ASGSCode'].apply(lambda x: list(map(str, x))).reset_index()
            for _, row in grouped.iterrows():
                rows.append({
                    'RegionType': row['RegionType'],
                    'ASGSCode': ', '.join(row['ASGSCode']),
                    'DetailedDescription': f"{check_type} - ASGS Codes: {', '.join(row['ASGSCode'])}"
                })

    # Sanity Checks
    if not sanity_checks['zero_births'].empty:
        grouped = sanity_checks['zero_births'].groupby('RegionType')['ASGSCode'].apply(lambda x: list(map(str, x))).reset_index()
        for _, row in grouped.iterrows():
            rows.append({
                'RegionType': row['RegionType'],
                'ASGSCode': ', '.join(row['ASGSCode']),
                'DetailedDescription': f"Zero Births - ASGS Codes: {', '.join(row['ASGSCode'])}"
            })

    if not sanity_checks['zero_deaths'].empty:
        grouped = sanity_checks['zero_deaths'].groupby('RegionType')['ASGSCode'].apply(lambda x: list(map(str, x))).reset_index()
        for _, row in grouped.iterrows():
            rows.append({
                'RegionType': row['RegionType'],
                'ASGSCode': ', '.join(row['ASGSCode']),
                'DetailedDescription': f"Zero Deaths - ASGS Codes: {', '.join(row['ASGSCode'])}"
            })

    if not sanity_checks['zero_population'].empty:
        grouped = sanity_checks['zero_population'].groupby('RegionType')['ASGSCode'].apply(lambda x: list(map(str, x))).reset_index()
        for _, row in grouped.iterrows():
            rows.append({
                'RegionType': row['RegionType'],
                'ASGSCode': ', '.join(row['ASGSCode']),
                'DetailedDescription': f"Zero Population - ASGS Codes: {', '.join(row['ASGSCode'])}"
            })

    if sanity_checks['missing_values'].any():
        for col, count in sanity_checks['missing_values'].items():
            rows.append({
                'RegionType': 'Missing Values',
                'ASGSCode': 'N/A',
                'DetailedDescription': f"Missing Values - Column: {col}, Missing Values: {count}"
            })

    if not sanity_checks['duplicates'].empty:
        rows.append({
            'RegionType': 'Duplicate Records',
            'ASGSCode': 'N/A',
            'DetailedDescription': f"Duplicate Records - Count: {sanity_checks['duplicates'].shape[0]}"
        })

    # Outlier Checks
    for region_type, result in outlier_checks.items():
        if isinstance(result['outliers'], pd.DataFrame) and not result['outliers'].empty:
            grouped = result['outliers'].groupby('RegionType')['ASGSCode'].apply(lambda x: list(map(str, x))).reset_index()
            for _, row in grouped.iterrows():
                rows.append({
                    'RegionType': f"Outliers in {region_type}",
                    'ASGSCode': ', '.join(row['ASGSCode']),
                    'DetailedDescription': f"Outliers - Growth Rate details for ASGS Codes: {', '.join(row['ASGSCode'])}"
                })
        else:
            rows.append({
                'RegionType': f"Outliers in {region_type}",
                'ASGSCode': 'N/A',
                'DetailedDescription': result['outliers']
            })

    return pd.DataFrame(rows)



if __name__ == '__main__':
    try:
        conn = connect_to_database()

        negative_checks = perform_negative_checks(conn)
        sanity_checks = perform_sanity_checks(conn)
        outlier_checks = perform_outlier_checks(conn)

        report_df = generate_report(negative_checks, sanity_checks, outlier_checks)
        
        # Print or save the report DataFrame
        print(report_df)
        report_df.to_csv('report_output.csv', index=False)

    finally:
        if conn:
            conn.close()
