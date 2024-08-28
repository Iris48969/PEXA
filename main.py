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
from sklearn.ensemble import IsolationForest # type: ignore
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
        # Check for zero values in Births, Deaths, and Population
        zero_births = df[(df['DataType'] == 'Births') & (df['Total'] == 0)]
        zero_deaths = df[(df['DataType'] == 'Deaths') & (df['Total'] == 0)]
        zero_population = df[(df['DataType'] == 'ERP') & (df['Total'] == 0)]
        print("\n****************** CHECK 4: Checking for Zero Values")
        print(f"Count of rows with Zero Births: {zero_births.shape[0]}")
        print(f"Count of rows with Zero Deaths: {zero_deaths.shape[0]}")
        print(f"Count of rows with Zero Population: {zero_population.shape[0]}")

        # Print unique ASGS Codes 
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

        # Print the results
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

def perform_ml_anomaly_detection(df):
    '''
    This function performs anomaly detection using Isolation Forest ML in ERP table at SA2 and FA levels.
    '''
    try:
        logging.info("Performing machine learning anomaly detection...")

        # Filter for ERP data at both SA2 and FA levels
        df = df[(df['DataType'] == 'ERP') & (df['RegionType'].isin(['SA2', 'FA']))]

        # Separate the data by RegionType
        results = {}
        for region_type in df['RegionType'].unique():
            # Filter data for the current RegionType
            region_df = df[df['RegionType'] == region_type]
            # Pivot the DataFrame to get wide format
            wide_df = region_df.pivot_table(index='ASGSCode', columns='Year', values='Total')
            wide_df = wide_df.dropna()
            flattened_df = wide_df.reset_index()
            flattened_df.set_index('ASGSCode', inplace=True)

            # Apply Isolation Forest for anomaly detection
            model = IsolationForest(contamination=0.05)  # Adjust contamination as needed
            anomalies = model.fit_predict(flattened_df)

            # Add anomaly detection results to the DataFrame
            flattened_df['Anomaly'] = anomalies
            anomalies_df = flattened_df[flattened_df['Anomaly'] == -1]

            # Collect unique ASGSCode for anomalies
            unique_asgs_codes = anomalies_df.index.unique().tolist()

            results[region_type] = {
                'num_rows': len(region_df),
                'num_anomalies': len(anomalies_df),
                'anomalies': anomalies_df if not anomalies_df.empty else 'No anomalies found.',
                'unique_asgs_codes': unique_asgs_codes
            }

        # Print the results
        print("\n****************** CHECK 8: MACHINE LEARNING ANOMALY DETECTION ******************")
        for region_type, result in results.items():
            print(f"\nRegionType: {region_type}")
            print(f"Number of rows: {result['num_rows']}")
            print(f"Number of anomalies found: {result['num_anomalies']}")
            if result['num_anomalies'] != 'No anomalies found.':
                print("Anomalies Details:")
                print(f"Unique ASGSCode with anomalies: {result['unique_asgs_codes']}")
            else:
                print(f"No Anomalies Found in {region_type}")
    except Exception as e:
        logging.error(f"Error performing machine learning anomaly detection: {e}")
        raise

def perform_bayesian_change_point_detection(df, penalty=10, similarity_threshold=6):
    '''
    Perform Bayesian Change Point Detection on ERP data for SA2 and FA regions.
    '''
    # Filter for ERP data at SA2 and FA levels
    df = df[(df['DataType'] == 'ERP') & (df['RegionType'].isin(['SA2', 'FA']))]

    results = {}
    for region_type in df['RegionType'].unique():
        region_df = df[df['RegionType'] == region_type]
        wide_df = region_df.pivot_table(index='ASGSCode', columns='Year', values='Total')
        wide_df = wide_df.dropna()

        # Store change points for each ASGSCode
        all_change_points = []

        for asgs_code in wide_df.index:
            time_series = wide_df.loc[asgs_code].values

            # Bayesian Change Point Detection
            time_series = time_series.reshape(-1, 1)  # Required shape for ruptures
            model = "l2"  # Model "l2" for mean shift
            algo = rpt.Pelt(model=model).fit(time_series)
            change_points_bayesian = algo.predict(pen=penalty)  # Penalty term to control sensitivity

            # Append detected change points
            all_change_points.append(change_points_bayesian)

        # Flatten list of change points and determine similarity
        flat_change_points = [item for sublist in all_change_points for item in sublist]
        change_points_counts = pd.Series(flat_change_points).value_counts()

        # Check for significant deviations
        significant_asgs_codes = [code for code, points in zip(wide_df.index, all_change_points) if len(points) >= similarity_threshold]

        if significant_asgs_codes:
            results[region_type] = {
                'num_series': len(wide_df),
                'significant_asgs_codes': significant_asgs_codes
            }
        else:
            results[region_type] = {
                'num_series': len(wide_df),
                'message': 'No significant anomalies detected in this region.'
            }

    # Print the results
    print("\n****************** CHECK: CHANGE POINT DETECTION ******************")
    for region_type, result in results.items():
        print(f"\nRegionType: {region_type}")
        print(f"Number of series: {result['num_series']}")
        if 'significant_asgs_codes' not in result:
            print(result['message'])
        else:
            print(f'Found {len(significant_asgs_codes)} ASGSCodes')

def detect_outliers_on_residuals(df):
    '''
    Perform outlier detection on time series residuals using ARIMA models.
    '''
    try:
        logging.info("Performing outlier detection on residuals...")
        
        # Filter for ERP data at SA2 and FA levels
        df = df[(df['DataType'] == 'ERP') & (df['RegionType'].isin(['SA2', 'FA']))]

        results = {}
        for region_type in df['RegionType'].unique():
            region_df = df[df['RegionType'] == region_type]
            wide_df = region_df.pivot_table(index='ASGSCode', columns='Year', values='Total')
            wide_df = wide_df.dropna()
            anomalies_found = False
            region_results = []

            for asgs_code in wide_df.index:
                time_series = wide_df.loc[asgs_code].values
                years = wide_df.columns
                time_series = pd.Series(time_series, index=years)    #create time series for each ASGSCode

                # Fit ARIMA model
                try:
                    model = ARIMA(time_series, order=(5, 1, 0))
                    fitted_model = model.fit()
                    residuals = fitted_model.resid
                    
                    # Calculate z-scores for residuals
                    residuals_z = zscore(residuals)
                    
                    # Detect outliers
                    outlier_indices = np.where(np.abs(residuals_z) > 3)[0]  # Outliers with z-score > 3
                    if len(outlier_indices) > 0:
                        anomalies_found = True
                        anomaly_years = years[outlier_indices]
                        region_results.append({
                            'ASGSCode': asgs_code,
                            'AnomalyYears': anomaly_years.tolist(),
                            'Residuals': residuals.tolist(),
                            'ResidualsZ': residuals_z.tolist()
                        })

                except Exception as e:
                    logging.error(f"Error fitting ARIMA model for ASGSCode {asgs_code}: {e}")

            if not anomalies_found:
                results[region_type] = {
                    'num_series': len(wide_df),
                    'message': 'No anomalies detected in this region.'
                }
            else:
                results[region_type] = {
                    'num_series': len(wide_df),
                    'anomalies': region_results
                }
        
        # Print the results
        print("\n****************** CHECK: OUTLIERS ON RESIDUALS ******************")
        for region_type, result in results.items():
            print(f"\nRegionType: {region_type}")
            print(f"Number of series: {result['num_series']}")
            if 'message' in result:
                print(result['message'])
            else:
                print(f"Anomalies Detected:")
                for anomaly in result['anomalies']:
                    print(f"ASGSCode: {anomaly['ASGSCode']}")
                    print(f"Anomaly Years: {anomaly['AnomalyYears']}")
                    
    except Exception as e:
        logging.error(f"Error performing outlier detection on residuals: {e}")
        raise

def main():
    try:
        # SQL Server connection
        sql_server_conn = connect_to_database()
        
        # Fetch data from SQL Server
        df = fetch_data_with_checks(sql_server_conn)
        # Perform checks
        perform_negative_checks(df)
        perform_sanity_checks(df)
        perform_outlier_checks(df)
        perform_ml_anomaly_detection(df)
        perform_bayesian_change_point_detection(df)
        detect_outliers_on_residuals(df)

        # Clean up connections
        sql_server_conn.close()
        logging.info("Database connections closed.")
    
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
