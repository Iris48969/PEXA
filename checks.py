"""
This file contains checks in the form of one function each that calls one SQL script
"""
from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
import os
import logging
import re
import numpy as np
import platform
from sklearn.ensemble import IsolationForest
import warnings

# Ignore SettingWithCopyWarning
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)


def execute_sql_query(conn, sql_query):
    try:
        current_system = platform.system()
        if current_system == "Darwin":
            # execute queries
            cursor = conn.cursor()
            cursor.execute(sql_query)
            data = cursor.fetchall()
            df = pd.DataFrame(data)
        elif current_system == "Windows":
            df = pd.read_sql_query(con=conn, sql= sql_query)
        logging.info("sql execute done")
        return df
    except Exception as e:
        logging.error(e)

def household_check(conn):
    """
    The purpose of this function is to identify abnormal spikes/drops in population forecasts
    in a timeseries format by checking the ratio of population to household count.
    input: connection
    output: list of lists containing [ASGSCode, Region Type, Description (earliest year an outlier appears)]
    """
    try:
        # Read the SQL query from the file
        ratio_sql = open(os.path.abspath("SQL_Queries/household_size.sql"), 'r').read()
        # Execute the SQL query and get the merged dataframe
        merged_df = execute_sql_query(conn=conn, sql_query=ratio_sql)
        logging.info("Data returned")
        outliers_df = merged_df[(merged_df['ratio'] >= 6) | (merged_df['ratio'] <= 1)]
        logging.info("Outlier dataframe found")
        earliest_outliers_df = outliers_df.groupby('ASGSCode').apply(
            lambda x: x.loc[x['Year'].idxmin()]
        ).reset_index(drop=True)
        logging.info("earliest_year")
        output = earliest_outliers_df[['ASGSCode', 'region_type']].copy()
        output['Description'] = "Found abnormal ERP/household ratio, Earliest abnormal year is: " + earliest_outliers_df['Year'].astype(str)
        output = output.rename(columns={'region_type': 'Region Type', 'ASGSCode': 'Code'})
        return output
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return None

def births_region_level_sum_check(conn):
    """
    The purpose of this function is to call an SQL script which sums the births of all FAs in a particular SA2 and checks
    if this value matches the total births of the SA2

    Input: Connection
    Output: Table which contains information of SA2s that failed the check (Code | Region Type | Description)
    """
    try:
        sql = open(os.path.abspath("SQL_Queries/Births Sum Check for FA vs SA2 Output.sql"), 'r').read()
        logging.info("Births region level sum check query opened")
        df = execute_sql_query(sql_query=sql, conn=conn)
        logging.info("Births region level sum check query executed")
        return df
    except Exception as e:
        logging.error(e)

def deaths_region_level_sum_check(conn):
    """
    The purpose of this function is to call an SQL script which sums the deaths of all FAs in a particular SA2 and checks
    if this value matches the total births of the SA2

    Input: Connection
    Output: Table which contains information of SA2s that failed the check (Code | Region Type | Description)
    """
    try:
        sql = open(os.path.abspath("SQL_Queries/Deaths Sum Check for FA vs SA2 Output.sql"), 'r').read()
        logging.info("Deaths region level sum check query opened")
        df = execute_sql_query(sql_query=sql, conn=conn)
        logging.info("Deaths region level sum check query executed")
        return df
    except Exception as e:
        logging.error(e)

def household_region_level_sum_check(conn):
    """
    The purpose of this function is to call an SQL script which sums the households of all FAs in a particular SA2 and checks
    if this value matches the total births of the SA2

    Input: Connection
    Output: Table which contains information of SA2s that failed the check (Code | Region Type | Description)
    """
    try:
        sql = open(os.path.abspath("SQL_Queries/Household Sum Check for FA vs SA2 Output.sql"), 'r').read()
        logging.info("Household region level sum check query opened")
        df = execute_sql_query(sql_query=sql, conn=conn)
        logging.info("Household region level sum check query executed")
        return df
    except Exception as e:
        logging.error(e)

def population_region_level_sum_check(conn):
    """
    The purpose of this function is to call an SQL script which sums the population of all FAs in a particular SA2 and checks
    if this value matches the total births of the SA2

    Input: Connection
    Output: Table which contains information of SA2s that failed the check (Code | Region Type | Description)
    """
    try:
        sql = open(os.path.abspath("SQL_Queries/Population Sum Check for FA vs SA2 Output.sql"), 'r').read()
        logging.info("Population region level sum check query opened")
        df = execute_sql_query(sql_query=sql, conn=conn)
        logging.info("Population region level sum check query executed")
        return df
    except Exception as e:
        logging.error(e)

def spike_check(conn):
    """
    The purpose of this function is to identify abnormal spike/drop of population forecast
    in a timeseries format.

    input: connection
    output: list of list which contains [region code, region type, description]
    """
    try:
        # getting queries
        ERP_query= open(os.path.abspath("SQL_Queries/ERP_table(FA&SA2).sql"),'r').read()
        area_type_query = open(os.path.abspath("SQL_Queries/Area_type.sql"),'r').read()
        logging.info("Query file opened")
        df = execute_sql_query(conn=conn, sql_query=ERP_query)
        wide_df = df.pivot_table(index='ERPYear', columns='ASGS_2016', values='ERP')
        area_type = execute_sql_query(conn=conn, sql_query=area_type_query)
        area_dict = area_type.set_index('ASGSCode').to_dict()['RegionType']
        logging.info("Query data returned")

        def check_outliers(data_list):
            """
            This function determine if there are outliers from the input data list.
            the rule of outlier is if the growth of population is more 5*IQR away from
            Q1 and Q3 + if the growth rate is more than 0.005 or less than 0.005 (ignoring
            small changes)

            input: list of digit
            output: True if there are outliers and False if there are not outliers 
            """
            Q1 = np.percentile(data_list, 25)
            Q3 = np.percentile(data_list, 75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 5 * IQR
            upper_bound = Q3 + 5 * IQR
            for data in data_list:
                if abs(data) > 0.005:
                    if data > upper_bound or data < lower_bound: return True
            return False
        # creating a percentage change df for ERP
        rate_of_change = wide_df.pct_change()
        rate_of_change = rate_of_change.drop(rate_of_change.index[0])

        output_list = []

        # perform checks and output result
        for region in wide_df.columns:
            if check_outliers(rate_of_change[region][1:]):
                output_list.append([region, area_dict[region], "Suddent spike or drop detected"])
        output_df = pd.DataFrame(output_list, columns=['Code', 'Region Type', 'Description'])
        return output_df
    except Exception as e:
        logging.error(e)

def trend_shape_check(conn):
    """
    The purpose of this function is to identify abnormal shape of population forecast
    in a timeseries format.

    input: connection
    output: list of list which contains [region code, region type, description]
    """
    try:
        # getting queries
        ERP_query= open(os.path.abspath("SQL_Queries/ERP_table(FA&SA2).sql"),'r').read()
        area_type_query = open(os.path.abspath("SQL_Queries/Area_type.sql"),'r').read()
        logging.info("Query file opened")

        # execute queries
        # cursor = conn.cursor()
        # cursor.execute(ERP_query)
        # data = cursor.fetchall()
        # df = pd.DataFrame(data)
        df = execute_sql_query(conn=conn, sql_query=ERP_query)
        wide_df = df.pivot_table(index='ERPYear', columns='ASGS_2016', values='ERP')
        # cursor.execute(area_type_query)
        # area_type = cursor.fetchall()
        # area_type = pd.DataFrame(area_type)
        area_type = execute_sql_query(conn=conn, sql_query=area_type_query)
        area_dict = area_type.set_index('ASGSCode').to_dict()['RegionType']
        logging.info("Query data returned")

        # creating a percentage change df for ERP
        rate_of_change = wide_df.pct_change()
        rate_of_change = rate_of_change.drop(rate_of_change.index[0])

        def encode_change(data_list):
            """
            This function is used to convert the timeseries data into a string containing
            "+", "-" and "0". 
            for example, if the data_list is [1,0,-1], it will be convered to "+0-"
            if data is > 0.005, it will be convert to "+", if data is < -0.005, it will
            be convert to "-", if it is inbetween it will be convered to "0"
            """
            output_string = ""
            for data in data_list:
                if data > 0.005: output_string += "+"
                elif data < -0.005: output_string += "-"
                else: output_string += "0"
            return output_string

        def find_abnormal_shape_encode(encode_string):
            """
            This function is used to find abnormal shape of trend through the encode string.
            For example, if the encode string contains +0- or -0+, it might indicate a small
            bell curve:

            Rules:
            +0- or -o+ ==> small bell curve
            +0(more than 1)- or -0(more than 1)+ ==> bell curve
            +-+ or -+- ==> wave/cycle
            +- ==> straight change of sign of growth of population
            """

            if re.search(r'\+0-', encode_string) or re.search(r'-0\+', encode_string):
                return True, "small bell curve detect"
            elif re.search(r'\+0+-', encode_string) or re.search(r'-0+\+', encode_string):
                return True, "Bell curve detect"
            elif re.search(r'\+-\+', encode_string) or re.search(r'-\+-', encode_string):
                return True, "wave detect"
            elif re.search(r'\+-', encode_string) or re.search(r'-\+', encode_string):
                return True, "straight change of sign detect"
            else: return False, "no message"

        output_list = []

        # perform checks and output result
        for region in wide_df.columns:
            encoded_string = encode_change(rate_of_change[region].values)
            is_abnormal, message = find_abnormal_shape_encode(encode_string = encoded_string)
            if is_abnormal:
                # if message == "small bell curve detect" : des = message 
                # elif message == "Bell curve detect": des = message 
                # elif message == "wave detect": des = message 
                # else: des = message
                output_list.append([region, area_dict[region], message])
        output_pd = pd.DataFrame(output_list, columns=['Code', 'Region Type', 'Description'])
        return output_pd
    except Exception as e:
        logging.error(e)


# Negative Check Function
def perform_negative_check(conn):
    '''
    This function checks for negative values in the Births, Deaths, and ERP tables.
    Returns a DataFrame with columns: Code, Region Type, Description.
    '''
    try:
        logging.info("Performing negative checks...")
        
        # Fetch query from the SQL file
        with open('SQL_Queries/Negative_Sanity_ML_Check.sql', 'r') as file:
            query = file.read()
        
        # Fetch data from the database
        df = execute_sql_query(sql_query=query, conn=conn)

        result_list = []

        for region_type in ['FA', 'SA2']:
            # Filter the dataframe for the current region type
            region_df = df[df['RegionType'] == region_type]
            
            # Check for negative values
            negative_births = region_df[(region_df['DataType'] == 'Births') & (region_df['Total'] < -10)]
            if not negative_births.empty:
                for code in negative_births['ASGSCode'].unique():
                    result_list.append({'Code': code, 'Region Type': region_type, 'Description': 'Negative Births Values Found'})

            negative_deaths = region_df[(region_df['DataType'] == 'Deaths') & (region_df['Total'] < -10)]
            if not negative_deaths.empty:
                for code in negative_deaths['ASGSCode'].unique():
                    result_list.append({'Code': code, 'Region Type': region_type, 'Description': 'Negative Deaths Values Found'})

            negative_erp = region_df[(region_df['DataType'] == 'ERP') & (region_df['Total'] < -10)]
            if not negative_erp.empty:
                for code in negative_erp['ASGSCode'].unique():
                    result_list.append({'Code': code, 'Region Type': region_type, 'Description': 'Negative ERP Values Found'})

        return pd.DataFrame(result_list, columns=['Code', 'Region Type', 'Description'])
    except Exception as e:
        logging.error(f"Error performing negative checks: {e}")
        return pd.DataFrame(columns=['Code', 'Region Type', 'Description'])  # Return DataFrame with correct columns on error


def perform_sanity_check(conn):
    '''
    This function performs checks for zero values, missing values, and duplicate values in Births, Deaths, ERP.
    Returns a DataFrame with columns: Code, Region Type, Description.
    '''
    try:
        logging.info("Performing sanity checks...")

        # Fetch query from the SQL file
        with open('SQL_Queries/Negative_Sanity_ML_Check.sql', 'r') as file:
            query = file.read()

        # Fetch data from the database
        df = execute_sql_query(sql_query=query, conn=conn)

        result_list = []

        for region_type in ['FA', 'SA2']:
            region_df = df[df['RegionType'] == region_type]
            
            # # Check for zero values
            # zero_births = region_df[(region_df['DataType'] == 'Births') & (region_df['Total'] == 0)]
            # if not zero_births.empty:
            #     for code in zero_births['ASGSCode'].unique():
            #         result_list.append({'Code': code, 'Region Type': region_type, 'Description': 'Zero Births Values Found'})

            # zero_deaths = region_df[(region_df['DataType'] == 'Deaths') & (region_df['Total'] == 0)]
            # if not zero_deaths.empty:
            #     for code in zero_deaths['ASGSCode'].unique():
            #         result_list.append({'Code': code, 'Region Type': region_type, 'Description': 'Zero Deaths Values Found'})

            # zero_population = region_df[(region_df['DataType'] == 'ERP') & (region_df['Total'] == 0)]
            # if not zero_population.empty:
            #     for code in zero_population['ASGSCode'].unique():
            #         result_list.append({'Code': code, 'Region Type': region_type, 'Description': 'Zero Population Values Found'})

            # Check for missing values
            missing_values = region_df.isnull().sum()
            if missing_values.any():
                result_list.append({'Code': 'All', 'Region Type': region_type, 'Description': 'Missing Values Detected'})

            # Check for duplicate records
            duplicates = region_df[region_df.duplicated()]
            if not duplicates.empty:
                for code in duplicates['ASGSCode'].unique():
                    result_list.append({'Code': code, 'Region Type': region_type, 'Description': 'Duplicate Records Found'})

        return pd.DataFrame(result_list, columns=['Code', 'Region Type', 'Description'])
    except Exception as e:
        logging.error(f"Error performing sanity checks: {e}")
        return pd.DataFrame(columns=['Code', 'Region Type', 'Description'])  # Return DataFrame with correct columns on error



# Machine Learning Anomaly Detection Function, 2021 Census
def perform_ml_anomaly_detection(conn):
    try:
        logging.info("Performing machine learning anomaly detection...")
        query = open(os.path.abspath('SQL_Queries/ERP_ML.sql'), 'r').read()
        df = execute_sql_query(sql_query=query, conn=conn)
        result_list = []

        def outlier_plot(data, outlier_method_name, x_var, y_var,region_type):
            # print(f'Outlier Method: {outlier_method_name}')
           
            # print(f'Number of anomalous values: {len(data[data["anomaly"] == -1])}')
            # print(f'Number of non-anomalous values: {len(data[data["anomaly"] == 1])}')
            # print(f'Total Number of values: {len(data)}')

            # Create FacetGrid
            g = sns.FacetGrid(data, col='anomaly', height=4, hue='anomaly', hue_order=[1, -1], palette={1: 'green', -1: 'red'})
            g.map(sns.scatterplot, x_var, y_var, s = 20, legend='full')
            g.fig.suptitle(f'{outlier_method_name} - {region_type}', y=1.10, fontweight='bold')
            
            # Set limits for x and y axes 
            g.set(xlim=(data[x_var].min(), data[x_var].max()), ylim=(0, data[y_var].max() + 1000))

            # Customize subplot titles
            axes = g.axes.flatten()
            axes[0].set_title(f'Outliers in {region_type}\n{len(data[data["anomaly"] == -1])} points')
            axes[1].set_title(f'Inliers in {region_type}\n{len(data[data["anomaly"] == 1])} points')

            for ax in axes:
                ax.set_xticks(data[x_var].unique())
                ax.set_xticklabels(data[x_var].unique(), rotation=45, ha='right')
           # Add legend 
            for ax in g.axes.flat:
                handles, labels = ax.get_legend_handles_labels()
                ax.legend(handles, [f'{label} ({region_type})' for label in labels], title='Anomaly Status')

            plt.tight_layout()
            plt.show()

            return g

        for region_type in ['FA', 'SA2']:
            region_df = df[df['RegionType'] == region_type]
            
            # Ensure ASGSCode is present
            if 'ASGSCode' not in region_df.columns:
                logging.error("Column 'ASGSCode' is missing from the DataFrame")
                return pd.DataFrame(columns=['Code', 'Region Type', 'Description'])

            # Prepare input features
            anomaly_inputs = ['ERPYear', 'Total']
            
            # Initialize and fit the Isolation Forest model
            model_IF = IsolationForest(contamination=0.003, random_state=42)
            region_df['anomaly_scores'] = model_IF.fit_predict(region_df[anomaly_inputs])
            region_df['anomaly'] = model_IF.predict(region_df[anomaly_inputs])
            # Manually set negative values as outliers
            region_df.loc[region_df['Total'] < 0, 'anomaly'] = -1
            
            # Plot results
            #outlier_plot(region_df, "Isolation Forest", "ERPYear", "Total", region_type)
            # Append results to the result list
            for index, row in region_df.iterrows():
                if row['anomaly'] == -1:
                    result_list.append({'Code': row['ASGSCode'], 'Region Type': region_type, 'Description': 'Machine Learning Anomaly Detected'})
        
        # Convert result_list to DataFrame
        result_df = pd.DataFrame(result_list, columns=['Code', 'Region Type', 'Description'])
        return result_df
      
    except Exception as e:
        logging.error(f"Error performing machine learning anomaly detection: {e}")
        return pd.DataFrame(columns=['Code', 'Region Type', 'Description'])
    
