"""
This file contains checks in the form of one function each that calls one SQL script
"""
import pandas as pd
import os
import logging
import re
import numpy as np

def household_check(conn):
    """
    The purpose of this function is to identify abnormal spikes/drops in population forecasts
    in a timeseries format by checking the ratio of population to household count.

    input: connection
    output: list of lists containing [region code, region type, description]
    """

    try:
        # Getting queries
        population_query = open(os.path.abspath("SQL_Queries/Population_number.sql"), 'r').read()
        household_query = open(os.path.abspath("SQL_Queries/Household_number.sql"), 'r').read()
        logging.info("Query file opened")

        # Execute queries
        cursor = conn.cursor()
        cursor.execute(population_query)
        pop_data = cursor.fetchall()
        pop_df = pd.DataFrame([tuple(row) for row in pop_data], columns=['ASGSCode', 'Year', 'Pop_Number'])
        logging.info("population data returned")

        cursor.execute(household_query)
        household_data = cursor.fetchall()
        household_df = pd.DataFrame([tuple(row) for row in household_data], columns=['ASGSCode', 'Year', 'Household_Number'])
        logging.info("household data returned")

        merged_df = pd.merge(pop_df, household_df, on=['ASGSCode', 'Year'], how='inner')
        logging.info("Merge of datafram done")

        # Calculate the ratio of Population to HouseholdCount
        merged_df['Ratio'] = merged_df['Pop_Number'] / merged_df['Household_Number']
        
        # Apply the filtering conditions
        outliers_df = merged_df[(merged_df['Ratio'] >= 5) | (merged_df['Ratio'] <= 1)]
        logging.info("Outlier dataframe found")

        # Get unique ASGSCode values that need to be checked
        unique_outlier_asgs_codes = outliers_df['ASGSCode'].unique()
        return outliers_df, unique_outlier_asgs_codes
    except Exception as e:
        logging.error(f"Error occurred: {e}")

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
        df = pd.read_sql_query(sql, conn)
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
        df = pd.read_sql_query(sql, conn)
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
        df = pd.read_sql_query(sql, conn)
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
        df = pd.read_sql_query(sql, conn)
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

        # execute queries
        cursor = conn.cursor()
        cursor.execute(ERP_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        wide_df = df.pivot_table(index='ERPYear', columns='ASGS_2016', values='ERP')
        cursor.execute(area_type_query)
        area_type = cursor.fetchall()
        area_type = pd.DataFrame(area_type)
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
        return output_list
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
        cursor = conn.cursor()
        cursor.execute(ERP_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        wide_df = df.pivot_table(index='ERPYear', columns='ASGS_2016', values='ERP')
        cursor.execute(area_type_query)
        area_type = cursor.fetchall()
        area_type = pd.DataFrame(area_type)
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
        return output_list
    except Exception as e:
        logging.error(e)

    