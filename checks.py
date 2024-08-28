"""
file contain checks as a format of function
"""
import pandas as pd
import os
import logging
import re
import numpy as np
def first_check(conn, parameter2):
    pass

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

    


