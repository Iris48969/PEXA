"""
file contain checks as a format of function
"""
import pandas as pd
import os
import logging
import re
import numpy as np

def household_check(conn):
    pass

def household_check(conn):
    """
    The purpose of this function is to identify abnormal spike/drop of population forecast
    in a timeseries format.

    input: connection
    output: list of list which contains [region code, region type, description]
    """
    try:
        # getting queries
        population_query= open(os.path.abspath("SQL_Queries/population_number.sql"),'r').read()
        household_query = open(os.path.abspath("SQL_Queries/Household_type.sql"),'r').read()
        logging.info("Query file opened")

        # execute queries
        cursor = conn.cursor()
        cursor.execute(population_query)
        pop_data = cursor.fetchall()
        pop_df = pd.DataFrame([tuple(row) for row in pop_data], columns=['ASGSCode', 'Year', 'Pop_Number'])

        cursor.execute(household_query)
        household_data = cursor.fetchall()
        household_df = pd.DataFrame([tuple(row) for row in household_data], columns=['ASGSCode', 'Year', 'Household_Number'])
        

