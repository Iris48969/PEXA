"""
This file contains checks in the form of one function each that calls one SQL script
"""
import os
import pandas as pd
import logging

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