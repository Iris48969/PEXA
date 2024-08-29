import logging
import platform
import pandas as pd
import re

# Set up basic configuration for logging
logging.basicConfig(
    filename='app.log',  # Log file name
    filemode='w',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("Log set up done, start running the file")

# Import the household_check function
try:
    from checks import household_check, births_region_level_sum_check, deaths_region_level_sum_check, household_region_level_sum_check, population_region_level_sum_check, trend_shape_check, spike_check
except Exception as e:
    logging.error(f"Import failed: {e}")

# Connect to the database
try:
    current_system = platform.system()
    if current_system == "Darwin":
        import pymssql
        conn = pymssql.connect(
            server='localhost',
            user='sa',
            password='MBS_project_2024',
            database='forecasts',
            as_dict=True)

    elif current_system == "Windows":
        import pyodbc
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=localhost;'
                              'Database=forecasts;'
                              'UID=sa;'
                              'PWD=MBS_project_2024')

    else:
        logging.error(f"Running on an unsupported system: {current_system}")
    logging.info("Connection to database was successful")
except Exception as e:
    logging.error(f"Connection to database failed: {e}")
    conn = None

# household ratio check 
if conn:
    try:
        logging.info("Trying to execute household check")
        outliers_df, unique_outlier_asgs_codes = household_check(conn)
        logging.info("Household check completed successfully")
        # Optionally, you can log or save the results:
        logging.info(f"Found {len(unique_outlier_asgs_codes)} unique outlier ASGS codes.")
    except Exception as e:
        logging.error(f"Household check failed: {e}")
else:
    logging.error("Skipped checks because the connection was not established.")

    
# region level consistency check
try:
    logging.info("Try to execute births check")
    births_check_output = births_region_level_sum_check(conn)
    logging.info("Births check done")
except Exception as e:
    logging.error(f"Births check failed: {e}")

try:
    logging.info("Try to execute deaths check")
    deaths_check_output = deaths_region_level_sum_check(conn)
    logging.info("Deaths check done")
except Exception as e:
    logging.error(f"Deaths check failed: {e}")

try:
    logging.info("Try to execute household check")
    household_check_output = household_region_level_sum_check(conn)
    logging.info("Household check done")
except Exception as e:
    logging.error(f"Household check failed: {e}")

try:
    logging.info("Try to execute population check")
    population_check_output = population_region_level_sum_check(conn)
    logging.info("Population check done")
except Exception as e:
    logging.error(f"Population check failed: {e}")

# pattern check 

try:
    logging.info("Try to execute spike check")
    spike_output = spike_check(conn) # so far filter out 327 region
    logging.info("spike check done")
    logging.info("Try to execute shape check")
    shape_output = trend_shape_check(conn) # so far filter out 360 region
    logging.info("shape check done")
except Exception as e:
    logging.error(e)

