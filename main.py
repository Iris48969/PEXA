import logging
import platform
import pandas as pd
import re
import warnings
import time

warnings.simplefilter(action='ignore', category=UserWarning)

# Set up basic configuration for logging
logging.basicConfig(
    filename='app.log',  # Log file name
    filemode='w',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("Log set up done, start running the file")

start_time = time.time()

# Import the household_check function
try:
    from checks import household_check, births_region_level_sum_check, deaths_region_level_sum_check, household_region_level_sum_check, population_region_level_sum_check, trend_shape_check, spike_check, perform_negative_check, perform_sanity_check, perform_ml_anomaly_detection
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
        outliers_df = household_check(conn)
        logging.info("Household check completed successfully")
        # Optionally, you can log or save the results:
        # logging.info(f"Found {len(unique_outlier_asgs_codes)} unique outlier ASGS codes.")
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

try:
    logging.info("Running Negative Checks:")
    negative_checks = perform_negative_check(conn)
    if not negative_checks.empty:
        pass
            # print("Negative Checks:")
            # print(negative_checks)
except Exception as e:
    logging.error(f"Negative check failed: {e}")

try:
    logging.info("Running Sanity Checks:")
    sanity_checks = perform_sanity_check(conn)
    if not sanity_checks.empty:
        pass
            # print("Sanity Checks:")
            # print(sanity_checks)
except Exception as e:
    logging.error(f"Sanity check failed: {e}")

try:
    logging.info("Running Machine Learning Anomaly Detection:")
    ml_anomaly = perform_ml_anomaly_detection(conn)
    if not ml_anomaly.empty:
        pass
            # print("ML Anomaly Detection:")
            # print(ml_anomaly)
except Exception as e:
    logging.error(f"ML Anomaly Detection check failed: {e}")


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

# print(spike_output)

# print(shape_output)

checks_list = [outliers_df, births_check_output, deaths_check_output, household_check_output, population_check_output, negative_checks,ml_anomaly, spike_output,shape_output]

merged_df = pd.concat(checks_list, ignore_index=True)
merged_df = merged_df.sort_values(by=['Region Type', 'Code'], ascending=[False, True])
print(merged_df)
merged_df.to_csv('final_output.csv', index=False)

end_time = time.time()
running_time = end_time - start_time

print(f'The number of unique abnormal region are: {len(merged_df["Code"].unique())}') # something wrong with sanity check, without it only has 565 region been tagged
print(f"Running time: {running_time:.6f} seconds")
