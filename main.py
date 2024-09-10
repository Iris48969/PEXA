import logging
import platform
import pandas as pd
import re
import warnings
import time
import sqlite3
from datetime import datetime
import os



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
    from checks import add_name_column, household_check, births_region_level_sum_check, deaths_region_level_sum_check, household_region_level_sum_check, population_region_level_sum_check, trend_shape_check, spike_check, perform_negative_check, perform_sanity_check, perform_ml_anomaly_detection
    from parameter_window import open_parameter_window
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
                            'Server=GDANSK;'
                            'Database=SafiTopsDown_Transformed;'
                            'Trusted_Connection=yes')
        
        """
        'Driver={SQL Server};'
                              'Server=localhost;'
                              'Database=forecasts;'
                              'UID=sa;'
                              'PWD=MBS_project_2024'
        """

    else:
        logging.error(f"Running on an unsupported system: {current_system}")
    logging.info("Connection to database was successful")
except Exception as e:
    logging.error(f"Connection to database failed: {e}")
    conn = None

# get input parameters
try:
    ratio_upper, ratio_lower, multiplier, sensitivity, contamination, sa4_code = open_parameter_window()
    logging.info("Got inputted parameters")
except Exception as e:
    logging.error(e)

# household ratio check 
if conn:
    try:
        logging.info("Trying to execute household check")
        ratio_df = household_check(conn, ratio_upper, ratio_lower, sa4_code)
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
    births_check_output = births_region_level_sum_check(conn, sa4_code)
    logging.info("Births check done")
except Exception as e:
    logging.error(f"Births check failed: {e}")

try:
    logging.info("Try to execute deaths check")
    deaths_check_output = deaths_region_level_sum_check(conn, sa4_code)
    logging.info("Deaths check done")
except Exception as e:
    logging.error(f"Deaths check failed: {e}")

try:
    logging.info("Try to execute household check")
    household_check_output = household_region_level_sum_check(conn, sa4_code)
    logging.info("Household check done")
except Exception as e:
    logging.error(f"Household check failed: {e}")

try:
    logging.info("Try to execute population check")
    population_check_output = population_region_level_sum_check(conn, sa4_code)
    logging.info("Population check done")
except Exception as e:
    logging.error(f"Population check failed: {e}")

try:
    logging.info("Running Negative Checks:")
    negative_checks = perform_negative_check(conn, sa4_code)
except Exception as e:
    logging.error(f"Negative check failed: {e}")

try:
    logging.info("Running Sanity Checks:")
    sanity_checks = perform_sanity_check(conn, sa4_code)
except Exception as e:
    logging.error(f"Sanity check failed: {e}")

try:
    logging.info("Running Machine Learning Anomaly Detection:")
    ml_anomaly = perform_ml_anomaly_detection(conn, contamination, sa4_code)
except Exception as e:
    logging.error(f"ML Anomaly Detection check failed: {e}")


# pattern check 

try:
    logging.info("Try to execute spike check")
    spike_output, total_number_of_region = spike_check(conn, sensitivity, multiplier, sa4_code) # so far filter out 327 region
    logging.info("spike check done")
    logging.info("Try to execute shape check")
    shape_output = trend_shape_check(conn, sensitivity, sa4_code) # so far filter out 360 region
    logging.info("shape check done")
except Exception as e:
    logging.error(e)

# print(spike_output)
# print(shape_output)
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
folder_name = f"output_{current_time}"
os.makedirs(folder_name, exist_ok=True)
output_summary_path = os.path.join(folder_name, 'output_summary.txt')
output_csv_path = os.path.join(folder_name, 'final_output.csv')



# merge result together and output a csv file
output_list = [ratio_df, sanity_checks, births_check_output, deaths_check_output, population_check_output, household_check_output, negative_checks,ml_anomaly, spike_output,shape_output]
merged_df = pd.concat(output_list, ignore_index=True)
merged_df = merged_df.sort_values(by=['Region Type', 'Code'], ascending=[False, True])
merged_df = add_name_column(merged_df, conn, sa4_code)
# print(merged_df)
merged_df.to_csv(output_csv_path, index=False)

# summary stat 
end_time = time.time()
running_time = end_time - start_time
percentage = (len(merged_df["Code"].unique()) / total_number_of_region) * 100
formatted_percentage = "{:.2f}%".format(percentage)

# Function to safely get the number of unique values in the first column of a DataFrame
def get_unique_count(df):
    if df is not None and not df.empty:
        return len(df.iloc[:, 0].unique())
    return 0

with open(output_summary_path, 'w') as file:
    # Parameters section
    file.write(f"{'#' * 45}\n")
    file.write(f"{' ' * 15}PARAMETERS\n")
    file.write(f"{'#' * 45}\n\n")
    file.write(f"Ratio Upper:      {ratio_upper}\n")
    file.write(f"Ratio Lower:      {ratio_lower}\n")
    file.write(f"Multiplier:       {multiplier}\n")
    file.write(f"Sensitivity:      {sensitivity}\n")
    file.write(f"Contamination:    {contamination}\n")
    file.write(f"SA4 Code:         {sa4_code}\n\n")
    
    # Summary statistics section
    file.write(f"{'#' * 45}\n")
    file.write(f"{' ' * 12}SUMMARY STATISTICS\n")
    file.write(f"{'#' * 45}\n\n")
    file.write(f"Unique Abnormal Regions:    {len(merged_df['Code'].unique())}\n")
    file.write(f"Proportion of Abnormal Regions:    {formatted_percentage}\n")
    file.write(f"Total Running Time:         {running_time:.6f} seconds\n\n")
    
    # Sanity checks section
    file.write(f"{'#' * 45}\n")
    file.write(f"{' ' * 14}SANITY CHECKS\n")
    file.write(f"{'#' * 45}\n\n")
    sanity_checks_len = get_unique_count(sanity_checks)
    file.write(f"Sanity Check Tagged Regions:    {sanity_checks_len}\n")
    negative_checks_len = get_unique_count(negative_checks)
    file.write(f"Negative Checks Tagged Regions: {negative_checks_len}\n\n")
    
    # Population / Household check section
    file.write(f"{'#' * 45}\n")
    file.write(f"{' ' * 10}POPULATION / HOUSEHOLD CHECKS\n")
    file.write(f"{'#' * 45}\n\n")
    ratio_checks_len = get_unique_count(ratio_df)
    file.write(f"Ratio Check Tagged Regions:     {ratio_checks_len}\n\n")
    
    # Region level consistency checks section
    file.write(f"{'#' * 45}\n")
    file.write(f"{' ' * 10}REGION LEVEL CONSISTENCY CHECKS\n")
    file.write(f"{'#' * 45}\n\n")
    births_check_len = get_unique_count(births_check_output)
    file.write(f"Births Check Tagged Regions:     {births_check_len}\n")
    deaths_check_len = get_unique_count(deaths_check_output)
    file.write(f"Deaths Check Tagged Regions:     {deaths_check_len}\n")
    household_check_len = get_unique_count(household_check_output)
    file.write(f"Household Check Tagged Regions:  {household_check_len}\n")
    population_check_len = get_unique_count(population_check_output)
    file.write(f"Population Check Tagged Regions: {population_check_len}\n\n")
    
    # Pattern checks section
    file.write(f"{'#' * 45}\n")
    file.write(f"{' ' * 15}PATTERN CHECKS\n")
    file.write(f"{'#' * 45}\n\n")
    ml_anomaly_len = get_unique_count(ml_anomaly)
    file.write(f"ML Anomaly Check Tagged Regions: {ml_anomaly_len}\n")
    spike_output_len = get_unique_count(spike_output)
    file.write(f"Spike Check Tagged Regions:      {spike_output_len}\n")
    shape_output_len = get_unique_count(shape_output)
    file.write(f"Shape Check Tagged Regions:      {shape_output_len}\n")



