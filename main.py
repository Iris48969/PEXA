import logging
import platform

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
    from checks import household_check
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

# Execute the checks 
if conn:
    try:
        logging.info("Trying to execute household check")
        outliers_df, unique_outlier_asgs_codes = household_check(conn)
        logging.info("Household check completed successfully")
        # Optionally, you can log or save the results:
        logging.info(f"Found {len(unique_outlier_asgs_codes)} unique outlier ASGS codes.")
    except Exception as e:
        logging.error(f"Household check failed: {e}")

    # Close the connection
    conn.close()
else:
    logging.error("Skipped checks because the connection was not established.")
