
import logging
import platform

# set up basic configuration for logging
logging.basicConfig(
    filename='app.log',  # Log file name
    filemode='w',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("log set up done, start running the file")

# import check functions into main
try:
    from checks import household_check, check_population_household_ratio
except Exception as e:
    logging.error(e)

# connect to the database
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

# execute the checks 
if conn:
    try:
        logging.info("Try to execute first check")
        first_check(conn, parameter2=...)
        logging.info("First check done")
    except Exception as e:
        logging.error(f"First check failed: {e}")

    try:
        logging.info("Try to execute household check")
        outliers_df, unique_outlier_asgs_codes = household_check(conn)
        logging.info("Household check done")
    except Exception as e:
        logging.error(f"Household check failed: {e}")

    # Close the connection
    conn.close()
else:
    logging.error("Skipped checks because connection was not established.")
