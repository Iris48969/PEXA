"""
file call the functions
"""

import pymssql
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
    from checks import first_check, second_check
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
    logging.info("Connection to database was done")
except:
    logging.error("Connection to database was failed")

# execute the checks 
try:
    logging.info("Try to execute first check")
    first_check(conn, parameter2=...)
    logging.info("first check done")
except Exception as e:
    logging.error(f"First check was failed: {e}")
    