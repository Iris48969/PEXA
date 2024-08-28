"""
file call the functions
"""
import logging
import platform
import re

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
    from checks import first_check, trend_shape_check, spike_check
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
    logging.info("Try to execute spike check")
    result = spike_check(conn) # so far filter out 375 region
    logging.info("spike check done")
    logging.info("Try to execute shape check")
    result += trend_shape_check(conn) # so far filter out 408 region
    logging.info("shape check done")
except Exception as e:
    logging.error(e)

# reformat output (put overlapping region together)
try:
    output_dict = {}
    for c, t, d in result:
        if c not in output_dict.keys():
            output_dict[c] = [t, [d]]
        else:
            output_dict[c] = [t, output_dict[c][1]+[d]]
    logging.info("reformat the output as dictionary")
except Exception as e:
    logging.error(e)

# print(output_dict[41032290])
# print(len(output_dict.keys()))