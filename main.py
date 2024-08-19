import pandas as pd
import numpy as np
import platform

current_system = platform.system()
if current_system == "Darwin":
    import pymssql
    connection = pymssql.connect(
    server='localhost',
    user='sa',
    password='MBS_project_2024',
    database='forecasts',
    as_dict=True)

elif current_system == "Windows":
    import pyodbc
    connection = pyodbc.connect('Driver={SQL Server};'
                                'Server=localhost;'
                                'Database=forecasts;'
                                'UID=sa;'
                                'PWD=MBS_project_2024')

else:
    print(f"Running on an unsupported system: {current_system}")

# Create cursor
# cursor = connection.cursor()


# Query database
# cursor.execute("SELECT ASGSCode FROM AreasAsgs where parent = 401 and ASGS = 2021")
# for row in cursor.fetchall():
#     print(row.ASGSCode)



def pattern_check(conn):
    '''
    Imput: connection to database
    output: list of region code that is flagged as unusual
    perform 2 checks at the same time:

    check 1: find the absolute difference between the ERP at a particular year 
    and the average ERP of the previous and later year, when the maximum absolute 
    difference is greater than 0.1, it is flaged as abnormal

    check 2: find the smoothness strength using long run short run variance ratio,
    flag if the maximum variance ratio is greater than 70. Also, perform sign check
    that flag region is there are change of trend of ERP (increasing trend follow by a 
    decreasing in ERP, vice verse)

    Output will be the region code that did not pass either check 1 or check 2
    '''

    # extract data table from database
    SQL_QUERY = """SELECT ASGS_2016, SUM([Number]) as ERP, ERPYear
    FROM [forecasts].[dbo].[ERP] e
    left join [forecasts].[dbo].[AreasAsgs] a
    on e.ASGS_2016 = a.ASGSCode
    where a.[RegionType] = 'SA2'
    group by ASGS_2016, ERPYear
    """
    cursor = conn.cursor()
    cursor.execute(SQL_QUERY)
    data = cursor.fetchall()
    df = pd.DataFrame(data)
    wide_df = df.pivot_table(index='ASGS_2016', columns='ERPYear', values='ERP')

    # check 1
    abnormal_list1 = []
    for idx, value in wide_df.iterrows():
        standar_data = list((value - np.mean(value))/np.std(value))
        years = list(wide_df.columns)
        diff_list = []
        for i in range(1, len(standar_data) -1):
            neighbor_avg = (standar_data[i-1] + standar_data[i+1]) / 2
            diff = abs(standar_data[i] - neighbor_avg)
            diff_list.append(abs(diff))
        if not max(diff_list) > 0.1: continue
        # print(f'for {idx}')
        # plt.plot(years, standar_data)
        # plt.show()
        abnormal_list1.append(idx)

    # check 2
    abnormal_list2 = []
    def all_same_sign(numbers):
        if all(x >= 0 for x in numbers) or all(x <= 0 for x in numbers):
            return True
        else:
            return False
    
    for idx, value in wide_df.iterrows():
        standar_data = list((value - np.mean(value))/np.std(value))
        years = list(wide_df.columns)
        diff_value = np.diff(standar_data)
        ts = pd.Series(standar_data, years)
        short_run_window = 5
        long_run_window = 15
        short_run_variance = ts.rolling(window=short_run_window).var()
        # Calculate Long-Run Variance
        long_run_variance = ts.rolling(window=long_run_window).var()
        # Calculate the ratio of long-run variance to short-run variance
        variance_ratio = long_run_variance / short_run_variance
        # Drop NaN values caused by rolling operations
        variance_ratio = variance_ratio.dropna()
        if not variance_ratio.any(): continue
        if max(variance_ratio) <= 70 and all_same_sign(diff_value): continue
        abnormal_list2.append(idx)
        # plt.plot(years, standar_data)
        # plt.show()
    
    unique_list = list(set(abnormal_list1 + abnormal_list2))

    print(f'ASGSCode that did no pass first check are {abnormal_list1}')
    print(f'ASGSCode that did no pass second check are {abnormal_list2}')
    print(f'ASGSCode that did no pass either one or both of the checks:  {unique_list}')

pattern_check(connection)
# Close connection
connection.close()