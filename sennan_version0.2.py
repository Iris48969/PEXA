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

SQL_QUERY = {
    "ERP_table": """SELECT ASGS_2016, SUM([Number]) as ERP, ERPYear
    FROM [forecasts].[dbo].[ERP] e
    left join [forecasts].[dbo].[AreasAsgs] a
    on e.ASGS_2016 = a.ASGSCode
    where a.[RegionType] = 'FA'
    group by ASGS_2016, ERPYear
    """,
    "household_table": """
    SELECT h.ASGSCode, h.ERPYear, h.Number
    from Households h
    left join [forecasts].[dbo].[AreasAsgs] a
    on h.ASGSCode = a.ASGSCode
    where a.[RegionType] = 'FA'
    """
}

cursor = connection.cursor()
cursor.execute(SQL_QUERY['ERP_table'])
data = cursor.fetchall()
df = pd.DataFrame(data)
# print(df)

wide_df = df.pivot_table(index='ERPYear', columns='ASGS_2016', values='ERP')
# print(wide_df)

cursor.execute(SQL_QUERY['household_table'])
data_household = cursor.fetchall()
df_household = pd.DataFrame(data_household)
# print(df)

wide_df_household = df_household.pivot_table(index='ERPYear', columns='ASGSCode', values='Number')

###############################################   abnormal drop ##########################################################

def find_upper_lower_bound(data):
    Q1 = np.percentile(data, 25)
    Q3 = np.percentile(data, 75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return (lower_bound, upper_bound)

# sign check
FA_with_popul_decr = []
number_flagged = 0
total_number = 0

change_popul_df = wide_df.diff().dropna()
change_pop_df_2year = wide_df.diff(periods=2).dropna()
decres_pop = [change for change in change_popul_df.min().values if change < 0]
decres_pop_2year = [change for change in change_pop_df_2year.min().values if change < 0]
# print(f"The number of FA region been decreasing population are: {len(decres_pop_2year)}")
# print(f"Total number of FA region: {len(change_popul_df.columns)}")

# plt.boxplot([decres_pop,decres_pop_2year], labels=["1 year", "2 year"])
# plt.show()
# print(f"The bound is: {find_upper_lower_bound(decres_pop)}")
# print(f"The bound is: {find_upper_lower_bound(decres_pop_2year)}")

# filtering out region with 10 ppl decrease within 1 year and 20 within 2 years
filtered_list_pop_decreas = []
for region in wide_df.columns:
    minimum_pop_growth_1_year = min(change_popul_df[region])
    # minimum_pop_growth_2_year = min(change_pop_df_2year[region])

    if minimum_pop_growth_1_year < -10:
        year_min = change_popul_df.index[change_popul_df[region] == minimum_pop_growth_1_year].values[0]
        # print(year_min)
        # plt.plot(wide_df.index, wide_df[region])
        # plt.show()
        filtered_list_pop_decreas.append([region, minimum_pop_growth_1_year, year_min])
# print(f"number_filtered = {len(filtered_list_pop_decreas)}")

i = 0
total_correlation_list = []
stro_mode_negative_corr = []
weak_correlation = []
for region, pop_growth, year in filtered_list_pop_decreas:
    # print(f'for region: {region} that has pop growth of {pop_growth} at year {year}')
    cor = float(np.corrcoef(wide_df[region].values, wide_df_household[region].values)[0,1])
    # print(f'the correlation is: {cor}')
    total_correlation_list.append(cor)
    if cor <= -0.5: stro_mode_negative_corr.append([region, cor])
    elif cor > -0.5 and cor <= 0.5: weak_correlation.append([region, cor])

###############################################   abnormal change(ERP) ####################################################

def check_outliers(data_list):
    Q1 = np.percentile(data_list, 25)
    Q3 = np.percentile(data_list, 75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 3 * IQR
    upper_bound = Q3 + 3 * IQR
    for data in data_list:
        if data > upper_bound or data < lower_bound: return True

def calculate_r_squared(y, y_pred):
    
    # Calculate the total sum of squares (SST)
    ss_total = np.sum((y - np.mean(y))**2)
    
    # Calculate the residual sum of squares (SSR)
    ss_residual = np.sum((y_pred - np.mean(y))**2)
    
    # Calculate R^2
    r_squared = 1 - (ss_residual / ss_total)
    
    return r_squared
total_number = 0
flagged_number = 0
change_popul_df = wide_df.diff().dropna()
correlation_list = []
for region in wide_df.columns:
    cor = float(np.corrcoef(wide_df[region].values, wide_df_household[region].values)[0,1])
    # print(f"there are outliers: {check_outliers(change_popul_df[region])}")
    if check_outliers(change_popul_df[region]) and cor < 0.5:
        print(f'for region {region}')
        correlation_list.append(cor)
        print(f'the correlation with household is: {cor}')
        # print(f"the r squared value is: {calculate_r_squared(wide_df[region], wide_df_household[region])}") 
        # print(np.array(wide_df[region])/np.array(wide_df_household[region]))
        flagged_number += 1
        # plt.plot(wide_df.index[0:len(wide_df.index)], wide_df[region])
        # plt.show()

        # plt.plot(wide_df.index[0:len(wide_df.index)], wide_df_household[region])
        # plt.show()

        # plt.plot(wide_df.index[0:len(wide_df.index)-1], change_popul_df[region])
        # plt.show()
    total_number += 1


print(f"total number of fa regions are: {total_number}")
print(f"total number of flagged fa regions are: {flagged_number}")

# plt.hist(correlation_list)
# plt.show()
