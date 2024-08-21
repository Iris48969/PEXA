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