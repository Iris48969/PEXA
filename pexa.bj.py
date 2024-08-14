
import pyodbc


# Connect to database
connection = pyodbc.connect('Driver={SQL Server};'
                            'Server=localhost;'
                            'Database=forecasts;'
                            'UID=sa;'
                            'PWD=MBS_project_2024')


# Create cursor
cursor = connection.cursor()


# Query database
cursor.execute("SELECT ASGSCode FROM AreasAsgs where parent = 401 and ASGS = 2021")
for row in cursor.fetchall():
    print(row.ASGSCode)

#household
cursor.execute("SELECT [ASGS_2016] ,[ERPYear], SUM([Number]) as Number FROM [forecasts].[dbo].[ERP] WHERE SexKey in (1, 2) GROUP BY ASGS_2016, ERPYear ORDER BY ERPYear")
for row in cursor.fetchall():
    print(row.ASGS_2016,row.ERPYear, row.Number)

# Maximum and Minimum
cursor.execute("""
    WITH BaseData AS (
        SELECT 
            ASGS_2016,
            ERPYear,
            SUM(Number) AS Number
        FROM 
            [forecasts].[dbo].[ERP]
        WHERE 
            ASGS_2016 = 401011001 
            AND SexKey IN (1, 2)
        GROUP BY 
            ASGS_2016,
            ERPYear
    )
    SELECT 
        a.ASGS_2016,
        a.ERPYear AS CurrentYear,
        a.Number AS CurrentNumber,
        b.ERPYear AS PreviousYear,
        b.Number AS PreviousNumber,
        -- Calculate the growth rate
        (a.Number - b.Number) AS GrowthRate,
        -- Add a flag to indicate if GrowthRate is less than 2000 or greater than 2500
        CASE 
            WHEN (a.Number - b.Number) < 2000 THEN 'Less than 2000'
            WHEN (a.Number - b.Number) > 2500 THEN 'Greater than 2500'
            ELSE 'Within Range'
        END AS GrowthFlag
    FROM 
        BaseData a
    JOIN 
        BaseData b 
    ON 
        a.ASGS_2016 = b.ASGS_2016 
        AND a.ERPYear = b.ERPYear + 5
    ORDER BY 
        a.ERPYear;
""")

# Fetch and print the results
for row in cursor.fetchall():
    print(f"ASGS_2016: {row.ASGS_2016}, CurrentYear: {row.CurrentYear}, "
          f"CurrentNumber: {row.CurrentNumber}, PreviousYear: {row.PreviousYear}, "
          f"PreviousNumber: {row.PreviousNumber}, GrowthRate: {row.GrowthRate}, "
          f"GrowthFlag: {row.GrowthFlag}")

# Close connection
connection.close()
