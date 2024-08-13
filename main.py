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
#cursor.execute("SELECT ASGSCode FROM AreasAsgs where parent = 401 and ASGS = 2021")
#for row in cursor.fetchall():
#    print(row.ASGSCode)

cursor.execute("SELECT ASGSCode, Name, RegionType, Parent  FROM AreasAsgs WHERE parent = 401 AND ASGS = 2021")
for row in cursor.fetchall():
    print(row.ASGSCode, row.RegionType)


# Close connection
connection.close()