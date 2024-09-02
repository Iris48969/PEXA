-- ERP Population Data
SELECT
    e.ASGS_2016 AS ASGSCode, 
    e.ERPYear,a.RegionType, 
    SUM(e.Number) AS Total
FROM 
    [forecasts].[dbo].[ERP] AS e
LEFT JOIN [forecasts].[dbo].[AreasAsgs] as a ON e.ASGS_2016 = a.ASGSCode
WHERE 
    a.RegionType IN ('FA','SA2') and a.ASGS = 2021 -- selecting the latest census 2021
GROUP BY 
a.RegionType, e.ERPYear,e.ASGS_2016