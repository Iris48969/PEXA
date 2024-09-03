-- ERP Population Data
SELECT
    e.ASGS_2016 AS ASGSCode, 
    e.ERPYear,a.RegionType, 
    SUM(e.Number) AS ERP,
    SUM(h.Number) AS Household
FROM 
    [dbo].[ERP] AS e
LEFT JOIN [dbo].[AreasAsgs] as a ON e.ASGS_2016 = a.ASGSCode
LEFT JOIN [dbo].[Households] as h ON e.ASGS_2016 = h.ASGSCode
WHERE 
    left(e.ASGS_2016,3) = {} AND a.RegionType IN ('FA','SA2') and a.ASGS = 2021 -- selecting the latest census 2021
GROUP BY 
a.RegionType, e.ERPYear,e.ASGS_2016