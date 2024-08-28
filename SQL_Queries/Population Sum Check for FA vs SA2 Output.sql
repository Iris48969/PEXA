-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM forecasts.dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND ASGSCode LIKE '4%'
)

-- Sum the population for each SA2 and the corresponding FAs
SELECT 
    SA2List.ASGSCode AS Code,
    'SA2' AS "Region Type",
    'Mismatch between sum of Population at SA2 level vs. sum of FAs within this SA2' AS Description

FROM 
    forecasts.dbo.ERP AS ERP
INNER JOIN 
    SA2List ON ERP.ASGS_2016 = SA2List.ASGSCode
GROUP BY 
    SA2List.ASGSCode
HAVING 
    ABS(SUM(ERP.Number) - (
        SELECT SUM(E2.Number)
        FROM forecasts.dbo.ERP AS E2
        WHERE E2.ASGS_2016 IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    )) > 1
ORDER BY 
    SA2List.ASGSCode;