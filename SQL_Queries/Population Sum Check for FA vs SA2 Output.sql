-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM forecasts.dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND ASGSCode LIKE '4%'
)

-- Sum the population for each SA2 and the corresponding FAs
SELECT 
    SA2List.ASGSCode AS SA2,
    -- Sum of Population at the SA2 Level
    SUM(ERP.Number) AS "Sum of Population by SA2",
    -- Sum of Population by FA under each SA2
    (
        SELECT SUM(E2.Number)
        FROM forecasts.dbo.ERP AS E2
        WHERE E2.ASGS_2016 IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    ) AS "Sum of Population by FA",

    -- Show a difference if difference is > 1, otherwise show NULL
    SUM(ERP.Number) - (
        SELECT SUM(E2.Number)
        FROM forecasts.dbo.ERP AS E2
        WHERE E2.ASGS_2016 IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    ) AS Difference,

    -- Reason for the difference
    'Mismatch between sum of Population at SA2 level vs. sum of FAs within this SA2' AS Reason

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
    SA2