-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM forecasts.dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND ASGSCode LIKE '4%'
)

SELECT 
    SA2List.ASGSCode AS SA2,
    ERP.SexKey,
    ERP.AgeKey,
    ERP.ERPYear,
    -- Sum of Number at the SA2 Level
    SUM(ERP.Number) AS "Sum of Population by SA2",
    -- Sum of Number by FA under each SA2
    (
        SELECT SUM(E2.Number)
        FROM forecasts.dbo.ERP AS E2
        WHERE E2.ASGS_2016 IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
        AND E2.SexKey = ERP.SexKey
        AND E2.AgeKey = ERP.AgeKey
        AND E2.ERPYear = ERP.ERPYear
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
        AND E2.SexKey = ERP.SexKey
        AND E2.AgeKey = ERP.AgeKey
        AND E2.ERPYear = ERP.ERPYear
    ) AS Difference,

    -- Reason for the difference
    'Mismatch between sum of Population at SA2 level vs. sum of FAs within this SA2' AS Reason

FROM 
    forecasts.dbo.ERP AS ERP
INNER JOIN 
    SA2List ON ERP.ASGS_2016 = SA2List.ASGSCode
GROUP BY 
    SA2List.ASGSCode, ERP.SexKey, ERP.AgeKey, ERP.ERPYear
HAVING 
    ABS(SUM(ERP.Number) - (
        SELECT SUM(E2.Number)
        FROM forecasts.dbo.ERP AS E2
        WHERE E2.ASGS_2016 IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
        AND E2.SexKey = ERP.SexKey
        AND E2.AgeKey = ERP.AgeKey
        AND E2.ERPYear = ERP.ERPYear
    )) > 1
ORDER BY 
    SA2, ERP.SexKey, ERP.AgeKey, ERP.ERPYear;