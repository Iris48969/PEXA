-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM forecasts.dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND ASGSCode LIKE '4%'
)

-- Sum the deaths for each SA2 and the corresponding FAs
SELECT 
    SA2List.ASGSCode AS SA2,
    -- Sum of Deaths at the SA2 Level
    SUM(Deaths.Number) AS "Sum of Deaths by SA2",
    -- Sum of Deaths by FA under each SA2
    (
        SELECT SUM(D2.Number)
        FROM forecasts.dbo.Deaths AS D2
        WHERE D2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    ) AS "Sum of Deaths by FA",

    -- Show a difference if difference is > 1, otherwise show NULL
    SUM(Deaths.Number) - (
        SELECT SUM(D2.Number)
        FROM forecasts.dbo.Deaths AS D2
        WHERE D2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    ) AS Difference,

    -- Reason for the difference
    'Mismatch between sum of Deaths at SA2 level vs. sum of FAs within this SA2' AS Reason

FROM 
    forecasts.dbo.Deaths AS Deaths
INNER JOIN 
    SA2List ON Deaths.ASGSCode = SA2List.ASGSCode
GROUP BY 
    SA2List.ASGSCode
HAVING 
    ABS(SUM(Deaths.Number) - (
        SELECT SUM(D2.Number)
        FROM forecasts.dbo.Deaths AS D2
        WHERE D2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    )) > 1
ORDER BY 
    SA2;
