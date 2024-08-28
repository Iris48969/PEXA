-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM forecasts.dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND ASGSCode LIKE '4%'
)

-- Sum the births for each SA2 and the corresponding FAs
SELECT 
    SA2List.ASGSCode AS SA2,
    -- Sum of Births at the SA2 Level
    SUM(Births.Number) AS "Sum of Births by SA2",
    -- Sum of Births by FA under each SA2
    (
        SELECT SUM(B2.Number)
        FROM forecasts.dbo.Births AS B2
        WHERE B2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    ) AS "Sum of Births by FA",

    -- Show a difference if difference is > 1, otherwise show NULL
    SUM(Births.Number) - (
        SELECT SUM(B2.Number)
        FROM forecasts.dbo.Births AS B2
        WHERE B2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    ) AS Difference,

    -- Reason for the difference
    'Mismatch between sum of Births at SA2 level vs. sum of FAs within this SA2' AS Reason

FROM 
    forecasts.dbo.Births AS Births
INNER JOIN 
    SA2List ON Births.ASGSCode = SA2List.ASGSCode
GROUP BY 
    SA2List.ASGSCode
HAVING 
    ABS(SUM(Births.Number) - (
        SELECT SUM(B2.Number)
        FROM forecasts.dbo.Births AS B2
        WHERE B2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    )) > 1
ORDER BY 
    SA2;
