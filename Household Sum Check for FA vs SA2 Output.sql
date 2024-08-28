-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM forecasts.dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND ASGSCode LIKE '4%'
)

-- Sum households for each SA2 and the corresponding FAs
SELECT 
    SA2List.ASGSCode AS SA2,
    -- Sum of Households at the SA2 Level for HhKey = 19
    SUM(Households.Number) AS "Sum of Households by SA2",
    -- Sum of Households by FA under each SA2 for HhKey = 19
    (
        SELECT SUM(H2.Number)
        FROM forecasts.dbo.Households AS H2
        WHERE H2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
        AND H2.HhKey = 19
    ) AS "Sum of Households by FA",

    -- Show a difference if difference is > 1, otherwise show NULL
    SUM(Households.Number) - (
        SELECT SUM(H2.Number)
        FROM forecasts.dbo.Households AS H2
        WHERE H2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
        AND H2.HhKey = 19
    ) AS Difference,

    -- Reason for the difference
    'Mismatch between sum of Households at SA2 level vs. sum of FAs within this SA2' AS Reason

FROM 
    forecasts.dbo.Households AS Households
INNER JOIN 
    SA2List ON Households.ASGSCode = SA2List.ASGSCode
WHERE
    Households.HhKey = 19
GROUP BY 
    SA2List.ASGSCode
HAVING 
    ABS(SUM(Households.Number) - (
        SELECT SUM(H2.Number)
        FROM forecasts.dbo.Households AS H2
        WHERE H2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM forecasts.dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
        AND H2.HhKey = 19
    )) > 1
ORDER BY 
    SA2;
