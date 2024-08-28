-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM forecasts.dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND ASGSCode LIKE '4%'
)

-- Sum households for each SA2 and the corresponding FAs
SELECT 
    SA2List.ASGSCode AS Code,
    'SA2' AS "Region Type",
    'Mismatch between sum of Households at SA2 level vs. sum of FAs within this SA2' AS Description

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
    SA2List.ASGSCode;