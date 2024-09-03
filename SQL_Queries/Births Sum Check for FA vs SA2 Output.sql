-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND left(ASGSCode,3) = {}
)

-- Sum the births for each SA2 and the corresponding FAs
SELECT 
    SA2List.ASGSCode AS Code,
    'SA2' AS "Region Type",
    -- Reason for the difference, renamed to Description
    'Mismatch between sum of Births at SA2 level vs. sum of FAs within this SA2' AS Description

FROM 
    dbo.Births AS Births
INNER JOIN 
    SA2List ON Births.ASGSCode = SA2List.ASGSCode
GROUP BY 
    SA2List.ASGSCode
HAVING 
    ABS(SUM(Births.Number) - (
        SELECT SUM(B2.Number)
        FROM dbo.Births AS B2
        WHERE B2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    )) > 1
ORDER BY 
    SA2List.ASGSCode;