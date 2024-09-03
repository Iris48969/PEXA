-- Generates the list of SA2s
WITH SA2List AS (
    SELECT DISTINCT ASGSCode
    FROM dbo.AreasAsgs
    WHERE RegionType = 'SA2' AND left(ASGSCode,3) = {}
)

-- Sum the deaths for each SA2 and the corresponding FAs
SELECT 
    SA2List.ASGSCode AS Code,
    'SA2' AS "Region Type",
    'Mismatch between sum of Deaths at SA2 level vs. sum of FAs within this SA2' AS Description

FROM 
    dbo.Deaths AS Deaths
INNER JOIN 
    SA2List ON Deaths.ASGSCode = SA2List.ASGSCode
GROUP BY 
    SA2List.ASGSCode
HAVING 
    ABS(SUM(Deaths.Number) - (
        SELECT SUM(D2.Number)
        FROM dbo.Deaths AS D2
        WHERE D2.ASGSCode IN (
            SELECT AreasAsgs.ASGSCode
            FROM dbo.AreasAsgs
            WHERE AreasAsgs.Parent = SA2List.ASGSCode
        )
    )) > 1
ORDER BY 
    SA2List.ASGSCode;