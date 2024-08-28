   WITH CombinedData AS (
        -- Births Data
        SELECT 
            a.ASGSCode, 
            CASE 
                WHEN a.SexKey = 1 THEN 'Male' 
                WHEN a.SexKey = 2 THEN 'Female' 
            END AS Sex,
            a.Year, b.RegionType, b.Parent,
            p.Name AS ParentName, 
            SUM(a.Number) AS Total,
            'Births' AS DataType
        FROM 
            Births AS a
        LEFT JOIN 
            AreasAsgs AS b 
            ON a.ASGSCode = b.ASGSCode
        LEFT JOIN 
            AreasAsgs AS p
            ON b.Parent = p.ASGSCode 
        WHERE 
            a.Number <= 0
            AND b.RegionType IN ('FA', 'SA2')
        GROUP BY 
            a.ASGSCode, a.SexKey, a.Year, b.RegionType, 
            b.Parent, p.Name

        UNION ALL
        
        -- Deaths Data
        SELECT 
            a.ASGSCode, 
            CASE 
                WHEN a.SexKey = 1 THEN 'Male' 
                WHEN a.SexKey = 2 THEN 'Female' 
            END AS Sex,
            a.Year, b.RegionType, b.Parent,
            p.Name AS ParentName, 
            SUM(a.Number) AS Total,
            'Deaths' AS DataType
        FROM 
            Deaths AS a
        LEFT JOIN 
            AreasAsgs AS b 
            ON a.ASGSCode = b.ASGSCode
        LEFT JOIN 
            AreasAsgs AS p
            ON b.Parent = p.ASGSCode -- Self-join to get parent region name
        WHERE 
            a.Number <= 0
            AND b.RegionType IN ('FA', 'SA2')
        GROUP BY 
            a.ASGSCode, a.SexKey, a.Year,
            b.RegionType, b.Parent, p.Name

        UNION ALL
        
        -- ERP Population Data
        SELECT
            a.ASGS_2016 AS ASGSCode, 
            CASE 
                WHEN a.SexKey = 1 THEN 'Male' 
                WHEN a.SexKey = 2 THEN 'Female' 
            END AS Sex,
            a.ERPYear AS Year,
            b.RegionType, b.Parent,
            p.Name AS ParentName, -- Get the name of the parent region
            SUM(a.Number) AS Total,
            'ERP' AS DataType
        FROM 
            [forecasts].[dbo].[ERP] AS a
        LEFT JOIN 
            [forecasts].[dbo].[AreasAsgs] AS b
            ON a.ASGS_2016 = b.ASGSCode
        LEFT JOIN 
            [forecasts].[dbo].[AreasAsgs] AS p
            ON b.Parent = p.ASGSCode -- Self-join to get parent region name
        WHERE 
            b.RegionType IN ('SA2', 'FA')
        GROUP BY 
            a.ASGS_2016, a.ERPYear, b.RegionType, 
            b.Parent, p.Name, a.SexKey
    )

    SELECT * 
    FROM CombinedData
    ORDER BY DataType, Year, Total;
