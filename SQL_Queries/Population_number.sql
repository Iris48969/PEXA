SELECT 
        [ASGS_2016] as ASGSCode,
        [ERPYear] as Year, 
        SUM([Number]) as Population 
    FROM 
        [forecasts].[dbo].[ERP] 
    GROUP BY 
        ASGS_2016, ERPYear 
    ORDER BY 
        ASGS_2016