SELECT 
        [ASGS_2016] as ASGSCode,
        [ERPYear] as Year, 
        SUM([Number]) as Population 
    FROM 
        [forecasts].[dbo].[ERP] 
    GROUP BY 
        ASGSCode, ERPYear 
    ORDER BY 
        ASGSCode