SELECT 
        [ASGSCode],
        [ERPYear]as Year,
        SUM([Number]) as HouseholdCount 
    FROM 
        [forecasts].[dbo].[Households]
    WHERE 
        HhKey = 19 
    GROUP BY 
        ASGSCode, HhKey, ERPYear 
    ORDER BY 
        ASGSCode