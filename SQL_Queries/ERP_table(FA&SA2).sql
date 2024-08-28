SELECT ASGS_2016, SUM([Number]) as ERP, ERPYear
    FROM [forecasts].[dbo].[ERP] e
    left join [forecasts].[dbo].[AreasAsgs] a
    on e.ASGS_2016 = a.ASGSCode
    where a.[RegionType] = 'FA' or a.[RegionType] = 'SA2'
    group by ASGS_2016, ERPYear