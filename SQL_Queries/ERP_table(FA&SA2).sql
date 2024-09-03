SELECT ASGS_2016, SUM([Number]) as ERP, ERPYear
    FROM [dbo].[ERP] e
    left join [dbo].[AreasAsgs] a
    on e.ASGS_2016 = a.ASGSCode
    where left(ASGS_2016,3) = {} AND (a.[RegionType] = 'FA' or a.[RegionType] = 'SA2')
    group by ASGS_2016, ERPYear