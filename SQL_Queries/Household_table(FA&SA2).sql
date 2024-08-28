SELECT h.ASGSCode, h.ERPYear, h.Number
    from Households h
    left join [forecasts].[dbo].[AreasAsgs] a
    on h.ASGSCode = a.ASGSCode
    where a.[RegionType] = 'FA' or a.[RegionType] = 'SA2'