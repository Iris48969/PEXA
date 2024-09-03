with structured_pop as (
SELECT ASGS_2016, SUM([Number]) as ERP, ERPYear, min(a.[RegionType]) as region_type
    FROM [forecasts].[dbo].[ERP] e
    left join [forecasts].[dbo].[AreasAsgs] a
    on e.ASGS_2016 = a.ASGSCode
    where a.[RegionType] = 'FA' or a.[RegionType] = 'SA2'
    group by ASGS_2016, ERPYear
)
SELECT h.ASGSCode, h.ERPYear as Year, p.ERP as Population, p.ERP/h.Number as ratio, p.region_type
    FROM 
        [forecasts].[dbo].[Households] h left join structured_pop p
        on h.ASGSCode = p.ASGS_2016 and h.ERPYear = p.ERPYear
        where left(ASGSCode,3) = {} AND h.Number != 0