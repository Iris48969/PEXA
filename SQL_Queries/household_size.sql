with structured_pop as (
SELECT ASGS_2016, SUM([Number]) as ERP, ERPYear, min(a.[RegionType]) as region_type
    FROM [dbo].[ERP] e
    left join [dbo].[AreasAsgs] a
    on e.ASGS_2016 = a.ASGSCode
    where (a.[RegionType] = 'FA' or a.[RegionType] = 'SA2')
    and left(e.ASGS_2016,3) = {}
    group by ASGS_2016, ERPYear
)
SELECT h.ASGSCode, h.ERPYear as Year, p.ERP as Population, p.ERP/h.Number as ratio, p.region_type
    FROM 
        [dbo].[Households] h inner join structured_pop p
        on h.ASGSCode = p.ASGS_2016 and h.ERPYear = p.ERPYear and h.HhKey = 19 -- This denotes SPD (Structured private dwellings)
        where h.Number != 0