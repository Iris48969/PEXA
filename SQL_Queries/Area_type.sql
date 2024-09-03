SELECT distinct ASGSCode, RegionType
    FROM [dbo].[AreasAsgs]
    where left(ASGSCode,3) = {}
    order by ASGSCode