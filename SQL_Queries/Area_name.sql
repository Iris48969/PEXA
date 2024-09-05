SELECT distinct ASGSCode, Name
    FROM [dbo].[AreasAsgs]
    where left(ASGSCode,3) = {}
    order by ASGSCode