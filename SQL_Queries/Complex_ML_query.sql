WITH PopulationDensity AS (
    SELECT 
        ERP.ASGS_2016,
        ERP.ERPYear,
        SUM(ERP.Number) AS TotalPopulation,
        SUM(Households.Number) AS Dwellings,
        CASE 
            WHEN SUM(Households.Number) = 0 THEN NULL
            ELSE SUM(ERP.Number) / NULLIF(SUM(Households.Number), 0)
        END AS PopulationDensity
    FROM 
        ERP
    JOIN 
        Households ON ERP.ASGS_2016 = Households.ASGSCode AND ERP.ERPYear = Households.ERPYear
    WHERE Households.HhKey = 19
    GROUP BY 
        ERP.ASGS_2016, 
        ERP.ERPYear
),
GrowthRate AS (
    SELECT 
        ASGS_2016,
        ERPYear,
        RegionType,
        Name,
        (SUM(Number) - LAG(SUM(Number)) OVER (PARTITION BY ASGS_2016 ORDER BY ERPYear)) / NULLIF(LAG(SUM(Number)) OVER (PARTITION BY ASGS_2016 ORDER BY ERPYear), 0) AS GrowthRate
    FROM 
        ERP
    JOIN AreasAsgs ON ERP.ASGS_2016 = AreasAsgs.ASGSCode 
    GROUP BY 
        ASGS_2016, 
        ERPYear,
        RegionType,
        Name
),
BirthDeathRatio AS (
    SELECT 
        Births.ASGSCode,
        Births.Year,
        SUM(Births.Number) AS Births,
        SUM(Deaths.Number) AS Deaths,
        CASE 
            WHEN SUM(Deaths.Number) = 0 THEN NULL
            ELSE SUM(Births.Number) / NULLIF(SUM(Deaths.Number), 0)
        END AS BirthDeathRatio
    FROM 
        Births
    JOIN 
        Deaths ON Births.ASGSCode = Deaths.ASGSCode AND Births.Year = Deaths.Year
    GROUP BY 
        Births.ASGSCode,
        Births.Year
)
SELECT 
    PD.ASGS_2016 AS ASGSCode,
    PD.ERPYear,
    PD.TotalPopulation,
    PD.Dwellings,
    PD.PopulationDensity,
    GR.GrowthRate,
    GR.RegionType,
    GR.Name,
    BDR.Births,
    BDR.Deaths,
    BDR.BirthDeathRatio
FROM 
    PopulationDensity PD
LEFT JOIN 
    GrowthRate GR ON PD.ASGS_2016 = GR.ASGS_2016 AND PD.ERPYear = GR.ERPYear
LEFT JOIN 
    BirthDeathRatio BDR ON PD.ASGS_2016 = BDR.ASGSCode AND PD.ERPYear = BDR.Year
WHERE 
    left(PD.ASGS_2016,3) = {}
ORDER BY 
    PD.ASGS_2016, 
    PD.ERPYear;
