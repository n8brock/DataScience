-- complex CTE-style SQL query to access data stored in a Snowflake Data Lakehouse
-- Constellation1 Data Labs API, Zillow, Construciton Monitor Permits API

with
geocity as(
    select
        geoid, geoname as city,
        case
            when (loweR(geoname) = 'albany' and state = 'OR') then 'Benton'
            when (lower(geoname) = 'madras' and state = 'OR') then 'Jefferson'
            when (lower(geoname) = 'salem' and state = 'OR') then 'Marion'
            else replace(countyname, ' County', '')
        end as county,
        state
    from lkp_geoid
    where state in('OR','WA','ID','MT') and geolevel = 'City'
),

assessor as (
-- treated as "timeless," since properties remain on record through history
-- LIM - CANT use current values - DOES NOT account for zoning changes
select
    iff(a1.TotalResLots=0,0, a1.ImprovedLot / a1.TotalResLots) as PercImprovedLot,
    iff(a1.TotalResLots=0,0, a1.SubLot / a1.TotalResLots)::float as PercSubLot,
    a1.* from (
    select
        g.geoid,
        median(a.assessedlandvalue) as AssessedLandValue,
        median(a.marketvalueland) as MarketLandValue,
        median(totalmarketvalue) as TotalMarketValue,
        median(taxamount) as Tax,
        SUM(CASE WHEN a.assessedimprovementvalue > 0 OR a.marketvalueimprovement > 0 THEN 1 ELSE 0 END)::int AS ImprovedLot,
        COUNT(*)::int AS TotalResLots,
        MEDIAN(a.lotsizeacres)::float AS MedLotSize,
        -- SUM(CASE WHEN a.lotsizeacres < 1 THEN 1 ELSE 0 END)::int AS SubLot
        SUM(CASE WHEN a.lotsizesquarefeet <= 10890 THEN 1 ELSE 0 END)::int AS SubLot
    from geocity g
    join c1_assessor a
    on (case lower(a.city) when 'coeur d alene' then 'coeur d''alene' else lower(a.city) end) = lower(g.city)
    and upper(a.stateorprovince) = upper(g.state)
    where a.standardizedlandusecode IN(
        'Agricultural / Rural (General)',
        'Duplex (2 units, any combination)',
        'Residential (General) (Single)',
        'Residential-Vacant Land',
        'Single Family Residential',
        'Single Family Residential (Assumed)',
        'Single Family Residential w/ADU',
        'Townhouse (Residential)',
        'Triplex (3 units, any combination)',
--            'Under Construction',
--            'Vacant Land - Exempt',
        'Vacant Land - Unspecified Improvement',
        'Vacant Land (General)')
        and (left(replace(a.zoning, '-', ''), 2) -- short version of zoning code was more consistent; codes often included comments, full names, etc.
            in('UR', 'TR', 'TC', 'SZ', 'SR', 'SF', 'RU', 'RS', 'RR', 'RP', 'RL', 'RD', 'RC', 'RA', 'R7', 'R5', 'R2', 'R1', 'MX', 'MU', 'LM', 'LD', 'FR', 'ER',
            'EF', 'EC', 'E6', 'E4', 'E3', 'E2', 'E ', 'AR', 'AG', 'AF', 'AC', 'A1', 'A ', 'A')
        or a.zoning is null)
        and a.stateorprovince in ('OR', 'WA', 'ID', 'MT')
    group by g.geoid
    ) a1
),

permits1 as (
select
    iff(p1.totalpermits=0,0, p1.prodbuilderpermits / p1.totalpermits)::float as PercProdBuilder,
    p1.* from (
    select
        g.geoid,
--        city_geoid, county_geoid, state_geoid,
        count(*) as TotalPermits,
        sum(CASE WHEN p.builder in('Lennar', 'D.R. Horton', 'CBH Homes', 'Hayden Homes', 'Toll Brothers', 'Richmond American Homes', 'Pahlisch Homes', 'Hubble Homes', 'Holt Homes', 'Taylor Morrison', 'MainVue Homes', 'Tresidio Homes', 'Pro Made Const', 'Blackrock Homes', 'KB Home', 'LGI Homes', 'Greenstone Homes', 'Pacific Lifestyle Homes', 'Benchmark Communities', 'Brighton Homes', 'Stone Bridge Homes NW', 'Soundbuilt Homes', 'Tri Pointe Homes', 'KB Homes', 'Shea Homes', 'MonteVista Homes', 'Pulte Homes', 'Weekley Homes', 'New Tradition Homes', 'Boise Hunter Homes', 'Aho Construction', 'Chad Davis Construction', 'Viking Homes', 'Berkeley Building Co')
            THEN 1 ELSE 0 END) AS ProdBuilderPermits
    from geocity g
    join permits p on p.city_geoid = g.geoid
    where p.permitdate >= '2022-08-01'::DATE and p.permitdate <= '2025-08-01'::DATE
        and p.permitstatus = 'Approved' and p.class = 'Residential' and p.type = 'Single Family Residence'
        and p.state in ('OR', 'WA', 'ID', 'MT')
    group by g.geoid
) p1
),

zillow as (
select
    iff(z1.PricePast=0,0, (z1.PriceNow - z1.PricePast) / z1.PricePast)::float as PriceGrowth,
    z1.* from (
    select
        a.geoid,
        a.PricePast,
        b.PriceNow
    from
    (select replace(geoid_sk, 'geoId/', '') as geoid, medianhomeprice_zillow as PricePast
        from zillow_medianhomeprice_city z
        where z.reportdate = '2019-07-31'::DATE
        group by z.reportdate, geoid_sk, medianhomeprice_zillow) a
    join
    (select replace(geoid_sk, 'geoId/', '') as geoid, medianhomeprice_zillow as PriceNow
        from zillow_medianhomeprice_city z
        where z.reportdate = '2025-07-31'::DATE
        group by z.reportdate, geoid_sk, medianhomeprice_zillow) b
    using(geoid)
) z1
),

demo as (
select
    iff(d1.HousingUnits=0,0, 1 - (d1.HousingUnits_Occupied / d1.HousingUnits))::float as PercAvailableUnits,
    d1.* from (
-- growth rates across entire time frame
    select
--        d.geoid, d.geoname as city, d.state,
        g.geoid,
        d.householdsize::float as HouseholdSize,
        d.housingunits::int as HousingUnits,
        d.housingunits_occupied::int as HousingUnits_Occupied,
        population_5yrprojected as Pop5YrProj,
        iff(population2010=0,0, (d.population - d.population2010)/population2010)::float as PopGrowth2010,
        d.mfi::int as mfi
    from geocity g
    join demographics d using(geoid)
    where d.geolevel = 'City' and d.state in('OR', 'WA', 'ID', 'MT')
) d1
)

select * from geocity
left join assessor using(geoid)
left join permits1 using(geoid)
left join zillow using(geoid)

left join demo using(geoid)
