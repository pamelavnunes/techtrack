with raw_table as (
    select
        territoryid as territory_id,
        name as territory_name,
        countryregioncode as country_region_code
    from
        {{source('adventure_works', 'salesterritory')}}
)

select * from raw_table