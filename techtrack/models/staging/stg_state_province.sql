with raw_table as (
    select
        stateprovinceid as state_province_id,
        stateprovincecode as state_province_code,
        name as name_province,
        countryregioncode as country_region_code,
        territoryid as territory_id
    from
        {{source('adventure_works', 'stateprovince')}}

)
select * from raw_table