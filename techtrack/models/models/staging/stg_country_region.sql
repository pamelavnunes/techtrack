with raw_table as (
    select
        countryregioncode as country_region_code,
        name as country_name
    from
        {{source('adventure_works', 'countryregion')}}

)
select * from raw_table