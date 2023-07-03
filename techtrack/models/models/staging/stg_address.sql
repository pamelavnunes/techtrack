with raw_table as (
    select
        addressid as address_id,
        city,
        stateprovinceid as state_province_id
    from
        {{source('adventure_works', 'address')}}
)

select * from raw_table