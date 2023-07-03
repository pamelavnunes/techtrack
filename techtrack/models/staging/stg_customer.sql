with raw_table as (
    select
        customerid as customer_id,
        personid as person_id,
        storeid as store_id,
        territoryid as territory_id
    from
        {{source('adventure_works', 'customer')}}

)
select * from raw_table