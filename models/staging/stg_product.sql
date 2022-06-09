with raw_table as (
    select
        productid as product_id,
        name as product_name
    from
        {{source('adventure_works', 'product')}}

)
select * from raw_table