with raw_table as (
    select
        salesorderid as sales_order_id,
        salesreasonid as sales_reason_id
    from
        {{source('adventure_works', 'salesorderheadersalesreason')}}
)

select * from raw_table