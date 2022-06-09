with raw_table as (
    select
        salesreasonid as sales_reason_id,
        name as sales_reason_description,
        reasontype as sales_reason_type
    from
        {{source('adventure_works', 'salesreason')}}
)

select * from raw_table
