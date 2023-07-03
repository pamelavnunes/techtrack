with raw_table as (
    select
        salesorderid as sales_order_id,
        orderdate as order_date,
        status as order_status,
        customerid as customer_id,
        territoryid as territory_id,
        shiptoaddressid as ship_address_id,
        creditcardid as creditcard_id,
        freight
    from
        {{source('adventure_works', 'salesorderheader')}}
)

select * from raw_table
