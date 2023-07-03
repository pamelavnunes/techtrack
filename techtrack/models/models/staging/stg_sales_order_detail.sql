with raw_table as (
    select
        salesorderid as sales_order_id,
        salesorderdetailid as sales_order_detail_id,
        orderqty as order_quantity,
        productid as product_id,
        unitprice as unit_price,
        unitpricediscount as unit_price_discount
    from
        {{source('adventure_works', 'salesorderdetail')}}

)
select * from raw_table