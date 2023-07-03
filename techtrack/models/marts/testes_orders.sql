with sales_order_header as (
    select
        sales_order_id,
        order_date,
        order_status,
        customer_id,
        territory_id,
        ship_address_id,
        creditcard_id,
        freight
    from
        {{ref('stg_sales_order_header')}}
),

sales_order_detail as (
    select
        sales_order_id,
        sales_order_detail_id,
        count(sales_order_detail_id) over(partition by sales_order_id) as total_products,
        order_quantity,
        product_id,
        unit_price,
        unit_price_discount,
        order_quantity * unit_price * (1-unit_price_discount) as total_prodcut_value
    from
        {{ref('stg_sales_order_detail')}}
),

final as (
    select
        order_header.sales_order_id,
        order_detail.sales_order_detail_id,
        order_detail.product_id,
        order_detail.total_prodcut_value,
        order_header.freight / order_detail.total_products as freight
    from
        sales_order_header as order_header
        inner join sales_order_detail as order_detail
            on order_header.sales_order_id = order_detail.sales_order_id
)

select * from final