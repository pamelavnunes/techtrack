with sales_order_header as (
    select
        sales_order_id,
        order_date,
        customer_id,
        territory_id,
        ship_address_id,
        creditcard_id,
        freight,
        order_status
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
        order_quantity * unit_price * (1-unit_price_discount) as total_product_value
    from
        {{ref('stg_sales_order_detail')}}
),

product as (
    select
        product_id,
        product_name
    from
        {{ref('stg_product')}}
),

sales_reason as (
    select
        sales_reason_id,
        sales_reason_description,
        sales_reason_type
    from {{ref('stg_sales_reason')}}
),

sales_order_header_reason as (
    select
        sales_order_id,
        sales_reason_id,
        ROW_NUMBER() OVER (PARTITION BY sales_order_id) as rn
    from {{ref('stg_sales_order_header_reason')}}
),

final as (
    select
        order_header.sales_order_id,
        order_header.order_date,
        order_header.customer_id,
        order_header.ship_address_id,
        order_header.creditcard_id,
        order_header.order_status,
        sales_reason.sales_reason_type,
        sales_reason.sales_reason_description,
        order_header.freight / order_detail.total_products as freight,
        order_detail.sales_order_detail_id,
        order_detail.product_id,
        product.product_name,
        order_quantity,
        order_detail.total_product_value
    from
        sales_order_header as order_header
        inner join sales_order_detail as order_detail
        left join product on order_detail.product_id = product.product_id
            on order_header.sales_order_id = order_detail.sales_order_id
        left join sales_order_header_reason
            on order_header.sales_order_id = sales_order_header_reason.sales_order_id and sales_order_header_reason.rn = 1
        left join sales_reason on sales_order_header_reason.sales_reason_id = sales_reason.sales_reason_id
)

select * from final
