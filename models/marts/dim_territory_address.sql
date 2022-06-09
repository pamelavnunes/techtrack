with stg_address as (
    select
         address_id,
         city,
         state_province_id
    from
        {{ref('stg_address')}}
),

state_province as (
    select
        state_province_id,
        state_province_code,
        name_province,
        country_region_code,
        territory_id
    from
        {{ref('stg_state_province')}}
),

country_region as (
    select
        country_region_code,
        country_name
    from
        {{ref('stg_country_region')}}

),

sales_territory as (
    select
     territory_id,
     territory_name,
     country_region_code
    from
        {{ref('stg_sales_territory')}}

),
final as (
    select
        stg_address.address_id,
        stg_address.city,
        state_province.state_province_id,
        state_province.state_province_code,
        state_province.name_province,
        state_province.territory_id,
        country_region.country_region_code,
        country_region.country_name,
        sales_territory.territory_name
    from
        stg_address
        left join state_province 
            on stg_address.state_province_id = state_province.state_province_id
        left join country_region
            on state_province.country_region_code = country_region.country_region_code
        left join sales_territory
            on state_province.territory_id = sales_territory.territory_id
)

select * from final
