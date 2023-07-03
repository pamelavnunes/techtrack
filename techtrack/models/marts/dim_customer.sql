with customer as (
    select
         customer_id,
         person_id,
         store_id
    from
        {{ref('stg_customer')}}
),

person as (
    select
         person_id,
         first_name,
         last_name,
         first_name || ' ' || last_name as full_name
    from
        {{ref('stg_person')}}
),

store as (
    select
         store_id,
         store_name
    from
        {{ref('stg_store')}}
),

final as (
    select
        customer.customer_id,
        case
            when customer.person_id is null and customer.store_id is not null then 'Store'
            when customer.person_id is not null and customer.store_id is null then 'Person'
            when customer.person_id is not null and customer.store_id is not null then 'Store/Person'
        end as customer_type,
        case
            when customer.person_id is null and customer.store_id is not null then store.store_name
            when customer.person_id is not null and customer.store_id is null then person.full_name
            when customer.person_id is not null and customer.store_id is not null then  store.store_name || ' - ' || person.full_name
        end as customer_name
    from
        customer
        left join person
            on customer.person_id = person.person_id
        left join store
            on customer.store_id = store.store_id
)
select * from final





