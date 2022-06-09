with creditcard as (
    select
         creditcard_id,
         card_type
    from
        {{ref('stg_credit_card')}}
)

select * from creditcard