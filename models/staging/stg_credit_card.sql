with raw_table as (
    select
        creditcardid as creditcard_id,
        cardtype as card_type
    from
        {{source('adventure_works', 'creditcard')}}

)
select * from raw_table