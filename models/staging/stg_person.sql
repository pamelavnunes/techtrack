with raw_table as (
    select
        businessentityid as person_id,
        firstname as first_name,
        lastname as last_name
    from
        {{source('adventure_works', 'person')}}

)
select * from raw_table