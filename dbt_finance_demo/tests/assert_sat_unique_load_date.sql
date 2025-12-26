-- 同じ時点(load_date)で同じエンティティに対して複数の異なる属性情報が存在したらエラー
with duplicate_check as (
    select
        order_hk,
        load_date,
        count(distinct order_hashdiff) as diff_count
    from {{ ref('sat_order_details') }}
    group by 1, 2
)
select * from duplicate_check where diff_count > 1