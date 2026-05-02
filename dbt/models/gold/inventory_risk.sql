{{ config(materialized='table') }}

with latest_inventory as (
    select
        *,
        row_number() over (
            partition by product_id, store_id
            order by event_ts desc
        ) as row_num
    from {{ ref('stg_inventory') }}
)

select
    product_id,
    sku,
    store_id,
    event_ts,
    on_hand,
    available_to_promise,
    reorder_point,
    case
        when available_to_promise <= 0 then 'stockout'
        when available_to_promise <= reorder_point then 'reorder'
        else 'healthy'
    end as inventory_risk_level
from latest_inventory
where row_num = 1

