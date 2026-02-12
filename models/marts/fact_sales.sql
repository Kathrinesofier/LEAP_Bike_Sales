{{ config(
    materialized='incremental',
    unique_key='sales_id'
) }}

with s as (

    select
        sales_id,
        sales_order_number,
        order_date_id,
        customer_id,
        product_id,
        order_qty,
        unit_price,
        extended_amount,
        unit_price_discount_pct,
        discount_amount,
        product_standard_cost,
        total_product_cost,
        sales_amount,
        tax_amount,
        freight

    from {{ ref('stg_sales') }}

),

with_dim_keys as (

    select
        s.*,
        c.sk_customer,
        p.sk_product
    from s
    left join {{ ref('dim_customers') }} c
      on s.customer_id = c.customer_id
    left join {{ ref('dim_products') }} p
      on s.product_id = p.product_id

)

select
    sales_id,
    sales_order_number,
    order_date_id,
    sk_customer,
    sk_product,
    order_qty,
    unit_price,
    extended_amount,
    unit_price_discount_pct,
    discount_amount,
    product_standard_cost,
    total_product_cost,
    sales_amount,
    tax_amount,
    freight,

    -- derived measures
    (extended_amount - discount_amount) as net_extended_amount,

    (sales_amount - total_product_cost) as gross_margin_amount,
    100 * (sales_amount - total_product_cost) / nullif(sales_amount, 0) as gross_margin_pct,

    freight / nullif(order_qty, 0) as freight_per_unit,

    tax_amount / nullif(sales_amount, 0) as tax_rate,

from with_dim_keys

{% if is_incremental() %}
where order_date_id >
      (select max(order_date_id) from {{ this }})
{% endif %}