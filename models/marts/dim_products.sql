select
  {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as sk_product,
  p.product_id,

  sp.sk_product_sub_category,  

  p.product_name,
  p.color,
  p.model_name,
  p.product_line,
  p.standard_cost,
  p.list_price,
  p.dealer_price,
  p.status
from {{ ref('stg_products') }} p
left join {{ ref('dim_products_sub_category') }} sp
  on p.product_sub_category_id = sp.product_sub_category_id
