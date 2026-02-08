select
  {{ dbt_utils.generate_surrogate_key(['product_sub_category_id']) }} as sk_product_sub_category,
  product_sub_category_id,
  product_category_id,
  product_sub_category_name
from {{ ref('stg_productsubcategory') }} 
