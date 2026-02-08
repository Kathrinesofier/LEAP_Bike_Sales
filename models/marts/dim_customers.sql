select
  {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as sk_customer,
  c.customer_id,

  g.sk_geography,                

  c.customer_name,
  c.gender,
  c.marital_status,
  c.date_of_birth,
  c.yearly_income,
  c.occupation,
  c.current_status
from {{ ref('stg_customers') }} c
left join {{ ref('dim_geography') }} g
  on c.geography_id = g.geography_id


