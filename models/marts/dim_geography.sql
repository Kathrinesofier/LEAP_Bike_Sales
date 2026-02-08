select
  {{ dbt_utils.generate_surrogate_key(['geography_id']) }} as sk_geography,
  geography_id,
  country_code,
  country,
  state_code,
  state_name,
  city,
  postal_code
from {{ ref('stg_geography') }}

