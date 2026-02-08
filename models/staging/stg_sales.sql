select *
from {{ source('bike_sales_src', 'sales') }}
