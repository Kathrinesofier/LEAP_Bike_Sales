with source as (

select *
from {{ source('bike_sales_src', 'geography') }}
), 

renamed_geography as (

    select
        GEOGRAPHYKEY as geography_id,
        CITY as city,
        STATEPROVINCECODE as state_code,
        STATEPROVINCENAME as state_name,
        COUNTRYREGIONCODE as country_code,
        CUSTOMER_COUNTRY as country, 
        POSTALCODE as postal_code, 
        IPADDRESSLOCATOR as ip_address_alocator
    from source

)

select *
from renamed_geography

