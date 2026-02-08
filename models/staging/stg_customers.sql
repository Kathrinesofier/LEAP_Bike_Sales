with source as (

select *
from {{ source('bike_sales_src', 'customers') }}

),

renamed_customer as (

    select
        CUSTOMERKEY as customer_id,
        GEOGRAPHYKEY as geography_id,
        NAME as customer_name,
        BIRTHDATE::date as date_of_birth,
        MARITALSTATUS as marital_status,
        GENDER as gender,
        replace(YEARLYINCOME, ',', '')::number(10,2) as yearly_income,
        NUMBERCHILDRENATHOME::integer as number_children_at_home,
        OCCUPATION as occupation,
        HOUSEOWNERFLAG::boolean as is_house_owner,
        NUMBERCARSOWNED as number_cars_owned,
        ADDRESSLINE1 as address_line_1,
        ADDRESSLINE2 as address_line2,
        PHONE as phone_number,
        DATEFIRSTPURCHASE::date as first_purchase_date,
        CURRENTSTATUS as current_status,
        AGE as age
    from source

)

select *
from renamed_customer
