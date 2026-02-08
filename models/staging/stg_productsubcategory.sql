with source as (

select *
from {{ source('bike_sales_src', 'productsubcategory') }}
), 

renamed_productsubcategory as (

    select
        PRODUCTSUBCATEGORYKEY as product_sub_category_id,
        PRODUCTCATEGORYKEY as product_category_id,
        PRODUCT_SUBCATEGORY as product_sub_category_name
    from source

)

select *
from renamed_productsubcategory