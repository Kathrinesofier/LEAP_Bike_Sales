with source as (

select *
from {{ source('bike_sales_src', 'products') }}
), 

renamed_products as (

    select
        PRODUCTKEY as product_id,
        PRODUCTSUBCATEGORYKEY as product_sub_category_id,
        PRODUCTNAME as product_name,
        STANDARDCOST as standard_cost, 
        COLOR as color,
        SAFETYSTOCKLEVEL as safety_stock_level,
        LISTPRICE as list_price,
        SIZERANGE as size_range,
        WEIGHT as weight,
        DAYSTOMANUFACTURE as days_to_manufacture,
        PRODUCTLINE as product_line,
        DEALERPRICE as dealer_price,
        CLASS as class,
        MODELNAME as model_name,
        DESCRIPTION as description,
        STARTDATE::date as start_date, 
        ENDDATE::date as end_date,
        STATUS as status,
        CURRENTSTOCK as current_stock
    from source

)

select *
from renamed_products

