with source as (

select *
from {{ source('bike_sales_src', 'sales') }}
), 

renamed_sales as (

    select
        RECORDKEY as sales_id,
        PRODUCTKEY as product_id,
        ORDERDATEKEY as order_date_id,
        CUSTOMERKEY as customer_id,
        SALESORDERNUMBER as sales_order_number,
        ORDERQUANTITY as order_qty,
        UNITPRICE as unit_price, 
        EXTENDEDAMOUNT as extended_amount, 
        UNITPRICEDISCOUNTPCT as unit_price_discount_pct,
        DISCOUNTAMOUNT as discount_amount,
        PRODUCTSTANDARDCOST as product_standard_cost,
        TOTALPRODUCTCOST as total_product_cost,
        SALESAMOUNT as sales_amount,
        TAXAMT as tax_amount,
        FREIGHT as freight,
        REGEXP_REPLACE(REGIONMONTHID, '[0-9]+$', '') AS region
    from source

)

select *
from renamed_sales
