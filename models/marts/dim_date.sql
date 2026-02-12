with bounds as (

    select
        min(order_date_id) as min_date_id,
        max(order_date_id) as max_date_id
    from {{ ref('stg_sales') }}
    where order_date_id is not null

),

-- convert min and max dates to date types
date_limits as (

    select
        to_date(min_date_id::string, 'YYYYMMDD') as min_date,
        to_date(max_date_id::string, 'YYYYMMDD') as max_date
    from bounds

),

-- Generate a full date spine from the earliest to latest order_date_id in sales,
-- ensuring dates exist even when no transactions occurred.
date_spine as (

    select
        dateadd(
            day,
            row_number() over (order by seq4()) - 1,
            min_date
        ) as date_day
    from date_limits,
         table(generator(rowcount => 100000)) 
    qualify date_day <= max_date

)

select
    to_varchar(date_day, 'YYYYMMDD') as order_date_id,
    date_day,

    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    monthname(date_day) as month_name,

    day(date_day) as day_of_month,
    dayofweekiso(date_day) as day_of_week_iso,
    dayname(date_day) as day_name,

    weekofyear(date_day) as week_of_year,

    date_trunc('month', date_day) as month_start_date,
    last_day(date_day, 'month') as month_end_date,

    case
        when dayofweekiso(date_day) in (6,7) then true
        else false
    end as is_weekend

from date_spine
order by date_day
