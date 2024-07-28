--We get this dataset from Google Analytics dataset in perform it on Google Bigquery
--This project assumes that I will receive data requests from departments such as the Sales Team, Production Team, and Purchasing Team. 
--Based on these requests, I will perform data extraction, filtering, and calculations as required. 

-- query 1
-- Calculate Quantity of items, Sales value and Order quantity by each Subcategory in last 12 months
-- The hardest problem in this query is solving "last 12 months" condition
Select 
    FORMAT_DATETIME('%h %Y', a.ModifiedDate) as period
    , b.Subcategory
    , sum(OrderQty) as num_item
    , sum(LineTotal) as total_sale
    , count(distinct salesorderid) as order_cnt
from `adventureworks2019.Sales.SalesOrderDetail` a
left join `adventureworks2019.Sales.Product` b using (productid)
where date(ModifiedDate) >= (
                            select date_sub(cast(max(modifieddate) as datetime), interval 12 MONTH)
                            from `adventureworks2019.Sales.SalesOrderDetail`)
group by 1,2
order by 1 DESC, 2 ASC;

--query 2
--Calculate % YoY growth rate by SubCategory then round it to 2 decimals and show top 3 categories with highest grow rate
with 
sale_info as (
  SELECT 
      FORMAT_TIMESTAMP("%Y", a.ModifiedDate) as yr
      , c.Name
      , sum(a.OrderQty) as qty_item

  FROM `adventureworks2019.Sales.SalesOrderDetail` a 
  LEFT JOIN `adventureworks2019.Production.Product` b on a.ProductID = b.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c on cast(b.ProductSubcategoryID as int) = c.ProductSubcategoryID

  GROUP BY 1,2
  ORDER BY 2 asc , 1 desc
    ),
sale_diff as (
  select *
  , lead (qty_item) over (partition by Name order by yr desc) as prv_qty
  , round(qty_item / (lead (qty_item) over (partition by Name order by yr desc)) -1,2) as qty_diff
  from sale_info
  order by 5 desc 
    ),
rk_qty_diff as (
  select *
      ,dense_rank() over( order by qty_diff desc) dk
  from sale_diff
    )

select distinct Name
      , qty_item
      , prv_qty
      , qty_diff
      , dk
from rk_qty_diff 
where dk <=3
order by dk
;

--query 3
--Ranking Top 3 TeritoryID with biggest Order quantity of every year.
-- Use DENSE_RANK to not skip the rank number if they have same quantity in a year
with raw_data as (
    SELECT 
        format_datetime('%Y' ,a.ModifiedDate) year
        , TerritoryID
        , sum(OrderQty) order_cnt
    from `adventureworks2019.Sales.SalesOrderDetail` a 
    left join `adventureworks2019.Sales.SalesOrderHeader` b 
    USING (salesorderid)
    group by 1,2)

select 
   *
from (
      select *
            , dense_rank() over (partition by year 
                          order by order_cnt DESC) ranking
      from raw_data)
where ranking <= 3
order by year DESC, ranking;

-- query 4
-- Calculate Total Discount Cost belongs to Seasonal Discount for each SubCategory
with filter_data as
        (select 
            format_datetime('%Y', a.ModifiedDate) year
            , subcategory
            , (unitprice * UnitPriceDiscount * orderqty) total_cost
        from `adventureworks2019.Sales.SalesOrderDetail` a
        left join `adventureworks2019.Sales.SpecialOffer` b
        using (specialofferid)
        left join `adventureworks2019.Sales.Product` c
        using (productid)
        where lower(type) like '%seasonal discount%')

select filter_data.year
        , subcategory 
        , sum(total_cost)
from filter_data
group by 1,2
order by year;

--query 5
-- Calculate Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
with overview as (
    select 
        extract(month from modifieddate) month_order
        , extract(year from modifieddate) year
        , customerid
        , count(distinct salesorderid) sale_cnt
    from `adventureworks2019.Sales.SalesOrderHeader` 
    where status = 5 
        and extract(year from modifieddate) = 2014
    group by 1,2,3)
    ,
    filter_data as (
      select *
          , ROW_NUMBER() over (partition by customerid order by month_order)  row_num
      from overview)
    ,
    first_order as (
    select 
        distinct month_order as month_join, year, customerid
    from filter_data
    where row_num = 1)
    ,
    all_join as (
    select 
        distinct a.month_order
        , a.year
        , a.customerid
        , b.month_join
        , concat('M', '-' ,a.month_order - b.month_join) as month_diff
    from overview a
    left join first_order b using(customerid)
    order by 1,3)

select month_join
      , month_diff
      , count(distinct customerid) customer_cnt
from all_join
group by 1,2
order by 1,2 ;

-- query 6
-- Show trend of Stock level and MoM difference % by all product in 2011.
-- Notice that growth rate can be null so that i convert null values to be 0  
with raw_data as
   ( select 
        Name
        , extract(month from a.modifieddate) mth
        , extract(year from a.modifieddate) yr
        , sum(stockedqty)  stock_qty
    from `adventureworks2019.Production.WorkOrder` a
    left join `adventureworks2019.Production.Product` b
    using (productid)
    where extract(year from a.modifieddate) = 2011
    group by 1,2,3
    order by name, mth DESC)
    ,
    filter_data as
   ( select *
          , Lead(stock_qty) over (partition by name
                                    order by mth DESC) prv_stock
    from raw_data
    order by name, mth DESC)

select * 
      , case when prv_stock is not null 
                  then round((stock_qty - prv_stock)/ prv_stock * 100, 1)
             else 0 end as gr_rate
from filter_data;

--query 7
--Calculate Ratio of stock/Sales in 2011 by product name and month
--Arrange results by month descending
--Use COALESCE to avoid null values then turn it into 0 value
with 
sale_info as (
  select 
      extract(month from a.ModifiedDate) as mth 
     , extract(year from a.ModifiedDate) as yr 
     , a.ProductId
     , b.Name
     , sum(a.OrderQty) as sales
  from `adventureworks2019.Sales.SalesOrderDetail` a 
  left join `adventureworks2019.Production.Product` b 
    on a.ProductID = b.ProductID
  where FORMAT_TIMESTAMP("%Y", a.ModifiedDate) = '2011'
  group by 1,2,3,4
), 

stock_info as (
  select
      extract(month from ModifiedDate) as mth 
      , extract(year from ModifiedDate) as yr 
      , ProductId
      , sum(StockedQty) as stock_cnt
  from `adventureworks2019.Production.WorkOrder`
  where FORMAT_TIMESTAMP("%Y", ModifiedDate) = '2011'
  group by 1,2,3
)

select
      a.*
    , coalesce(b.stock_cnt,0) as stock
    , round(coalesce(b.stock_cnt,0) / sales,2) as ratio
from sale_info a 
full join stock_info b 
  on a.ProductId = b.ProductId
and a.mth = b.mth 
and a.yr = b.yr
order by 1 desc, 7 desc
;

-- query 8
-- Find out number of order and total value at Pending status in 2014
SELECT 
    extract(year from modifieddate) as year
    , status
    , count(distinct purchaseorderid) ord_cnt
    , sum(totaldue) total_value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader` 
where extract(year from modifieddate) = 2014
      and status = 1
group by 1,2;