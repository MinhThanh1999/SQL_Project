--We get this E-commerce dataset from Google Analytics dataset and perform it on Google Bigquery
--We have 8 queries, each addressing a different problem about E-commerce, as shown below

--query 1
--Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'
GROUP BY 1
ORDER BY 1;

--query 2 
--Calculate Bounce rate per traffic source in July 2017 (where Bounce_rate = num_bounce/total_visit) then place order by total_visit DESCENDING
SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;


-- query 3
--Calculate revenue by traffic source by week, by month in June 2017
with 
month_data as(
  SELECT
    "Month" as time_type,
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    trafficSource.source AS source,
    SUM(p.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
    unnest(hits) hits,
    unnest(product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  order by revenue DESC
),

week_data as(
  SELECT
    "Week" as time_type,
    format_date("%Y%W", parse_date("%Y%m%d", date)) as week,
    trafficSource.source AS source,
    SUM(p.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
    unnest(hits) hits,
    unnest(product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  order by revenue DESC
)

select * from month_data
union all
select * from week_data;

-- query 4
-- Calculate average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017
with cte as (
    SELECT date, totals.transactions, totals.pageviews, fullVisitorId, product.productRevenue FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
    where _table_suffix between '0601' and '0731')  
    ,
    purchase_data as
    (select 
        FORMAT_DATE('%Y%m', parse_date('%Y%m%d', date)) as  month
        , SUM(pageviews) / count(distinct fullvisitorid) as avg_pageview_purchase
    from cte
    where productRevenue is not null
        and transactions >= 1
    group by month)
    ,
    non_purchase_data as (
        select 
        FORMAT_DATE('%Y%m', parse_date('%Y%m%d', date)) as  month
        , SUM(pageviews) / count(distinct fullvisitorid) as avg_pageview_non_purchase
    from cte
    where productRevenue is null
        and transactions is null
    group by month)
    
select *
from non_purchase_data
FULL JOIN purchase_data
using(month)
order by month;

--query 5
-- Calculate average number of transactions per user that made a purchase in July 2017
with cte as (
    SELECT date, totals.transactions, fullVisitorId, product.productRevenue FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
    where _table_suffix between '0701' and '0731') 

select month, 
    (total_transaction / num) as avg_per_user_purchase
from   (SELECT 
        count(distinct fullvisitorid) as num
        , FORMAT_DATE('%Y%m', parse_date('%Y%m%d', date)) as  month
        , sum(transactions)  as total_transaction
        from cte
        where transactions >= 1 and productrevenue is not null
        group by month) as m
group by month, m.total_transaction, num;

-- query 6
-- Calculate average amount of money spent per session. Only include purchaser data in July 2017
with cte as (
    SELECT date, totals.transactions, fullVisitorId, product.productRevenue, totals.visits,  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
    where _table_suffix between '0701' and '0731'
            and productrevenue is not null
            and totals.transactions >= 1) 

select 
    FORMAT_DATE('%Y%m', parse_date('%Y%m%d', date)) as  month
    , round((sum(productrevenue) / count(visits) / 1000000), 2) as avg_spend_per_session
from cte
group by month;

--query 7
-- Figure out other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. The output only show product name and the quantity was ordered
with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC;

--query 8
--Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017
--Where Add_to_cart_rate = number product add_to_cart/number_product_view, Purchase_rate = number_product_purchase/number_product_view
with
product_view as(
  SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    count(product.productSKU) as num_product_view
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  , UNNEST(hits) AS hits
  , UNNEST(hits.product) as product
  WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND hits.eCommerceAction.action_type = '2'
  GROUP BY 1
),

add_to_cart as(
  SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    count(product.productSKU) as num_addtocart
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  , UNNEST(hits) AS hits
  , UNNEST(hits.product) as product
  WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND hits.eCommerceAction.action_type = '3'
  GROUP BY 1
),

purchase as(
  SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    count(product.productSKU) as num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  , UNNEST(hits) AS hits
  , UNNEST(hits.product) as product
  WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND hits.eCommerceAction.action_type = '6'
  and product.productRevenue is not null   
  group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
left join add_to_cart a on pv.month = a.month
left join purchase p on pv.month = p.month
order by pv.month;
