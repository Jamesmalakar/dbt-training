select 
-- from raw orders
{{ dbt_utils.generate_surrogate_key(['o.orderid', 'c.customerid', 'p.productid']) }} as sk_orders ,
o.orderid,
o.orderdate,
o.shipdate,
o.shipmode,
o.ordersellingprice,
o.ordercostprice,
o.ordersellingprice - o.ordercostprice as orderprofit 
, c.customerid
, c.customername
, c.segment
, c.country
, p.category
, p.productname
, p.subcategory
, p.productid
, d.delivery_team

from 
{{ ref('raw_orders') }} as o 
left join {{ref('raw_customers')}} as c 
On o.customerid = c.customerid
left join {{ref('raw_products')}} as p 
On o.productid = p.productid
left join {{ref("delivery_team")}}  as d 
On o.shipmode = d.shipmode 










