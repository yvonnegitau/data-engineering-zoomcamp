## How many taxi trips were there on January 15? Consider only trips that started on January 15.
select  count(*) from yellow_taxi_data where to_char(tpep_pickup_datetime, 'mm-dd') = '01-15';
Answer: 53024

## Find the largest tip for each day. On which day it was the largest tip in January?
select  max(tip_amount) as max_tip, to_char(tpep_pickup_datetime, 'mm-dd') from yellow_taxi_data GROUP BY to_char(tpep_pickup_datetime, 'mm-dd') order by max_tip DESC ;
Answer: 20th January

## What was the most popular destination for passengers picked up in central park on January 14?
Note: Standard SQL disallows references to column aliases in a WHERE clause. This restriction is imposed because when the WHERE clause is evaluated, the column value may not yet have been determined.

select  z_do."Zone" as do_name,  count(z_do."Zone")from yellow_taxi_data as y
inner join zones as z_pu on y."PULocationID" = z_pu."LocationID" 
left join zones as z_do on  y."DOLocationID" = z_do."LocationID"
where to_char(tpep_pickup_datetime, 'mm-dd') = '01-14' and z_pu."Zone" = 'Central Park'
group by z_do."Zone"
order by count(z_do."Zone") DESC;

Answer: Upper East Side South

## What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)?

Enter two zone names separated by a slash For example: "Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

select 
concat(COALESCE(z_pu."Zone", 'Unknown'),'/',COALESCE(z_do."Zone", 'Unknown')) as zone_name, avg(y.total_amount) as total_price
from yellow_taxi_data as y
inner join zones as z_pu on y."PULocationID" = z_pu."LocationID" 
left join zones as z_do on  y."DOLocationID" = z_do."LocationID"
group by zone_name
order by total_price DESC
limit 1;

Answer: Alphabet City/Unknown

