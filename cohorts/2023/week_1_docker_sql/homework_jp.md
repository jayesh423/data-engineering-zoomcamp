
Assignment:
1. Which tag has the following text? - *Write the image ID to the file* 
docker build --help

2. Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

docker run -it --entrypoint bash python:3.9
root@26bc9c5646d4:/# pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4

	
3. How many taxi trips were totally made on January 15?
 select count(8) from public.green_taxi_data where date(lpep_p
 ickup_datetime)='2019-01-15' and date(lpep_dropoff_datetime)='2019-01-15' limit 10
+-------+
| count |
|-------|
| 20530 |
+-------+

4. Which was the day with the largest trip distance

 select date(lpep_pickup_datetime), max(trip_distance) from pu
 blic.green_taxi_data group by date(lpep_pickup_datetime)  order by 2 desc limit 10
+------------+--------+
| date       | max    |
|------------+--------|
| 2019-01-15 | 117.99 |
| 2019-01-18 | 80.96  |
| 2019-01-28 | 64.27  |

5. In 2019-01-01 how many trips had 2 and 3 passengers?

select sum(case when passenger_count=2 then 1 else 0 end) cou
 nt_2, sum(case when passenger_count=3 then 1 else 0 end)count_3 from public.green_ta
 xi_data where date(lpep_pickup_datetime)='2019-01-01' limit 10
+---------+---------+
| count_2 | count_3 |
|---------+---------|
| 1282    | 254     |
+---------+---------+

6. For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

 select dz."Zone" DO_Zone, t.tip_amount from public.green_taxi
 _data t inner join public.zones pz on t."PULocationID"=pz."LocationID" inner join pu
 blic.zones dz on t."DOLocationID"=dz."LocationID" where pz."Zone"='Astoria' order by
  t.tip_amount desc limit 3
+-------------------------------+------------+
| do_zone                       | tip_amount |
|-------------------------------+------------|
| Long Island City/Queens Plaza | 88.0       |
| Central Park                  | 30.0       |
| <null>                        | 25.0       |




	
	
