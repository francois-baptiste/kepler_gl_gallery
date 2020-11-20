With T0 AS( 
SELECT population, libjs4us.h3.ST_H3(ST_GEOGPOINT(longitude_centroid, latitude_centroid), 10) key FROM `bigquery-public-data.worldpop.population_grid_1km` WHERE last_updated = "2017-01-01"
),
T1 AS (
SELECT sum(population) population, key
from T0
group by key),
T2 AS (SELECT
  array (SELECT
    struct(libjs4us.h3.h3ToParent(key,len) as key, population) mystruct
  FROM
    UNNEST(GENERATE_ARRAY(0,libjs4us.h3.h3GetResolution(key))) AS len) keys
FROM
  T1),
T3 AS (select mystruct.key, sum(mystruct.population) as count from T2,
unnest(keys) as mystruct
group by mystruct.key
order by mystruct.key),
T8 AS(Select key, count
from T3
where count > 1000000 or libjs4us.h3.h3GetResolution(key)=0
),
T9 AS (SELECT count, 
  array (SELECT
    libjs4us.h3.h3ToParent(key,len) 
  FROM
    UNNEST(GENERATE_ARRAY(greatest(libjs4us.h3.h3GetResolution(key)-1,0),libjs4us.h3.h3GetResolution(key))) AS len) keys
FROM
  T8),
T10 AS (select key, sum(count) as count, count(*) as cellnb from T9,
unnest(keys) as key
group by key
order by key),
T11 AS (select key, count from T10 where cellnb>1 or libjs4us.h3.h3GetResolution(key)=0),
T12 AS (select keys as key,count
from T11, unnest(libjs4us.h3.h3ToChildren(key,libjs4us.h3.h3GetResolution(key)+1)) keys
UNION ALL
select key, 0 from unnest(libjs4us.h3.kRing(libjs4us.h3.geoToH3(0,0,0), 100)) key),
T13 AS (select key, count  from T12
where key not in (SELECT key from T11)),
T14 AS (
select libjs4us.h3.h3GetResolution(key) res, key, count,
from T13)
select key from T14
order by libjs4us.h3.h3GetResolution(key)
