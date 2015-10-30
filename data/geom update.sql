UPDATE schelling SET geom = subquery.geom
FROM (SELECT gid,geom FROM manzanas) as subquery
WHERE schelling.gid = subquery.gid;