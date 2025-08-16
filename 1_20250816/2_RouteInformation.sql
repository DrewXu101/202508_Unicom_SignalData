-- Movement
-- August 
DROP TABLE IF EXISTS tmp_monthly_pairs_202208;
CREATE TABLE tmp_monthly_pairs_202208 AS
SELECT DISTINCT
  m.uid,
  m.moi_id
FROM move_month m
WHERE m.date BETWEEN 20220801 AND 20220831
  AND m.city = 'V0110000'
-- AND m.city = 'V0110000'   -- 如需限定城市，放开这一行
;

DROP TABLE IF EXISTS tmp_routes_pairs_202208;
CREATE TABLE tmp_routes_pairs_202208 AS
SELECT DISTINCT
  r.uid,
  r.moi_id
FROM move_rn r
WHERE r.date BETWEEN 20220801 AND 20220831
  AND r.city = 'V0110000'
-- AND r.city = 'V0110000'   -- 如需限定城市，放开这一行
;

-- 汇总
SELECT
  a.total_trips,
  b.matched_trips,
  CASE
    WHEN a.total_trips = 0 THEN NULL
    ELSE ROUND(100.0 * b.matched_trips / a.total_trips, 2)
  END AS pct_with_route
FROM
  ( SELECT COUNT(1) AS total_trips
    FROM tmp_monthly_pairs_202208
  ) a
CROSS JOIN
  ( SELECT COUNT(1) AS matched_trips
    FROM tmp_monthly_pairs_202208 m
    LEFT SEMI JOIN tmp_routes_pairs_202208 r
      ON r.uid = m.uid AND r.moi_id = m.moi_id
  ) b;

-- November
DROP TABLE IF EXISTS tmp_monthly_pairs_202311;
CREATE TABLE tmp_monthly_pairs_202311 AS
SELECT DISTINCT
  m.uid,
  m.moi_id
FROM move_month m
WHERE m.date BETWEEN 20231101 AND 20231130
  AND m.city = 'V0110000'
-- AND m.city = 'V0110000'   -- 如需限定城市，放开这一行
;

DROP TABLE IF EXISTS tmp_routes_pairs_202311;
CREATE TABLE tmp_routes_pairs_202311 AS
SELECT DISTINCT
  r.uid,
  r.moi_id
FROM move_rn r
WHERE r.date BETWEEN 20231101 AND 20231130
  AND r.city = 'V0110000'
-- AND r.city = 'V0110000'   -- 如需限定城市，放开这一行
;

-- 汇总
SELECT
  a.total_trips,
  b.matched_trips,
  CASE
    WHEN a.total_trips = 0 THEN NULL
    ELSE ROUND(100.0 * b.matched_trips / a.total_trips, 2)
  END AS pct_with_route
FROM
  ( SELECT COUNT(1) AS total_trips
    FROM tmp_monthly_pairs_202311
  ) a
CROSS JOIN
  ( SELECT COUNT(1) AS matched_trips
    FROM tmp_monthly_pairs_202311 m
    LEFT SEMI JOIN tmp_routes_pairs_202311 r
      ON r.uid = m.uid AND r.moi_id = m.moi_id
  ) b;