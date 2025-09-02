DROP TABLE IF EXISTS move_month_subsample_xm;
CREATE TABLE move_month_subsample_xm AS
SELECT
    m.uid,
    m.move_id,
    m.stime,
    m.etime,
    m.mode,
    m.start_grid_id,
    m.end_grid_id,
    m.distance,
    m.`time`,
    m.moi_id,
    m.is_core,
    m.start_ptype,
    m.end_ptype,
    m.province,
    m.city,
    m.`date`,

    -- period (yyyymm)
    substr(m.`date`,1,6) AS period,

    -- 本地标记（厦门：is_core='Y' 且 id_area 前四位='3502'）
    CASE
        WHEN m.is_core = 'Y'
         AND ua.id_area IS NOT NULL
         AND substr(ua.id_area,1,4) = '3502'
        THEN 1 ELSE 0
    END AS is_local,

    -- 用户属性
    ua.gender,
    ua.age,
    ua.id_area,

    -- 年龄分组
    CASE
        WHEN ua.age IN ('01','02','03','04')               THEN 1
        WHEN ua.age IN ('05','06','07')                    THEN 2
        WHEN ua.age IN ('08','09','10')                    THEN 3
        WHEN ua.age IN ('11','12','13','14','15')          THEN 4
        ELSE NULL
    END AS age_group

FROM move_month m
LEFT JOIN (
    SELECT DISTINCT uid, gender, age, id_area
    FROM user_attribute
) ua
  ON m.uid = ua.uid
WHERE m.city = 'V0350200'
  AND (
       (m.`date` BETWEEN '20220801' AND '20220831')
    OR (m.`date` BETWEEN '20231101' AND '20231130')
  );

-- 1) 早间 07:00–11:59 到达：正差异 Top1000（end_grid_id）
SELECT t.grid_id, t.diff_cnt, g.centroid_lat, g.centroid_lon
FROM (
  SELECT
    end_grid_id AS grid_id,
    SUM(CASE WHEN period = '202311'
              AND CAST(SUBSTR(etime,12,2) AS INT) BETWEEN 7 AND 11
             THEN 1 ELSE 0 END)
  - SUM(CASE WHEN period = '202208'
              AND CAST(SUBSTR(etime,12,2) AS INT) BETWEEN 7 AND 11
             THEN 1 ELSE 0 END) AS diff_cnt
  FROM move_month_subsample_xm
  WHERE etime IS NOT NULL
  GROUP BY end_grid_id
) t
JOIN grid g ON g.grid_id = t.grid_id
-- AND g.city = 'V0350200'
ORDER BY t.diff_cnt DESC
LIMIT 1000;

-- 2) 早间 07:00–11:59 到达：负差异 Top1000（end_grid_id）
SELECT t.grid_id, t.diff_cnt, g.centroid_lat, g.centroid_lon
FROM (
  SELECT
    end_grid_id AS grid_id,
    SUM(CASE WHEN period = '202311'
              AND CAST(SUBSTR(etime,12,2) AS INT) BETWEEN 7 AND 11
             THEN 1 ELSE 0 END)
  - SUM(CASE WHEN period = '202208'
              AND CAST(SUBSTR(etime,12,2) AS INT) BETWEEN 7 AND 11
             THEN 1 ELSE 0 END) AS diff_cnt
  FROM move_month_subsample_xm
  WHERE etime IS NOT NULL
  GROUP BY end_grid_id
) t
JOIN grid g ON g.grid_id = t.grid_id
-- AND g.city = 'V0350200'
ORDER BY t.diff_cnt ASC
LIMIT 1000;

-- 3) 午后 12:00–23:59 出发：正差异 Top1000（start_grid_id）

SELECT t.grid_id, t.diff_cnt, g.centroid_lat, g.centroid_lon
FROM (
  SELECT
    start_grid_id AS grid_id,
    SUM(CASE WHEN period = '202311'
              AND CAST(SUBSTR(stime,12,2) AS INT) BETWEEN 12 AND 23
             THEN 1 ELSE 0 END)
  - SUM(CASE WHEN period = '202208'
              AND CAST(SUBSTR(stime,12,2) AS INT) BETWEEN 12 AND 23
             THEN 1 ELSE 0 END) AS diff_cnt
  FROM move_month_subsample_xm
  WHERE stime IS NOT NULL
  GROUP BY start_grid_id
) t
JOIN grid g ON g.grid_id = t.grid_id
-- AND g.city = 'V0350200'
ORDER BY t.diff_cnt DESC
LIMIT 1000;

-- 4) 午后 12:00–23:59 出发：负差异 Top1000（start_grid_id）
SELECT t.grid_id, t.diff_cnt, g.centroid_lat, g.centroid_lon
FROM (
  SELECT
    start_grid_id AS grid_id,
    SUM(CASE WHEN period = '202311'
              AND CAST(SUBSTR(stime,12,2) AS INT) BETWEEN 12 AND 23
             THEN 1 ELSE 0 END)
  - SUM(CASE WHEN period = '202208'
              AND CAST(SUBSTR(stime,12,2) AS INT) BETWEEN 12 AND 23
             THEN 1 ELSE 0 END) AS diff_cnt
  FROM move_month_subsample_xm
  WHERE stime IS NOT NULL
  GROUP BY start_grid_id
) t
JOIN grid g ON g.grid_id = t.grid_id
-- AND g.city = 'V0350200'
ORDER BY t.diff_cnt ASC
LIMIT 1000;

-- Refined demographics, age groups

SELECT
  x.window_label,          -- '07:00-11:59(arrival)' / '12:00-23:59(departure)'
  x.period,                -- 202208 / 202311
  x.age_group,             -- 1-4
  x.cnt,
  ROUND(x.cnt * 100.0 / SUM(x.cnt) OVER (PARTITION BY x.window_label, x.period), 2) AS pct
FROM (
  SELECT
    '07:00-11:59(arrival)' AS window_label,
    period,
    age_group,
    COUNT(*) AS cnt
  FROM move_month_subsample_xm
  WHERE age_group IS NOT NULL
    AND etime IS NOT NULL
    AND CAST(SUBSTR(etime,12,2) AS INT) BETWEEN 7 AND 11
  GROUP BY period, age_group

  UNION ALL

  SELECT
    '12:00-23:59(departure)' AS window_label,
    period,
    age_group,
    COUNT(*) AS cnt
  FROM move_month_subsample_xm
  WHERE age_group IS NOT NULL
    AND stime IS NOT NULL
    AND CAST(SUBSTR(stime,12,2) AS INT) BETWEEN 12 AND 23
  GROUP BY period, age_group
) x
ORDER BY x.window_label, x.period, x.age_group;
