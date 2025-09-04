DROP TABLE IF EXISTS move_month_subsample;
CREATE TABLE move_month_subsample AS
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

    -- 新增 period 列（yyyymm）
    substr(m.`date`,1,6) AS period,

    -- 本地标记（新定义：is_core='Y' 且 id_area 前两位='11'）
    CASE
        WHEN m.is_core = 'Y'
         AND ua.id_area IS NOT NULL
         AND substr(ua.id_area,1,2) = '11'
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
WHERE m.city = 'V0110000'
  AND (
       (m.`date` BETWEEN '20220801' AND '20220831')
    OR (m.`date` BETWEEN '20231101' AND '20231130')
  );

-- Discrepancy btw 2 periods
-- Let's check the grids with the largest frequency difference

-- 1) 早间 07:00–11:59 到达：正差异最大 1000（end_grid_id）
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
  FROM move_month_subsample
  WHERE etime IS NOT NULL
  GROUP BY end_grid_id
) t
JOIN grid g ON g.grid_id = t.grid_id
ORDER BY t.diff_cnt DESC;

-- 2) 早间 07:00–11:59 到达：负差异最大 1000（end_grid_id）
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
  FROM move_month_subsample
  WHERE etime IS NOT NULL
  GROUP BY end_grid_id
) t
JOIN grid g ON g.grid_id = t.grid_id
ORDER BY t.diff_cnt ASC;

-- 3) 午后 12:00–23:59 出发：正差异最大 1000（start_grid_id）
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
  FROM move_month_subsample
  WHERE stime IS NOT NULL
  GROUP BY start_grid_id
) t
JOIN grid g ON g.grid_id = t.grid_id
ORDER BY t.diff_cnt DESC;

-- 4) 午后 12:00–23:59 出发：负差异最大 1000（start_grid_id）
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
  FROM move_month_subsample
  WHERE stime IS NOT NULL
  GROUP BY start_grid_id
) t
JOIN grid g ON g.grid_id = t.grid_id
ORDER BY t.diff_cnt ASC;

-- Refined demographics, age groups

SELECT
  t.window_label,         -- '07:00-11:59(arrival)' 或 '12:00-23:59(departure)'
  t.period,               -- 202208 / 202311
  t.age_group,            -- 1-4
  t.cnt,                  -- 该窗 × period × 年龄组 的条数
  ROUND(t.cnt * 100.0 / SUM(t.cnt) OVER (PARTITION BY t.window_label, t.period), 2) AS pct
FROM (
  -- 早间 07:00–11:59，用 etime（按“到达”口径）
  SELECT
    '07:00-11:59(arrival)' AS window_label,
    period,
    age_group,
    COUNT(*) AS cnt
  FROM move_month_subsample
  WHERE age_group IS NOT NULL
    AND etime IS NOT NULL
    AND CAST(SUBSTR(etime,12,2) AS INT) BETWEEN 7 AND 11
  GROUP BY period, age_group

  UNION ALL

  -- 午后 12:00–23:59，用 stime（按“出发”口径）
  SELECT
    '12:00-23:59(departure)' AS window_label,
    period,
    age_group,
    COUNT(*) AS cnt
  FROM move_month_subsample
  WHERE age_group IS NOT NULL
    AND stime IS NOT NULL
    AND CAST(SUBSTR(stime,12,2) AS INT) BETWEEN 12 AND 23
  GROUP BY period, age_group
) t
ORDER BY t.window_label, t.period, t.age_group;
