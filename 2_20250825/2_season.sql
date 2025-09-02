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
        
-- is_local 
SELECT
        substr(`date`,
        1,
        6) AS stat_month,
-- 抽取年月
  is_local,
        COUNT(*) AS n_obs,
        ROUND( COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION   
    BY
        substr(`date`,
        1,
        6)),
        2 ) AS pct   
    FROM
        move_month_subsample   
    GROUP BY
        substr(`date`,
        1,
        6),
        is_local   
    ORDER BY
        stat_month,
        is_local; 
        
        
-- is_local
SELECT
        substr(`date`, 1, 6) AS stat_month,
        is_local,
        COUNT(*) AS n_obs,
        COUNT(DISTINCT uid) AS n_uid,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION 
    BY
        substr(`date`, 1, 6)), 2) AS pct,
        AVG(distance) AS avg_distance,
        AVG(`time`) AS avg_time 
    FROM
        move_month_subsample 
    GROUP BY
        is_local,
        substr(`date`,
        1,
        6) 
    ORDER BY
        stat_month,
        n_obs DESC; 
        


-- Grid Analysis

-- Overall, w/o finer deomographical groups
-- start place 
-- 1) 202208 · AM_7_12
SELECT
  '202208'  AS stat_month,
  'AM_7_12' AS time_window,
  t.start_grid_id,
  gc.centroid_lat AS start_lat,
  gc.centroid_lon AS start_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 1982091330.0 AS cnt_share_in_month
FROM (
  SELECT start_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, start_grid_id) AS rn
  FROM (
    SELECT start_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample
    WHERE substr(`date`,1,6) = '202208'
      AND hour(stime) BETWEEN 7 AND 12
      AND start_grid_id <> '-1'
    GROUP BY start_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.start_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.start_grid_id;

-- 2) 202208 · PM_13_23
SELECT
  '202208'  AS stat_month,
  'PM_13_23' AS time_window,
  t.start_grid_id,
  gc.centroid_lat AS start_lat,
  gc.centroid_lon AS start_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 1982091330.0 AS cnt_share_in_month
FROM (
  SELECT start_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, start_grid_id) AS rn
  FROM (
    SELECT start_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample
    WHERE substr(`date`,1,6) = '202208'
      AND hour(stime) BETWEEN 13 AND 23
      AND start_grid_id <> '-1'
    GROUP BY start_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.start_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.start_grid_id;

-- 3) 202311 · AM_7_12
SELECT
  '202311'  AS stat_month,
  'AM_7_12' AS time_window,
  t.start_grid_id,
  gc.centroid_lat AS start_lat,
  gc.centroid_lon AS start_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 973917412.0 AS cnt_share_in_month
FROM (
  SELECT start_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, start_grid_id) AS rn
  FROM (
    SELECT start_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample
    WHERE substr(`date`,1,6) = '202311'
      AND hour(stime) BETWEEN 7 AND 12
      AND start_grid_id <> '-1'
    GROUP BY start_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.start_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.start_grid_id;

-- 4) 202311 · PM_13_23
SELECT
  '202311'  AS stat_month,
  'PM_13_23' AS time_window,
  t.start_grid_id,
  gc.centroid_lat AS start_lat,
  gc.centroid_lon AS start_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 973917412.0 AS cnt_share_in_month
FROM (
  SELECT start_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, start_grid_id) AS rn
  FROM (
    SELECT start_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample
    WHERE substr(`date`,1,6) = '202311'
      AND hour(stime) BETWEEN 13 AND 23
      AND start_grid_id <> '-1'
    GROUP BY start_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.start_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.start_grid_id;


-- end place
-- 1) 202208 · AM_7_12
SELECT
  '202208'  AS stat_month,
  'AM_7_12' AS time_window,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 1982091330.0 AS cnt_share_in_month
FROM (
  SELECT end_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, end_grid_id) AS rn
  FROM (
    SELECT end_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample
    WHERE substr(`date`,1,6) = '202208'
      AND hour(etime) BETWEEN 7 AND 12
      AND end_grid_id <> '-1'
    GROUP BY end_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.end_grid_id;

-- 2) 202208 · PM_13_23
SELECT
  '202208'  AS stat_month,
  'PM_13_23' AS time_window,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 1982091330.0 AS cnt_share_in_month
FROM (
  SELECT end_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, end_grid_id) AS rn
  FROM (
    SELECT end_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample
    WHERE substr(`date`,1,6) = '202208'
      AND hour(etime) BETWEEN 13 AND 23
      AND end_grid_id <> '-1'
    GROUP BY end_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.end_grid_id;

-- 3) 202311 · AM_7_12
SELECT
  '202311'  AS stat_month,
  'AM_7_12' AS time_window,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 973917412.0 AS cnt_share_in_month
FROM (
  SELECT end_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, end_grid_id) AS rn
  FROM (
    SELECT end_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample
    WHERE substr(`date`,1,6) = '202311'
      AND hour(etime) BETWEEN 7 AND 12
      AND end_grid_id <> '-1'
    GROUP BY end_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.end_grid_id;

-- 4) 202311 · PM_13_23
SELECT
  '202311'  AS stat_month,
  'PM_13_23' AS time_window,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 973917412.0 AS cnt_share_in_month
FROM (
  SELECT end_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, end_grid_id) AS rn
  FROM (
    SELECT end_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample
    WHERE substr(`date`,1,6) = '202311'
      AND hour(etime) BETWEEN 13 AND 23
      AND end_grid_id <> '-1'
    GROUP BY end_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.end_grid_id;

-- non_local people?
-- 1) 按 uid 统计两期是否出现
DROP TABLE IF EXISTS uid_period_presence_tmp;
CREATE TABLE uid_period_presence_tmp AS
SELECT
  uid,
  MAX(CASE WHEN period = '202208' THEN 1 ELSE 0 END) AS has_202208,
  MAX(CASE WHEN period = '202311' THEN 1 ELSE 0 END) AS has_202311
FROM move_month_subsample
WHERE period IN ('202208','202311')
GROUP BY uid
;

-- 2) 回连到明细表，生成居住类型分组
DROP TABLE IF EXISTS move_month_subsample_segmented;
CREATE TABLE move_month_subsample_segmented AS
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
  m.period,
  m.is_local,
  m.gender,
  m.age,
  m.id_area,
  m.age_group,
  CASE
    WHEN m.is_local = 1 THEN 'local'
    WHEN m.is_local = 0 AND p.has_202208 = 1 AND p.has_202311 = 1 THEN 'semi_local'
    WHEN m.is_local = 0 AND (p.has_202208 + p.has_202311) = 1 THEN 'non_local'
    ELSE 'unknown'
  END AS residency_group
FROM move_month_subsample m
LEFT JOIN uid_period_presence_tmp p
  ON m.uid = p.uid
WHERE m.period IN ('202208','202311')
;

-- factor in semi_local group 
SELECT
        substr(`date`, 1, 6) AS stat_month,
        residency_group,
        COUNT(*) AS n_obs,
        COUNT(DISTINCT uid) AS n_uid,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION 
    BY
        substr(`date`, 1, 6)), 2) AS pct,
        AVG(distance) AS avg_distance,
        AVG(`time`) AS avg_time 
    FROM
        move_month_subsample_segmented 
    GROUP BY
        residency_group,
        substr(`date`,
        1,
        6) 
    ORDER BY
        stat_month,
        n_obs DESC; 
        
-- collapsed semi_local and local into the same group 

SELECT
  stat_month,
  residency_group_collapsed AS residency_group,
  COUNT(*)                         AS n_obs,
  COUNT(DISTINCT uid)              AS n_uid,
  ROUND(
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY stat_month)
  , 2)                              AS pct,
  AVG(distance)                    AS avg_distance,
  AVG(`time`)                      AS avg_time
FROM (
  SELECT
    substr(`date`,1,6) AS stat_month,
    CASE
      WHEN residency_group IN ('local','semi_local') THEN 'local_or_semi'
      ELSE residency_group
    END AS residency_group_collapsed,
    uid,
    distance,
    `time`
  FROM move_month_subsample_segmented
  -- 如需只看这两个月，可解注释：
  -- WHERE substr(`date`,1,6) IN ('202208','202311')
) t
GROUP BY stat_month, residency_group_collapsed
ORDER BY stat_month, n_obs DESC;

-- Why non_local's average dist increases significantly? 

-- 1) 202208 · AM_7_12 · Non-local only
SELECT
  '202208'  AS stat_month,
  'AM_7_12' AS time_window,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 1982091330.0 AS cnt_share_in_month
FROM (
  SELECT end_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, end_grid_id) AS rn
  FROM (
    SELECT end_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample_segmented
    WHERE substr(`date`,1,6) = '202208'
      AND hour(etime) BETWEEN 7 AND 12
      AND end_grid_id <> '-1'
      AND residency_group = 'non_local'
    GROUP BY end_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.end_grid_id;

-- 2) 202311 · AM_7_12 · Non-local only
SELECT
  '202311'  AS stat_month,
  'AM_7_12' AS time_window,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 973917412.0 AS cnt_share_in_month
FROM (
  SELECT end_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, end_grid_id) AS rn
  FROM (
    SELECT end_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample_segmented
    WHERE substr(`date`,1,6) = '202311'
      AND hour(etime) BETWEEN 7 AND 12
      AND end_grid_id <> '-1'
      AND residency_group = 'non_local'
    GROUP BY end_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.end_grid_id;

-- 3) 202208 · PM_13_23 · Non-local only
SELECT
  '202208'  AS stat_month,
  'PM_13_23' AS time_window,
  t.start_grid_id,
  gc.centroid_lat AS start_lat,
  gc.centroid_lon AS start_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 1982091330.0 AS cnt_share_in_month
FROM (
  SELECT start_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, start_grid_id) AS rn
  FROM (
    SELECT start_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample_segmented
    WHERE substr(`date`,1,6) = '202208'
      AND hour(stime) BETWEEN 13 AND 23
      AND start_grid_id <> '-1'
      AND residency_group = 'non_local'
    GROUP BY start_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.start_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.start_grid_id;

-- 4) 202311 · PM_13_23 · Non-local only
SELECT
  '202311'  AS stat_month,
  'PM_13_23' AS time_window,
  t.start_grid_id,
  gc.centroid_lat AS start_lat,
  gc.centroid_lon AS start_lon,
  t.cnt,
  CAST(t.cnt AS DOUBLE) / 973917412.0 AS cnt_share_in_month
FROM (
  SELECT start_grid_id, cnt,
         ROW_NUMBER() OVER (ORDER BY cnt DESC, start_grid_id) AS rn
  FROM (
    SELECT start_grid_id, COUNT(*) AS cnt
    FROM move_month_subsample_segmented
    WHERE substr(`date`,1,6) = '202311'
      AND hour(stime) BETWEEN 13 AND 23
      AND start_grid_id <> '-1'
      AND residency_group = 'non_local'
    GROUP BY start_grid_id
  ) x
) t
LEFT JOIN grid gc
  ON t.start_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.cnt DESC, t.start_grid_id;

-- Why locals travel less? 
-- 202208 · local · Top 2000 busiest end_grid_id
DROP TABLE IF EXISTS top2000_local_endgrid_202208;
CREATE TABLE top2000_local_endgrid_202208 AS
SELECT end_grid_id, cnt
FROM (
  SELECT end_grid_id, COUNT(*) AS cnt,
         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, end_grid_id) AS rn
  FROM move_month_subsample_segmented
  WHERE substr(`date`,1,6) = '202208'
    AND residency_group = 'local'
    AND end_grid_id <> '-1'
  GROUP BY end_grid_id
) t
WHERE rn <= 2000
;

-- 202311 · local · Top 2000 busiest end_grid_id
DROP TABLE IF EXISTS top2000_local_endgrid_202311;
CREATE TABLE top2000_local_endgrid_202311 AS
SELECT end_grid_id, cnt
FROM (
  SELECT end_grid_id, COUNT(*) AS cnt,
         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, end_grid_id) AS rn
  FROM move_month_subsample_segmented
  WHERE substr(`date`,1,6) = '202311'
    AND residency_group = 'local'
    AND end_grid_id <> '-1'
  GROUP BY end_grid_id
) t
WHERE rn <= 2000
;


-- 只在 202208 出现（消失的热点）
SELECT
  a.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  a.cnt,
  CAST(a.cnt AS DOUBLE) / 1982091330.0 AS cnt_share_in_month
FROM top2000_local_endgrid_202208 a
LEFT JOIN top2000_local_endgrid_202311 b
  ON a.end_grid_id = b.end_grid_id
LEFT JOIN grid gc
  ON a.end_grid_id = gc.grid_id
WHERE b.end_grid_id IS NULL
ORDER BY a.cnt DESC, a.end_grid_id;

-- 只在 202311 出现（新增的热点）
SELECT
  b.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  b.cnt,
  CAST(b.cnt AS DOUBLE) / 973917412.0 AS cnt_share_in_month
FROM top2000_local_endgrid_202311 b
LEFT JOIN top2000_local_endgrid_202208 a
  ON b.end_grid_id = a.end_grid_id
LEFT JOIN grid gc
  ON b.end_grid_id = gc.grid_id
WHERE a.end_grid_id IS NULL
ORDER BY b.cnt DESC, b.end_grid_id;

-- Issue Hive Storage: cannot create new tables 

-- vanished
SELECT
  a.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  a.cnt,
  CAST(a.cnt AS DOUBLE) / 1982091330.0 AS cnt_share_in_month
FROM (
  -- 202208 · local · Top 2000 end_grid_id
  SELECT end_grid_id, cnt
  FROM (
    SELECT
      end_grid_id,
      COUNT(*) AS cnt,
      ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, end_grid_id) AS rn
    FROM move_month_subsample_segmented
    WHERE substr(`date`,1,6) = '202208'
      AND residency_group = 'local'
      AND end_grid_id <> '-1'
    GROUP BY end_grid_id
  ) t
  WHERE rn <= 2000
) a
LEFT JOIN (
  -- 202311 · local · Top 2000 end_grid_id
  SELECT end_grid_id
  FROM (
    SELECT
      end_grid_id,
      COUNT(*) AS cnt,
      ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, end_grid_id) AS rn
    FROM move_month_subsample_segmented
    WHERE substr(`date`,1,6) = '202311'
      AND residency_group = 'local'
      AND end_grid_id <> '-1'
    GROUP BY end_grid_id
  ) t
  WHERE rn <= 2000
) b
  ON a.end_grid_id = b.end_grid_id
LEFT JOIN grid gc
  ON a.end_grid_id = gc.grid_id
WHERE b.end_grid_id IS NULL
ORDER BY a.cnt DESC, a.end_grid_id;

-- new
SELECT
  b.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  b.cnt,
  CAST(b.cnt AS DOUBLE) / 973917412.0 AS cnt_share_in_month
FROM (
  -- 202311 Top2000
  SELECT end_grid_id, cnt
  FROM (
    SELECT
      end_grid_id,
      COUNT(*) AS cnt,
      ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, end_grid_id) AS rn
    FROM move_month_subsample_segmented
    WHERE substr(`date`,1,6) = '202311'
      AND residency_group = 'local'
      AND end_grid_id <> '-1'
    GROUP BY end_grid_id
  ) t
  WHERE rn <= 2000
) b
LEFT JOIN (
  -- 202208 Top2000
  SELECT end_grid_id
  FROM (
    SELECT
      end_grid_id,
      COUNT(*) AS cnt,
      ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, end_grid_id) AS rn
    FROM move_month_subsample_segmented
    WHERE substr(`date`,1,6) = '202208'
      AND residency_group = 'local'
      AND end_grid_id <> '-1'
    GROUP BY end_grid_id
  ) t
  WHERE rn <= 2000
) a
  ON b.end_grid_id = a.end_grid_id
LEFT JOIN grid gc
  ON b.end_grid_id = gc.grid_id
WHERE a.end_grid_id IS NULL
ORDER BY b.cnt DESC, b.end_grid_id;

-- Gender difference within the local group? 

-- 每个性别各取前2000个 busiest 目的地格子（合并 202208 与 202311）
SELECT
  t.gender,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt
FROM (
  SELECT
    gender,
    end_grid_id,
    COUNT(*) AS cnt,
    ROW_NUMBER() OVER (
      PARTITION BY gender
      ORDER BY COUNT(*) DESC, end_grid_id
    ) AS rn
  FROM move_month_subsample_segmented
  WHERE CAST(end_ptype AS STRING) NOT IN ('1','2')   -- 过滤 end_ptype=1/2
    AND end_grid_id <> '-1'                          -- 去掉无效格子
    AND gender IS NOT NULL
  GROUP BY gender, end_grid_id
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 1000
ORDER BY t.gender, t.cnt DESC, t.end_grid_id;

--

SELECT
  c.gender,
  c.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  c.cnt
FROM (
  -- 先按 gender × end_grid_id 计数
  SELECT a.gender, a.end_grid_id, a.cnt
  FROM (
    SELECT
      gender,
      end_grid_id,
      COUNT(*) AS cnt
    FROM move_month_subsample_segmented
    WHERE CAST(end_ptype AS STRING) NOT IN ('1','2')   -- 过滤 end_ptype = 1/2
      AND end_grid_id <> '-1'                          -- 去掉无效格子
      AND gender IS NOT NULL
    GROUP BY gender, end_grid_id
  ) a
  -- 自连接：统计同一 gender 下，比 a 更“靠前”的格子数量
  LEFT JOIN (
    SELECT
      gender,
      end_grid_id,
      COUNT(*) AS cnt
    FROM move_month_subsample_segmented
    WHERE CAST(end_ptype AS STRING) NOT IN ('1','2')
      AND end_grid_id <> '-1'
      AND gender IS NOT NULL
    GROUP BY gender, end_grid_id
  ) b
    ON a.gender = b.gender
   AND (b.cnt > a.cnt OR (b.cnt = a.cnt AND b.end_grid_id < a.end_grid_id))
  GROUP BY a.gender, a.end_grid_id, a.cnt
  HAVING COUNT(b.end_grid_id) < 1000   -- 仅保留每个性别前1000
) c
LEFT JOIN grid gc
  ON c.end_grid_id = gc.grid_id
ORDER BY c.gender, c.cnt DESC, c.end_grid_id;

-- 在每个 period（202208 / 202311）内，对每个性别各取前2000
SELECT
  t.period,
  t.gender,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt
FROM (
  SELECT
    period,
    gender,
    end_grid_id,
    COUNT(*) AS cnt,
    ROW_NUMBER() OVER (
      PARTITION BY period, gender
      ORDER BY COUNT(*) DESC, end_grid_id
    ) AS rn
  FROM move_month_subsample_segmented
  WHERE CAST(end_ptype AS STRING) NOT IN ('1','2')   -- 过滤 end_ptype=1/2
    AND end_grid_id <> '-1'
    AND period IN ('202208','202311')
    AND gender IS NOT NULL
  GROUP BY period, gender, end_grid_id
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 1000
ORDER BY t.period, t.gender, t.cnt DESC, t.end_grid_id;

--

SELECT
  t.period,
  t.gender,
  t.end_grid_id,
  gc.centroid_lat AS end_lat,
  gc.centroid_lon AS end_lon,
  t.cnt,
  -- 组内份额：cnt / (同 period×gender 的总 cnt)
  t.cnt / t.total_cnt AS share_in_group
FROM (
  -- 中层：在已聚合的 a 上做窗口，得到组内总量与排名
  SELECT
    a.period,
    a.gender,
    a.end_grid_id,
    a.cnt,
    SUM(a.cnt) OVER (PARTITION BY a.period, a.gender) AS total_cnt,
    ROW_NUMBER() OVER (
      PARTITION BY a.period, a.gender
      ORDER BY a.cnt DESC, a.end_grid_id
    ) AS rn
  FROM (
    -- 底层：先按 period×gender×end_grid_id 聚合出 cnt
    SELECT
      period,
      gender,
      end_grid_id,
      COUNT(1) AS cnt
    FROM move_month_subsample_segmented
    WHERE
      -- 选其一：若 end_ptype 是数值型，用这一行
      (end_ptype IS NULL OR end_ptype NOT IN (1, 2))
      -- 若 end_ptype 是字符串型，改用这一行：
      -- COALESCE(CAST(end_ptype AS STRING), '') NOT IN ('1','2')
      AND end_grid_id <> '-1'
      AND period IN ('202208','202311')
      AND gender IS NOT NULL
    GROUP BY period, gender, end_grid_id
  ) a
) t
LEFT JOIN grid gc
  ON t.end_grid_id = gc.grid_id
WHERE t.rn <= 2000
ORDER BY t.period, t.gender, t.cnt DESC, t.end_grid_id;