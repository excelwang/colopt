--/tmp/hdfs/hive.log.2017-01-09
--q29
INSERT INTO TABLE q29_hive_power_test_0_result
SELECT category_id_1, category_id_2, COUNT (*) AS cnt
FROM (
  -- Make category "purchased together" pairs
  -- combining collect_set + sorting + makepairs(array, noSelfParing)
  -- ensures we get no pairs with swapped places like: (12,24),(24,12).
  -- We only produce tuples (12,24) ensuring that the smaller number is always on the left side
  SELECT makePairs(sort_array(itemArray), false) AS (category_id_1,category_id_2)
  FROM (
    SELECT collect_set(i_category_id) as itemArray --(_list= with duplicates, _set = distinct)
    FROM web_sales ws, item i
    WHERE ws.ws_item_sk = i.i_item_sk
    AND i.i_category_id IS NOT NULL
    GROUP BY ws_order_number
  ) collectedList
) pairs
GROUP BY category_id_1, category_id_2
ORDER BY cnt DESC, category_id_1, category_id_2
LIMIT 100;
--q16
INSERT INTO TABLE q16_hive_power_test_0_result
SELECT w_state, i_item_id,
  SUM(
    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') < unix_timestamp('2001-03-16','yyyy-MM-dd'))
    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
    ELSE 0.0 END
  ) AS sales_before,
  SUM(
    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') >= unix_timestamp('2001-03-16','yyyy-MM-dd'))
    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
    ELSE 0.0 END
  ) AS sales_after
FROM (
  SELECT *
  FROM web_sales ws
  LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number
    AND ws.ws_item_sk = wr.wr_item_sk)
) a1
JOIN item i ON a1.ws_item_sk = i.i_item_sk
JOIN warehouse w ON a1.ws_warehouse_sk = w.w_warehouse_sk
JOIN date_dim d ON a1.ws_sold_date_sk = d.d_date_sk
AND unix_timestamp(d.d_date, 'yyyy-MM-dd') >= unix_timestamp('2001-03-16', 'yyyy-MM-dd') - 30*24*60*60 --subtract 30 days in seconds
AND unix_timestamp(d.d_date, 'yyyy-MM-dd') <= unix_timestamp('2001-03-16', 'yyyy-MM-dd') + 30*24*60*60 --add 30 days in seconds
GROUP BY w_state,i_item_id
ORDER BY w_state,i_item_id
LIMIT 100;
