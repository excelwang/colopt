use bigbench;
select histogram_numeric(ws_item_sk,85) from web_sales;
select histogram_numeric(i_item_sk,85) from item;
select histogram_numeric(ss_item_sk,85) from store_sales;
select histogram_numeric(ws_warehouse_sk,85) from web_sales;
select histogram_numeric(w_warehouse_sk,85) from warehouse;
select histogram_numeric(wcs_item_sk,85) from web_clickstreams;
select histogram_numeric(wcs_user_sk,85) from web_clickstreams;
select histogram_numeric(ss_customer_sk,85) from store_sales;
select histogram_numeric(pr_item_sk,85) from product_reviews;
select histogram_numeric(ws_item_sk,85) from web_sales;
