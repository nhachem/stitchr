#!/usr/bin/env bash

## nohup ./bash/moveDataSetList.sh web_sales_1,store_sales_1,web_sales_3  > /tmp/logs/testRun.log &


## this is a full mixed +

# time ./bash/moveDataSetList.sh catalog_page_1,customer_1,customer_address_1,customer_demographics_1,date_dim_1,dbgen_version_1,household_demographics_1,income_band_1,item_3,promotion_1,time_dim_1,warehouse_1,web_page_1,web_site_1,catalog_returns_1,catalog_sales_1,inventory_1,store_sales_3,web_returns_1,call_center_1,reason_1,web_sales_1,ship_mode_1,store_1,store_returns_3

## time ./bash/moveDataSetList.sh catalog_page_1,customer_1,customer_address_1,customer_demographics_1,date_dim_1,dbgen_version_1,household_demographics_1,income_band_1,item_1,promotion_1,time_dim_1,warehouse_1,web_page_1,web_site_1,catalog_returns_1,catalog_sales_1,inventory_1,store_sales_1,web_returns_1,call_center_1,reason_1,web_sales_1,ship_mode_1,store_1,store_returns_1

time ./bash/moveDataSetList.sh catalog_page_3,customer_3,customer_address_3,customer_demographics_3,date_dim_3,dbgen_version_3,household_demographics_3,income_band_3,item_3,promotion_3,time_dim_3,warehouse_3,web_page_3,web_site_3,catalog_returns_3,catalog_sales_3,inventory_3,store_sales_3,web_returns_3,call_center_3,reason_3,web_sales_3,ship_mode_3,store_3,store_returns_3

time ./bash/moveDataSetGroup.sh tpcds_file
## files and postgres db
time ./bash/moveDataSetGroup.sh mixed


 ## jdbc direct
time ./bash/moveDataSetList.sh wscs1_direct_1
 


