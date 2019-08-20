INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (35, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'promotion', 'promotion.dat', null, 1, 14, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (36, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'time_dim', 'time_dim.dat', null, 1, 20, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (37, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'warehouse', 'warehouse.dat', null, 1, 21, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (60, 'postgresql', 'database', 'base', 'public', 'view', 'wscs1_direct', '(select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales) v)', null, 1, -1, 1, 5);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (55, 'parquet', 'file', 'derived', 'tpcds', 'file', 'q4', 'select   
                  t_s_secyear.customer_id
                 ,t_s_secyear.customer_first_name
                 ,t_s_secyear.customer_last_name
                 ,t_s_secyear.customer_birth_country
 from {{ year_total }} t_s_firstyear
     ,{{ year_total }} t_s_secyear
     ,{{ year_total }} t_c_firstyear
     ,{{ year_total }} t_c_secyear
     ,{{ year_total }} t_w_firstyear
     ,{{ year_total }} t_w_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
   and t_s_firstyear.customer_id = t_c_secyear.customer_id
   and t_s_firstyear.customer_id = t_c_firstyear.customer_id
   and t_s_firstyear.customer_id = t_w_firstyear.customer_id
   and t_s_firstyear.customer_id = t_w_secyear.customer_id
   and t_s_firstyear.sale_type = ''s''
   and t_c_firstyear.sale_type = ''c''
   and t_w_firstyear.sale_type = ''w''
   and t_s_secyear.sale_type = ''s''
   and t_c_secyear.sale_type = ''c''
   and t_w_secyear.sale_type = ''w''
   and t_s_firstyear.dyear =  1999
   and t_s_secyear.dyear = 1999+1
   and t_c_firstyear.dyear =  1999
   and t_c_secyear.dyear =  1999+1
   and t_w_firstyear.dyear = 1999
   and t_w_secyear.dyear = 1999+1
   and t_s_firstyear.year_total > 0
   and t_c_firstyear.year_total > 0
   and t_w_firstyear.year_total > 0
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
 order by t_s_secyear.customer_id
         ,t_s_secyear.customer_first_name
         ,t_s_secyear.customer_last_name
         ,t_s_secyear.customer_birth_country', null, 1, -1, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (57, 'postgresql', 'database', 'derived', 'public', 'view', 'q21', 'select d_week_seq1
       ,round(sun_sales1/sun_sales2,2) sun
       ,round(mon_sales1/mon_sales2,2) mon
       ,round(tue_sales1/tue_sales2,2) tue
       ,round(wed_sales1/wed_sales2,2) wed
       ,round(thu_sales1/thu_sales2,2) thu
       ,round(fri_sales1/fri_sales2,2) fri
       ,round(sat_sales1/sat_sales2,2) sat
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs1 as wswscs,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs1 as wswscs
      ,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1', null, 1, -1, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (53, 'parquet', 'file', 'derived', 'tpcds', 'file', 'wscs', 'select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from {{ web_sales }} web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from {{ catalog_sales }} catalog_sales) v', null, 1, -1, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (56, 'postgresql', 'database', 'derived', 'public', 'view', 'wscs1', 'select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales) v', null, 1, -1, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (58, 'postgresql', 'database', 'derived', 'public', 'view', 'wswscs1', 'select d_week_seq,
        sum(case when (d_day_name=''Sunday'') then sales_price else null end) sun_sales,
        sum(case when (d_day_name=''Monday'') then sales_price else null end) mon_sales,
        sum(case when (d_day_name=''Tuesday'') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name=''Wednesday'') then sales_price else null end) wed_sales,
        sum(case when (d_day_name=''Thursday'') then sales_price else null end) thu_sales,
        sum(case when (d_day_name=''Friday'') then sales_price else null end) fri_sales,
        sum(case when (d_day_name=''Saturday'') then sales_price else null end) sat_sales
 from wscs1 as wscs
     ,date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq', null, 1, -1, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (1, 'postgresql', 'database', 'base', 'public', 'table', 'catalog_page', 'public.catalog_page', null, 1, 2, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (26, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'catalog_page', 'catalog_page.dat', null, 1, 2, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (27, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'customer', 'customer.dat', null, 1, 5, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (28, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'customer_address', 'customer_address.dat', null, 1, 6, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (59, 'postgresql', 'database', 'base', 'public', 'table', 'web_sales_m', 'public.web_sales', 'ws_sold_date_sk', 4, -1, -1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (29, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'customer_demographics', 'customer_demographics.dat', null, 1, 7, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (2, 'postgresql', 'database', 'base', 'public', 'table', 'customer', 'public.customer', null, 1, 5, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (3, 'postgresql', 'database', 'base', 'public', 'table', 'customer_address', 'public.customer_address', null, 1, 6, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (4, 'postgresql', 'database', 'base', 'public', 'table', 'customer_demographics', 'public.customer_demographics', null, 1, 7, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (5, 'postgresql', 'database', 'base', 'public', 'table', 'date_dim', 'public.date_dim', null, 1, 8, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (6, 'postgresql', 'database', 'base', 'public', 'table', 'dbgen_version', 'public.dbgen_version', null, 1, 9, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (20, 'postgresql', 'database', 'base', 'public', 'table', 'call_center', 'public.call_center', null, 1, 1, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (21, 'postgresql', 'database', 'base', 'public', 'table', 'reason', 'public.reason', null, 1, 15, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (23, 'postgresql', 'database', 'base', 'public', 'table', 'ship_mode', 'public.ship_mode', null, 1, 16, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (22, 'postgresql', 'database', 'base', 'public', 'table', 'web_sales', 'public.web_sales', 'ws_item_sk', 4, 24, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (24, 'postgresql', 'database', 'base', 'public', 'table', 'store', 'public.store', null, 1, 17, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (25, 'postgresql', 'database', 'base', 'public', 'table', 'store_returns', 'public.store_returns', null, 1, 18, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (30, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'date_dim', 'date_dim.dat', null, 1, 8, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (31, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'dbgen_version', 'dbgen_version.dat', null, 1, 9, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (32, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'household_demographics', 'household_demographics.dat', null, 1, 10, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (33, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'income_band', 'income_band.dat', null, 1, 11, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (34, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'item', 'item.dat', null, 1, 13, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (38, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'web_page', 'web_page.dat', null, 1, 22, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (39, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'web_site', 'web_site.dat', null, 1, 25, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (40, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'catalog_returns', 'catalog_returns.dat', null, 1, 3, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (41, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'catalog_sales', 'catalog_sales.dat', null, 1, 4, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (42, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'inventory', 'inventory.dat', null, 1, 12, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (43, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'store_sales', 'store_sales.dat', null, 1, 19, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (44, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'web_returns', 'web_returns.dat', null, 1, 23, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (45, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'call_center', 'call_center.dat', null, 1, 1, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (46, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'reason', 'reason.dat', null, 1, 15, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (50, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'store_returns', 'store_returns.dat', null, 1, 18, 3, 5);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (7, 'postgresql', 'database', 'base', 'public', 'table', 'household_demographics', 'public.household_demographics', null, 1, 10, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (8, 'postgresql', 'database', 'base', 'public', 'table', 'income_band', 'public.income_band', null, 1, 11, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (9, 'postgresql', 'database', 'base', 'public', 'table', 'item', 'public.item', null, 1, 13, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (10, 'postgresql', 'database', 'base', 'public', 'table', 'promotion', 'public.promotion', null, 1, 14, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (11, 'postgresql', 'database', 'base', 'public', 'table', 'time_dim', 'public.time_dim', null, 1, 20, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (12, 'postgresql', 'database', 'base', 'public', 'table', 'warehouse', 'public.warehouse', null, 1, 21, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (13, 'postgresql', 'database', 'base', 'public', 'table', 'web_page', 'public.web_page', null, 1, 22, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (14, 'postgresql', 'database', 'base', 'public', 'table', 'web_site', 'public.web_site', null, 1, 25, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (15, 'postgresql', 'database', 'base', 'public', 'table', 'catalog_returns', 'public.catalog_returns', null, 1, 3, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (16, 'postgresql', 'database', 'base', 'public', 'table', 'catalog_sales', 'public.catalog_sales', null, 1, 4, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (17, 'postgresql', 'database', 'base', 'public', 'table', 'inventory', 'public.inventory', null, 1, 12, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (18, 'postgresql', 'database', 'base', 'public', 'table', 'store_sales', 'public.store_sales', null, 1, 19, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (19, 'postgresql', 'database', 'base', 'public', 'table', 'web_returns', 'public.web_returns', null, 1, 23, 1, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (47, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'web_sales', 'web_sales.dat', null, 1, 24, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (48, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'ship_mode', 'ship_mode.dat', null, 1, 16, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (49, 'pipeDelimited', 'file', 'base', 'tpcds', 'file', 'store', 'store.dat', null, 1, 17, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (54, 'parquet', 'file', 'derived', 'tpcds', 'file', 'year_total', 'select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
       ,''s'' sale_type
 from {{ customer }} customer
     ,{{ store_sales }} store_sales
     ,{{ date_dim }} date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
       ,''c'' sale_type
 from {{ customer }} customer
     ,{{ catalog_sales }} catalog_sales
     ,{{ date_dim }} date_dim
 where c_customer_sk = cs_bill_customer_sk
   and cs_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
       ,''w'' sale_type
 from {{ customer }} customer
     ,{{ web_sales }} web_sales
     ,{{ date_dim }} date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year', null, 1, -1, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (52, 'parquet', 'file', 'derived', 'tpcds', 'file', 'wswscs', 'select d_week_seq,
        sum(case when (d_day_name=''Sunday'') then sales_price else null end) sun_sales,
        sum(case when (d_day_name=''Monday'') then sales_price else null end) mon_sales,
        sum(case when (d_day_name=''Tuesday'') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name=''Wednesday'') then sales_price else null end) wed_sales,
        sum(case when (d_day_name=''Thursday'') then sales_price else null end) thu_sales,
        sum(case when (d_day_name=''Friday'') then sales_price else null end) fri_sales,
        sum(case when (d_day_name=''Saturday'') then sales_price else null end) sat_sales
 from {{ wscs }} wscs
     ,{{ date_dim }} date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq', null, 1, -1, 3, 2);
INSERT INTO dataset (id, format, storage_type, mode, container, object_type, object_name, query, partition_key, number_partitions, schema_id, data_persistence_src_id, data_persistence_dest_id) VALUES (51, 'parquet', 'file', 'derived', 'tpcds', 'file', 'q2', 'select d_week_seq1
       ,round(sun_sales1/sun_sales2,2) sun
       ,round(mon_sales1/mon_sales2,2) mon
       ,round(tue_sales1/tue_sales2,2) tue
       ,round(wed_sales1/wed_sales2,2) wed
       ,round(thu_sales1/thu_sales2,2) thu
       ,round(fri_sales1/fri_sales2,2) fri
       ,round(sat_sales1/sat_sales2,2) sat
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from {{ wswscs }} wswscs,{{ date_dim }} date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from {{ wswscs }} wswscs
      ,{{ date_dim }} date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1', null, 1, -1, 3, 2);
