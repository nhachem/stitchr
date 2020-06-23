INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 55, 2, 'parquet', false, 'append', 'select   
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
         ,t_s_secyear.customer_birth_country');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 53, 2, 'parquet', false, 'append', 'select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from {{ web_sales }} web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from {{ catalog_sales }} catalog_sales) v');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 35, 4, 'parquet', false, 'append', 'promotion.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 36, 4, 'parquet', false, 'append', 'time_dim.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 37, 4, 'parquet', false, 'append', 'warehouse.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 59, 2, 'parquet', false, 'append', 'public.web_sales');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 26, 4, 'parquet', false, 'append', 'catalog_page.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 27, 4, 'parquet', false, 'append', 'customer.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 28, 4, 'parquet', false, 'append', 'customer_address.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 29, 4, 'parquet', false, 'append', 'customer_demographics.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 60, 0, 'parquet', false, 'overwrite', '(select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales) v)');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 56, 0, 'parquet', false, 'overwrite', 'select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales) v');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 30, 4, 'parquet', false, 'append', 'date_dim.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 31, 4, 'parquet', false, 'append', 'dbgen_version.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 32, 4, 'parquet', false, 'append', 'household_demographics.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 33, 4, 'parquet', false, 'append', 'income_band.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 34, 4, 'parquet', false, 'append', 'item.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 38, 4, 'parquet', false, 'append', 'web_page.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 39, 4, 'parquet', false, 'append', 'web_site.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 40, 4, 'parquet', false, 'append', 'catalog_returns.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 41, 4, 'parquet', false, 'append', 'catalog_sales.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 42, 4, 'parquet', false, 'append', 'inventory.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 43, 4, 'parquet', false, 'append', 'store_sales.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 44, 4, 'parquet', false, 'append', 'web_returns.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 45, 4, 'parquet', false, 'append', 'call_center.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 46, 4, 'parquet', false, 'append', 'reason.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 54, 2, 'parquet', false, 'append', 'select c_customer_id customer_id
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
         ,d_year');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 52, 2, 'parquet', false, 'append', 'select d_week_seq,
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
 group by d_week_seq');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 51, 2, 'parquet', false, 'append', 'select d_week_seq1
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
 order by d_week_seq1');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 1, 0, 'parquet', false, 'overwrite', 'public.catalog_page');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 2, 0, 'parquet', false, 'overwrite', 'public.customer');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 3, 0, 'parquet', false, 'overwrite', 'public.customer_address');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 4, 0, 'parquet', false, 'overwrite', 'public.customer_demographics');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 5, 0, 'parquet', false, 'overwrite', 'public.date_dim');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 6, 0, 'parquet', false, 'overwrite', 'public.dbgen_version');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 50, 4, 'parquet', false, 'append', 'store_returns.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 47, 4, 'parquet', false, 'append', 'web_sales.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 48, 4, 'parquet', false, 'append', 'ship_mode.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 49, 4, 'parquet', false, 'append', 'store.dat');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 21, 0, 'parquet', false, 'overwrite', 'public.reason');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 23, 0, 'parquet', false, 'overwrite', 'public.ship_mode');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 24, 0, 'parquet', false, 'overwrite', 'public.store');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 25, 0, 'parquet', false, 'overwrite', 'public.store_returns');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 20, 0, 'parquet', false, 'overwrite', 'public.call_center');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 7, 0, 'parquet', false, 'overwrite', 'public.household_demographics');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 57, 0, 'parquet', false, 'overwrite', 'select d_week_seq1
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
 order by d_week_seq1');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 58, 0, 'parquet', false, 'overwrite', 'select d_week_seq,
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
 group by d_week_seq');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 8, 0, 'parquet', false, 'overwrite', 'public.income_band');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 9, 0, 'parquet', false, 'overwrite', 'public.item');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 10, 0, 'parquet', false, 'overwrite', 'public.promotion');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 11, 0, 'parquet', false, 'overwrite', 'public.time_dim');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 12, 0, 'parquet', false, 'overwrite', 'public.warehouse');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 13, 0, 'parquet', false, 'overwrite', 'public.web_page');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 14, 0, 'parquet', false, 'overwrite', 'public.web_site');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 15, 0, 'parquet', false, 'overwrite', 'public.catalog_returns');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 16, 0, 'parquet', false, 'overwrite', 'public.catalog_sales');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 17, 0, 'parquet', false, 'overwrite', 'public.inventory');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 18, 0, 'parquet', false, 'overwrite', 'public.store_sales');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 19, 0, 'parquet', false, 'overwrite', 'public.web_returns');
INSERT INTO dataset_transform (transform_id, source_dataset_id, target_persistence_id, target_format, add_run_time_ref, write_mode, query) VALUES (1, 22, 0, 'parquet', false, 'overwrite', 'public.web_sales');
