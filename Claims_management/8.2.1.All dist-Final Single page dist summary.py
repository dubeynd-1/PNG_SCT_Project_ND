# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM claims_mgmt.claims_mgmt_summary_fortnight where rptmonthyear = '202602'

# COMMAND ----------

import io
import math
import requests
import calendar
from dateutil.rrule import rrule, MONTHLY
import shutil
import os

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, DateType, IntegerType, BooleanType
from pyspark.sql.functions import lit, current_date, col
from pyspark.sql import functions as F
from pyspark.sql.functions import  *
from pyspark.sql.window import Window

# COMMAND ----------

# today_str = datetime.today().strftime("%d%m%Y")
# print(today_str)
current_date = (datetime.now()-timedelta(days=0)).strftime('%Y-%m-%d')
# print(current_date)
previous_date = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
# print(previous_date)

dbutils.widgets.text("start_date", previous_date, "param_start_date")
dbutils.widgets.text("end_date", current_date, "param_end_date")
start_date_value = dbutils.widgets.get("start_date")
end_date_value = dbutils.widgets.get("end_date")

date_obj = datetime.strptime(start_date_value, "%Y-%m-%d")
month_start = date_obj.replace(day=1).strftime("%Y-%m-%d")
formatted_date = date_obj.strftime("%Y%m")

# Fortnight boundaries
f1_end = date_obj.strftime("%Y-%m-14")
f2_start = date_obj.strftime("%Y-%m-15")

print(formatted_date)
print(month_start)
print(start_date_value,end_date_value)
print(f1_end,f2_start)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Branch + SUB D IHR Claims
# MAGIC

# COMMAND ----------


df_1 = spark.sql(f"""select
1 as rnk, 
distributor,
'Overall' as section,
'Branch + SUB D IHR Claims' as details,
sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' then amountdisbursed else 0 end) as f1,
sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}' then amountdisbursed else 0 end) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
group by distributor""")
display(df_1.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Positive Enteries : Excluded During Fortnight Payments

# COMMAND ----------

# MAGIC %md
# MAGIC #### Distributor configured Initiatives not recommended by P&G

# COMMAND ----------


df_2 = spark.sql(f"""select 
                 2 as rnk,
                 distributor,
'Positive-Excluded during claims process' as section,
'Distributor configured Initiatives not recommended by P&G' as details, 
coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight='Not Consider - Site Plan' then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly='Not Consider - Site Plan' then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed>0
group by distributor""")
display(df_2.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### MR/Ecom Damage, SLOG Claims,CPC
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select distinct matrix_check_fortnight
# MAGIC from claims_mgmt.claims_mgmt_raw_report 
# MAGIC where RptMonthYear = '202601'
# MAGIC and amountdisbursed>0
# MAGIC and distributor='2001068583'

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select sum(case when (matrix_check_fortnight = 'Not Consider') then amountdisbursed end) as f1
# MAGIC from claims_mgmt.claims_mgmt_raw_report 
# MAGIC where RptMonthYear = '202605'
# MAGIC and amountdisbursed>0
# MAGIC and distributor='2001068583'

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC -- Not Consider - Medplus
# MAGIC -- Not Consider - Meesho
# MAGIC
# MAGIC select sum(case when (ShipDate>= '2026-05-01' and ShipDate<= '2026-05-14' 
# MAGIC and Remarks_Fortnight in ('Not Consider-PPD','Not Consider-Brainbees','Not Consider-Air Plaza','Not Consider-CPC','Not Consider-Ullage')) then amountdisbursed end) as f1
# MAGIC from claims_mgmt.claims_mgmt_raw_report 
# MAGIC where RptMonthYear = '202605'
# MAGIC and amountdisbursed>0
# MAGIC and distributor='2001068583'

# COMMAND ----------


df_3 = spark.sql(f"""select 
                 3 as rnk,
                 distributor,
'Positive-Excluded during claims process' as section,
'MR/Ecom Damage, SLOG Claims,CPC' as details,
coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight in ('Not Consider-PPD','Not Consider-Brainbees','Not Consider-Air Plaza','Not Consider-CPC','Not Consider-Ullage','Not Consider - Meesho','Not Consider - Medplus') then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly in ('Not Consider-PPD','Not Consider-Brainbees','Not Consider-Air Plaza','Not Consider-CPC','Not Consider-Ullage','Not Consider - Meesho','Not Consider - Medplus') then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed>0
group by distributor""")
display(df_3.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Corporate Initiatives/Productivity/Rishta/Shakti
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Remarks_Fortnight
# MAGIC from claims_mgmt.claims_mgmt_raw_report
# MAGIC where RptMonthYear='202601'

# COMMAND ----------


df_4 = spark.sql(f"""select 
                 4 as rnk,
                 distributor,
'Positive-Excluded during claims process' as section,
'Corporate Initiatives/Productivity/Rishta/Shakti' as details,
coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight in ('Not Consider - Productivity','Not Consider - GB') then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly in ('Not Consider - Productivity','Not Consider - GB') then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed>0
group by distributor""")
display(df_4.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### MRI Claims
# MAGIC

# COMMAND ----------


df_5 = spark.sql(f"""select 
                 5 as rnk,
                 distributor,
'Positive-Excluded during claims process' as section,
'MRI Claims' as details,
coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight='Not Consider-MR-I-Adaptive' then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly='Not Consider-MR-I-Adaptive' then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed>0
group by distributor""")
display(df_5.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Laundry claims

# COMMAND ----------


df_6 = spark.sql(f"""select 
                 6 as rnk,
                 distributor,
'Positive-Excluded during claims process' as section,
'Laundry claims' as details,
coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight='Not Consider - Laundry Plan' then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly='Not Consider - Laundry Plan' then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed>0
group by distributor""")
display(df_6.limit(2))

# COMMAND ----------


df_7 = spark.sql(f"""select 
                 7 as rnk,
                 distributor,
'Positive-Excluded during claims process' as section,
'Multi Brand claims' as details,
coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight='Not Consider - Multi Brand Scheme' then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly='Multi Brand Scheme' then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed>0
group by distributor""")
display(df_7.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Negative claims

# COMMAND ----------

# MAGIC %md
# MAGIC #### Distributor configured Initiatives not recommended by P&G

# COMMAND ----------


df_8 = spark.sql(f"""select 
                 8 as rnk,
                 distributor,
'Negative-Excluded during claims process' as section,
'Distributor configured Initiatives not recommended by P&G' as details,
 coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight='Not Consider - Site Plan' then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly='Not Consider - Site Plan' then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed<0
group by distributor""")
display(df_8.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### MR/Ecom Damage, SLOG Claims,CPC
# MAGIC

# COMMAND ----------

df_9 = spark.sql(f"""select 
                 9 as rnk,
                 distributor,
'Negative-Excluded during claims process' as section,
'MR/Ecom Damage, SLOG Claims,CPC' as details, coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight in ('Not Consider-PPD','Not Consider-Brainbees','Not Consider-Air Plaza','Not Consider-CPC','Not Consider-Ullage','Not Consider - Meesho','Not Consider - Medplus') then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly in ('Not Consider-PPD','Not Consider-Brainbees','Not Consider-Air Plaza','Not Consider-CPC','Not Consider-Ullage','Not Consider - Meesho','Not Consider - Medplus') then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed<0 
group by distributor""")
display(df_9.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Corporate Initiatives/Productivity/Rishta/Shakti
# MAGIC

# COMMAND ----------

## add not consider-GB also, in -ve too
df_10 = spark.sql(f"""select
                 10 as rnk,
                 distributor, 
'Negative-Excluded during claims process' as section,
'Corporate Initiatives/Productivity/Rishta/Shakti' as details, coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight in ('Not Consider - Productivity','Not Consider - GB') then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly in ('Not Consider - Productivity','Not Consider - GB') then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed<0 
group by distributor""")
display(df_10.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### MRI Claims
# MAGIC

# COMMAND ----------

df_11 = spark.sql(f"""select 
                  11 as rnk,
                  distributor,
'Negative-Excluded during claims process' as section,
'MRI Claims' as details, coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight='Not Consider-MR-I-Adaptive' then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly='Not Consider-MR-I-Adaptive' then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed<0
group by distributor""")
display(df_11.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Laundry claims

# COMMAND ----------


df_12 = spark.sql(f"""select 
                  12 as rnk,
                  distributor,
'Negative-Excluded during claims process' as section,
'Laundry claims' as details, coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight='Not Consider - Laundry Plan' then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly='Not Consider - Laundry Plan' then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed<0
group by distributor""")
display(df_12.limit(2))

# COMMAND ----------


df_13 = spark.sql(f"""select 
                  13 as rnk,
                  distributor,
'Negative-Excluded during claims process' as section,
'Multi Brand claims' as details, coalesce(sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' 
and Remarks_Fortnight='Not Consider - Multi Brand Scheme' then amountdisbursed end),0) as f1,
coalesce(sum(case when ShipDate>= '{f2_start}' and ShipDate<= '{end_date_value}'
and Remarks_Monthly='Multi Brand Scheme' then amountdisbursed end),0) as f2
from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = {formatted_date}
and amountdisbursed<0
group by distributor""")
display(df_13.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Verification : Excluded During Fortnight Payment
# MAGIC - Using summary tables (claims_mgmt_summary_fortnight and claims_mgmt_summary_monthly) directly

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wrong Channel
# MAGIC

# COMMAND ----------


df_14 = spark.sql(f"""
with f1 as (select distributor,coalesce(sum(wrong_channel)*-1,0) as f1
from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear={formatted_date} group by distributor),
f2 as (
  select distributor,coalesce(sum(wrong_channel)*-1,0) as f2
from claims_mgmt.claims_mgmt_summary_monthly where RptMonthYear={formatted_date} group by distributor
)
select 14 as rnk,'Performance Verification' as section,
'Wrong channel' as details, f1.*,coalesce(f2.f2,0) as f2 from f1 left join f2  on f1.distributor=f2.distributor """)
display(df_14.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wrong Rate
# MAGIC

# COMMAND ----------


df_15 = spark.sql(f"""with f1 as (select distributor,coalesce(sum(wrong_rate)*-1,0) as f1
from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear={formatted_date} group by distributor),
f2 as (
  select distributor,coalesce(sum(wrong_rate)*-1,0) as f2
from claims_mgmt.claims_mgmt_summary_monthly where RptMonthYear={formatted_date} group by distributor
)
select 15 as rnk,'Performance Verification' as section,
'Wrong rate' as details,f1.*,coalesce(f2.f2,0) as f2 from f1 left join f2  on f1.distributor=f2.distributor """)
display(df_15.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partial SRN
# MAGIC

# COMMAND ----------


df_16 = spark.sql(f"""select 16 as rnk,distributor,'Performance Verification' as section,
'Partial SRN' as details, 0 as f1, coalesce(sum(Disallowance_SRN)*-1,0) as f2
from claims_mgmt.claims_mgmt_summary_monthly where RptMonthYear={formatted_date} group by distributor""")
display(df_16.limit(2))


# COMMAND ----------

# MAGIC %md
# MAGIC #### LFG Initiatives - Free Goods

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct RptMonthYear from claims_mgmt.claims_mgmt_summary_fortnight

# COMMAND ----------


df_17 = spark.sql(f""" with f1 as (select distributor,coalesce(sum(Free_Goods)*-1,0) as f1
from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear={formatted_date} group by distributor),
f2 as (
  select distributor,coalesce(sum(Free_Goods)*-1,0) as f2
from claims_mgmt.claims_mgmt_summary_monthly where RptMonthYear={formatted_date} group by distributor
)
select 17 as rnk,'Performance Verification' as section,
'LFG initiatives - Free goods' as details, f1.*,coalesce(f2.f2,0) as f2 from f1 left join f2  on f1.distributor=f2.distributor """)
display(df_17.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wrong Date

# COMMAND ----------


df_18 = spark.sql(f"""with f1 as (select distributor,coalesce(sum(wrong_date)*-1,0) as f1
from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear={formatted_date} group by distributor),
f2 as (
  select distributor,coalesce(sum(wrong_date)*-1,0) as f2
from claims_mgmt.claims_mgmt_summary_monthly where RptMonthYear={formatted_date} group by distributor
)
select 18 as rnk,'Performance Verification' as section,
'Wrong Date' as details,f1.*,coalesce(f2.f2,0) as f2 from f1 left join f2 
on f1.distributor=f2.distributor""")
display(df_18.limit(2))

# COMMAND ----------


# Removing settlement disallowance as it overlaps with wrong date calculations
# df_18 = spark.sql(f"""with f1 as (select distributor,coalesce(sum(settelement_check_Amount)*-1,0) as f1
# from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear={formatted_date} group by distributor),
# f2 as (
#   select distributor,coalesce(sum(settelement_check_Amount)*-1,0) as f2
# from claims_mgmt.claims_mgmt_summary_monthly where RptMonthYear={formatted_date} group by distributor
# )
# select 18 as rnk,'Performance Verification' as section,
# 'Settlement disallowance' as details,f1.*,coalesce(f2.f2,0) as f2 from f1 left join f2 
# on f1.distributor=f2.distributor""")
# display(df_18.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Untagged SRN Recovery

# COMMAND ----------

df_19 = spark.sql(f"""select 19 as rnk,DistCode AS distributor,'Performance Verification' as section,
'Untagged SRN Recovery' as details, 
coalesce(sum(case when `Date`>= '{start_date_value}' and `Date`<= '{f1_end}' then Discount end),0) as f1,
coalesce(sum(case when `Date`>= '{f2_start}' and `Date`<= '{end_date_value}' then Discount end),0) as f2
FROM cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails s
-- LEFT JOIN stg.dist_branch_site_mapping cm
--         ON s.DistCode = cm.DistributorCode
--         AND s.BranchCode = cm.BranchCode
    WHERE s.DocNumber = s.ApplyToDocNum
        AND s.TransactionType = 'Saleable RETURN'
    group by s.DistCode""")
display(df_19)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combining all the dfs

# COMMAND ----------

from functools import reduce

df_list = [
    df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10, df_11, df_12, df_13, df_14, df_15, df_16, df_17, df_18, df_19
]

final_df = reduce(lambda a,b: a.unionByName(b), df_list)
final_df = (
    final_df
    .withColumnRenamed('rnk', 'sr_no')
    .withColumnRenamed('f1', 'first_fortnight')
    .withColumnRenamed('f2', 'second_fortnight')
    .withColumn('total',col('first_fortnight')+col('second_fortnight'))
)
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating the final disbursement as per matrix
# MAGIC - branch sub sum - (sum of positive and negative sections) + partial SRN + Untagged SRN Recovery

# COMMAND ----------

final_row_df = (
    final_df
    .groupBy('distributor')
    .agg(
        sum(when(col('details')=='Branch + SUB D IHR Claims', col('first_fortnight'))).alias('branch_f1'),
        sum(when(col('details')=='Branch + SUB D IHR Claims', col('second_fortnight'))).alias('branch_f2'),
        sum(when(col('section').isin('Positive-Excluded during claims process','Negative-Excluded during claims process'), col('first_fortnight'))).alias('claims_f1'),
        # sum(when(col('section').isin('Positive-Excluded during claims process','Negative-Excluded during claims process'), col('second_fortnight'))).alias('claims_f2'),
        # Removing monthly laundry and multibrand schemes from total disbursement because it will already be included in the ihr value as it goes in 'Consider' for monthly
         sum(when((col('section').isin('Positive-Excluded during claims process','Negative-Excluded during claims process')) & (~col('details').isin('Laundry claims','Multi Brand claims')), col('second_fortnight'))).alias('claims_f2'),

        sum(when((col('section')=='Performance Verification') & (col('details').isin('Partial SRN','Untagged SRN Recovery')), col('first_fortnight'))).alias('srn_f1'),
        sum(when((col('section')=='Performance Verification') & (col('details').isin('Partial SRN','Untagged SRN Recovery')), col('second_fortnight'))).alias('srn_f2'),
    )
    .withColumn('srn_f1',coalesce(col('srn_f1'),lit(0)))
    .withColumn('srn_f2',coalesce(col('srn_f2'),lit(0)))
)
display(final_row_df.limit(2))

# COMMAND ----------

final_row_df = (
    final_row_df
    .select(
        lit(20).alias('sr_no'),
        col('distributor'),
        lit('Calculations').alias('section'),
        lit('Final Disbursement as per Matrix').alias('details'),
        (col('branch_f1')-col('claims_f1')+col('srn_f1')).alias("first_fortnight"),
        (col('branch_f2')-col('claims_f2')+col('srn_f2')).alias("second_fortnight"),
        (col('first_fortnight')+col('second_fortnight')).alias("total")

    )
)
final_row_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding TSOI adjustment and Recovery/Payment placeholders

# COMMAND ----------

tsoi_row_df = (
    final_df
    .select("distributor").distinct()
    .withColumn("sr_no",lit(21))
    .withColumn("section",lit("Calculations"))
    .withColumn("details",lit("TSOI Adjustment"))
    .withColumn("first_fortnight",lit(0))
    .withColumn("second_fortnight",lit(0))
    .withColumn("total",lit(0))
)
# display(tsoi_row_df)

# COMMAND ----------

final_df_with_matrix = (
    final_df
    .unionByName(final_row_df)
    .unionByName(tsoi_row_df)
    .withColumn('rptmonthyear',lit(formatted_date))
)
final_df_with_matrix.createOrReplaceTempView('final_df_with_matrix')
display(final_df_with_matrix)

# COMMAND ----------

recovery_row_df = (
    final_df_with_matrix
    .groupBy('distributor','rptmonthyear')
    .agg(
        sum(when(col('details').isin('Final Disbursement as per Matrix','TSOI Adjustment'), col('first_fortnight'))).alias('first_fortnight'),
        sum(when(col('details').isin('Final Disbursement as per Matrix','TSOI Adjustment'), col('second_fortnight'))).alias('second_fortnight'),
        sum(when(col('details').isin('Final Disbursement as per Matrix','TSOI Adjustment'), col('total'))).alias('total')
    )
    .withColumn("sr_no",lit(22))
    .withColumn("section",lit("Calculations"))
    .withColumn("details",lit("Recovery/Payment"))
)

# COMMAND ----------

final_df_with_matrix_1 = (
    final_df_with_matrix
    .unionByName(recovery_row_df)
)
final_df_with_matrix_1.createOrReplaceTempView('final_df_with_matrix_1')
display(final_df_with_matrix_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving the final table

# COMMAND ----------

spark.sql(f''' select distinct DistributorCode, DistributorName from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy ''').createOrReplaceTempView("dist_names")

# COMMAND ----------

final_df_with_matrix_2 = (spark.sql(f"""
  select c.*,d.DistributorName from 
  final_df_with_matrix_1 c
  left join
        dist_names d
        on c.Distributor=d.DistributorCode
  order by distributor, sr_no
""")
)

# COMMAND ----------

spark.sql(f'''
          delete from claims_mgmt.single_page_dist_summary 
          where rptmonthyear like "{formatted_date}" 
          ''').show()

# COMMAND ----------

final_df_with_matrix_2.write.mode("append").option('mergeSchema','true').saveAsTable("claims_mgmt.single_page_dist_summary")

# COMMAND ----------

# %sql
# select * from claims_mgmt.single_page_dist_summary where rptmonthyear = '202604'

# COMMAND ----------

# %sql
# select * from claims_mgmt.single_page_dist_summary where rptmonthyear = '202601'

# COMMAND ----------

# %sql
# select * from claims_mgmt.single_page_dist_summary
# where rptmonthyear='202601'

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe
# MAGIC claims_mgmt.single_page_dist_summary 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe claims_mgmt.claims_mgmt_summary_fortnight

# COMMAND ----------

# MAGIC %skip
# MAGIC # %skip
# MAGIC df=( spark.sql("select * from claims_mgmt.test_delete")
# MAGIC )
# MAGIC
# MAGIC # df.write.mode("overwrite").saveAsTable("claims_mgmt.test_delete")

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC df_1=(
# MAGIC     df
# MAGIC     .withColumn('Final_Disallowance',col('Final_Disallowance')-col('settelement_check_Amount'))
# MAGIC     .withColumn('settelement_check_Amount',lit(0).cast('decimal(38,6)'))
# MAGIC     
# MAGIC )
# MAGIC df_1.cache()
# MAGIC df_1.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC %sql
# MAGIC restore table claims_mgmt.claims_mgmt_summary_fortnight to version as of 37

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC spark.sql(f'''delete from claims_mgmt.claims_mgmt_summary_fortnight where rptmonthyear="202601" ''').display()
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC # df_1.write.mode("append").saveAsTable("claims_mgmt.claims_mgmt_summary_fortnight")

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC %sql
# MAGIC select * from claims_mgmt.claims_mgmt_summary_fortnight
# MAGIC where RptMonthYear='202601' limit 5