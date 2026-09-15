# Databricks notebook source
# DBTITLE 1,Import required libraries
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

dbutils.widgets.text("start_date","")
dbutils.widgets.text("end_date","")

# COMMAND ----------

# DBTITLE 1,Reading start date and end date
start_date=dbutils.widgets.get("start_date")
end_date=dbutils.widgets.get("end_date")
if start_date=="" and end_date=="":
    start_date_value = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
    end_date_value = (datetime.now()-timedelta(days=0)).strftime('%Y-%m-%d')
else:
    start_date_value=start_date
    end_date_value=end_date

date_obj = datetime.strptime(start_date_value, "%Y-%m-%d")
month_start = date_obj.replace(day=1).strftime("%Y-%m-%d")
formatted_date = date_obj.strftime("%Y%m")
ihr_date_format=start_date_value[:-2]+"%"

print(formatted_date)
print(month_start)
print(ihr_date_format)
print(start_date_value,end_date_value)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scheme master config

# COMMAND ----------

# DBTITLE 1,INSERT INTO SCHEME MASTER CONFIG
# %sql
# -- DELETE FROM stg.scheme_master_conf
# -- WHERE initiativecode IN ('LBBE____N');

# INSERT INTO stg.scheme_master_conf (
#     initiativecode,
#     `description_used_y/n`,
#     what_is_used,
#     type,
#     account_or_channel,
#     scheme_type,
#     sub_type,
#     matrix_check_fortnight,
#     matrix_check_monthly_release,
#     `period_monthly_/_quaterly/yearly`,
#     scheme_code_consideration_remark_1,
#     scheme_code_consideration_remark_2
# )
# VALUES
# ('LDG____N','Yes','Ullage','Ullage','MR','P&G - Quatarly','Regular','Not Consider','Not Consider','Monthly','Not Consider-Ullage','Not Consider'),

# ('SmallALL','No','No','Site','Not Consider','Not Consider','Monthly','Not Consider','Not Consider','Monthly','Not Consider - Site Plan','Not Consider - Site Plan'),

# ('SmallAriel','No','No','Site','Not Consider','Not Consider','Monthly','Not Consider','Not Consider','Monthly','Not Consider - Site Plan','Not Consider - Site Plan'),

# ('VTCFS018','No','No','Site','Not Consider','Not Consider','Monthly','Not Consider','Not Consider','Monthly','Not Consider - Site Plan','Not Consider - Site Plan'),

# ('WSSDPL','No','No','Site','Not Consider','Not Consider','Monthly','Not Consider','Not Consider','Monthly','Not Consider - Site Plan','Not Consider - Site Plan');

# COMMAND ----------

# DBTITLE 1,Scheme master config
spark.sql(f'''select initiativecode,`description_used_y/n` as desc_flg, what_is_used as desc,matrix_check_fortnight,matrix_check_monthly_release,scheme_code_consideration_remark_1,scheme_code_consideration_remark_2 from stg.scheme_master_conf''').createOrReplaceTempView("smt")

### filter schemes valid for this fortnight/month
spark.sql(f'''
          select *except(rn) from (select *,  row_number() over(partition by initiativecode order by case when upper(trim(matrix_check_fortnight))=upper("Not Consider") then 1 else 0 end desc) as rn from (
          select /*+ Broadcast(t2) */
 distinct t1.initiativecode,t1.initiativename, t2.initiativecode as pattern_scheme,matrix_check_fortnight,scheme_code_consideration_remark_1,matrix_check_monthly_release,scheme_code_consideration_remark_2,desc_flg,desc
  from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr t1 left join smt t2 on t1.InitiativeCode like concat(t2.initiativecode,"%") and (t2.desc_flg="No" OR (t2.desc_flg='Yes' and locate(upper(t2.desc),(upper(t1.initiativename)))>0 ))
  where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}'
) )where rn=1
          ''').createOrReplaceTempView("vs_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC select initiativecode,`description_used_y/n` as desc_flg, what_is_used as desc,matrix_check_fortnight,matrix_check_monthly_release,scheme_code_consideration_remark_1,scheme_code_consideration_remark_2 from stg.scheme_master_conf

# COMMAND ----------

# MAGIC %md
# MAGIC # pattern mapping below

# COMMAND ----------

#Send this list to Sandip for verifying

#Action: If any schemes codes don't match a pattern, Sandip updates the scheme master config and sends it, use that file to refresh the table and check again


#Can refresh the table either by uploading file as csv and reading data from there or using excel formulas to convert data into dataframe format and use the scheme master config update script after changing the 'data' array in the first cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vs_list
# MAGIC WHERE pattern_scheme IS NULL
# MAGIC -- #   AND initiativecode RLIKE '[A-Za-z]$';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Missing scheme codes - Master

# COMMAND ----------

# DBTITLE 1,Master missing codes
missing_master_df = (
    spark.sql(f"""
              select distinct date_format(shipdate,'yyyyMM') as rptmonthyr,InitiativeCode, sum(amountdisbursed) as amount_disbursed,
              case when InitiativeCode not like 'L%' then 'Sitecode' else 'Normal' end as type_of_code
              from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr
              where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and InitiativeCode not in (
                select scheme_code from (
                select scheme_code from sd_science.trade_plan_dtls_master
                union
                select scheme_code from stg.masters_auditex
                ) where scheme_code is not null
              )
              group by InitiativeCode, case when InitiativeCode not like 'L%' then 'Sitecode' else 'Normal' end, date_format(shipdate,'yyyyMM')
              order by amount_disbursed desc
              """)
)
display(missing_master_df)

# missing_master_df(filter)
# Send this list to Sandip for verifying

# COMMAND ----------

# MAGIC %md
# MAGIC #### Missing scheme codes - Mapping

# COMMAND ----------

# DBTITLE 1,Mapping missing codes
missing_mapping_df = (
    spark.sql(f"""
              select distinct date_format(shipdate,'yyyyMM') as rptmonthyr,InitiativeCode, sum(amountdisbursed) as amount_disbursed,
              case when InitiativeCode not like 'L%' then 'Sitecode' 
              else 'Normal' end as type_of_code
              from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr
              where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and InitiativeCode not in (
                select scheme_code from (
                select scheme_code from sd_science.trade_plan_dtls_mapping_v1
                ) where scheme_code is not null
              )
              group by InitiativeCode, case when InitiativeCode not like 'L%' then 'Sitecode' else 'Normal' end, date_format(shipdate,'yyyyMM')
              order by amount_disbursed desc
              """)
)
display(missing_mapping_df)
# Send this list to Sandip for verifying

# COMMAND ----------

# MAGIC %md
# MAGIC #### Missing from both channel and dime channel summary

# COMMAND ----------

missing_comb_cs_df = spark.sql(f"""
    select distinct
        date_format(shipdate,'yyyyMM') as rptmonthyr,sum(amountdisbursed) as amount,
        InitiativeCode,InitiativeName,InitiativeEndDate,
        case when InitiativeCode not like 'L%' then 'Sitecode' else 'Normal' end as type_of_code
    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr
    where ShipDate >= '{start_date_value}'
      and ShipDate <= '{end_date_value}'
      and InitiativeEndDate >='{month_start}'
      and InitiativeCode not in (
          select scheme_code from (
              select initcode as scheme_code
              from stg.cs_combined
              where rptmonthyear = '{formatted_date}'
              union
              select initcode as scheme_code
              from claims_mgmt.dime_channel_summary
              where rptmonthyear = '{formatted_date}'
          )
          where scheme_code is not null
      )
    group by
        InitiativeCode,InitiativeEndDate,InitiativeName,
        case when InitiativeCode not like 'L%' then 'Sitecode' else 'Normal' end,
        date_format(shipdate,'yyyyMM')
""")


display(missing_comb_cs_df)


# COMMAND ----------

# DBTITLE 1,TEST FOR THREE MONTHS
# First load the channel summary and dime summary tables from the claims ingestion script and then do this check to make sure we have updated information in the tables

# missing_comb_cs_df = (
#     spark.sql(f"""
#               select distinct date_format(shipdate,'yyyyMM') as rptmonthyr,InitiativeCode,
#               case when InitiativeCode not like 'L%' then 'Sitecode' else 'Normal' end as type_of_code
#               from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr
#               where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and InitiativeCode not in (
#                 select scheme_code from (
#                 select initcode as scheme_code from stg.cs_combined
#                 where rptmonthyear between date_format(to_date('{start_date_value}'),'yyyyMM') and date_format(to_date('{end_date_value}'),'yyyyMM')
#                 union
#                 select initcode as scheme_code from claims_mgmt.dime_channel_summary
#                 where rptmonthyear between date_format(to_date('{start_date_value}'),'yyyyMM') and date_format(to_date('{end_date_value}'),'yyyyMM')
#                 ) where scheme_code is not null
#               )
#               group by InitiativeCode, case when InitiativeCode not like 'L%' then 'Sitecode' else 'Normal' end, date_format(shipdate,'yyyyMM')
#               """)
# )
# display(missing_comb_cs_df)
# Send this list to Sandip for verifying

# COMMAND ----------

# DBTITLE 1,Checking for overlapping scheme codes in CS and DCS
# MAGIC %skip
# MAGIC # Checking for schemes present in both channel summary and dime channel summary
# MAGIC overlap_df = (spark.sql(f"""
# MAGIC select distinct cs.scheme_code
# MAGIC from ( select initcode as scheme_code from stg.cs_combined
# MAGIC                 where rptmonthyear='{formatted_date}') cs
# MAGIC inner join
# MAGIC (select initcode as scheme_code from claims_mgmt.dime_channel_summary
# MAGIC                 where rptmonthyear='{formatted_date}') dcs
# MAGIC on cs.scheme_code=dcs.scheme_code
# MAGIC                 
# MAGIC                 """)
# MAGIC )
# MAGIC display(overlap_df)

# COMMAND ----------

# DBTITLE 1,Only retailer level schemes should be present in DCS
# MAGIC %skip
# MAGIC
# MAGIC # Identifying schemes that are not retailer level but are present in dime channel summary
# MAGIC ret_dcs = (
# MAGIC     spark.sql(f"""
# MAGIC               select distinct dcs.scheme_code
# MAGIC               from (
# MAGIC                  select initcode as scheme_code from claims_mgmt.dime_channel_summary
# MAGIC                 where rptmonthyear='{formatted_date}' 
# MAGIC               ) dcs
# MAGIC               left anti join retailer_schemes rls
# MAGIC               on dcs.scheme_code=rls.scheme_code
# MAGIC               """)
# MAGIC )
# MAGIC display(ret_dcs)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg.masters_auditex where Scheme_Code = 'LTR2607N00008283'

# COMMAND ----------

