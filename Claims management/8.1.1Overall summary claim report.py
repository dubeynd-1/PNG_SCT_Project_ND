# Databricks notebook source
# %sql
# select * from claims_mgmt.claims_mgmt_raw_report where SiteName = 'Mumbai' and RptMonthYear = '202601'

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
from pyspark.sql.types import NumericType

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
# MAGIC #### Distributor lookup table 

# COMMAND ----------

spark.sql(f''' select distinct DistributorCode, DistributorName from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy ''').createOrReplaceTempView("dist_names")

# COMMAND ----------

# MAGIC %md
# MAGIC ## For monthly

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_mgmt.claims_mgmt_summary_monthly

# COMMAND ----------

if end_date_value[-2:]!="14":
    result_month=spark.sql(f"""
    with df_base as (
            with base as (
            select c.RptMonthYear,c.SiteName,c.Distributor,c.DistributorName, 
            sum(c.Amount_Disbursed_Monthly) as AmountDisbursed_IHR,
            -- sum(CASE WHEN cs.initcode IS NOT NULL THEN Amount_Disbursed_Monthly end) as approved_amount,
            -- sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then Amount_Disbursed_Monthly end) as approved_amount,
            -- sum(case when (c.initiativeCode is not null and cs.initcode is null) and (c.initiativeCode is not null and m.scheme_Code is null) and (c.initiativeCode is not null and dc.initcode is null) and (Amount_Disbursed_Monthly>0) then Amount_Disbursed_Monthly else 0 end) as cs_not_approved,
            sum(case when c.InitiativeCode is not null and cs.initcode is null and dc.initcode is null and Amount_Disbursed_Monthly > 0 then Amount_Disbursed_Monthly else 0 end) as cs_not_approved,
            -- sum(case when (c.initiativeCode is not null and cs.initcode is null) and (c.initiativeCode is not null and dc.initcode is null) and (Amount_Disbursed_Monthly>0) then Amount_Disbursed_Monthly else 0 end) as cs_not_approved,
            0 as tsoi_adjustment,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.settelement_check_Amount end) as Disallowance_Settlement,
            -- sum(c.Wrong_Rate) as wrong_rate,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.Wrong_Rate end) as wrong_Rate,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.Wrong_date end) as wrong_date,
            -- sum(c.Free_Goods) as free_goods,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.free_goods end) as free_goods,
            -- sum(c.Wrong_Channel) as wrong_channel,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.Wrong_Channel end) as Wrong_Channel,
            0 as CN_AR_Adjustment,
            0 as CN_SRN,
            0 as Other_CN_Disallowance,
            0 as Cash_Claims,
            0 as UNTAGGED_SRN_RECOVERY
            from claims_mgmt.claims_mgmt_summary_monthly c
            LEFT JOIN 
            (select distinct initcode from stg.cs_combined where initcode in (select initiativeCode from claims_mgmt.claims_mgmt_summary_monthly) and rptmonthyear = "{formatted_date}") cs
            ON c.InitiativeCode = cs.initcode
            left join (select distinct scheme_code from sd_science.trade_plan_dtls_mapping_V1 where scheme_code in (select initiativeCode from claims_mgmt.claims_mgmt_summary_monthly)) m 
            on m.scheme_Code=c.InitiativeCode
            left join (select distinct initcode from claims_mgmt.dime_channel_summary where initcode in (select initiativeCode from claims_mgmt.claims_mgmt_summary_monthly) and rptmonthyear = "{formatted_date}") dc on dc.initcode=c.InitiativeCode
            where c.RptMonthYear = {formatted_date}
            group by c.RptMonthYear,c.SiteName,c.Distributor,c.DistributorName
            )
            select 
            RptMonthYear,
            sitename,Distributor,distributorname,amountdisbursed_ihr,tsoi_adjustment,
            cs_not_approved,
            0 as total_approved_amount,
            wrong_rate,
            wrong_date,
            free_goods,
            Disallowance_Settlement,
            wrong_channel,
            CN_AR_Adjustment,
            CN_SRN,
            Other_CN_Disallowance,
            Cash_Claims,
            UNTAGGED_SRN_RECOVERY
            from base
            order by DistributorName
            ),
        sales_return_df as (
        select c.RptMonthYear,c.SiteName,c.distributor,d.Distributorname,
        sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{end_date_value}' then amountdisbursed end) as total_ihr,
        -- sum(case when 
        --     (matrix_check_monthly_release!="Consider" or matrix_check_fortnight!="Consider") 
        --     and 
        --     (matrix_check_fortnight!="Consider" or dayofmonth(shipdate) <=14) 
        --     and 
        --     (ShipDate>= '{start_date_value}' and ShipDate<= '{end_date_value}')
        --     then amountdisbursed end) as not_consider_for_calculation,
        sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{end_date_value}' then amountdisbursed end) - sum(case when 
        (trim(upper(matrix_check_monthly_release))=upper("Consider") or trim(upper(matrix_check_fortnight))=upper("Consider")) 
        then Second_FortNight_IHR end) as not_consider_for_calculation,
        sum(case when date_format(srn_shipdate,'yyyyMM')= {formatted_date} and 
                      date_format(shipdate,'yyyyMM')= {formatted_date}
                      and (trim(upper(matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(shipdate) >14)
                      then Disallowance_SRN else 0 end
                      ) as sales_return_cm,
        sum(case when date_format(srn_shipdate,'yyyyMM')= {formatted_date} and 
                      date_format(shipdate,'yyyyMM')<> {formatted_date}
                      and (trim(upper(matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(shipdate) >14)
                      then Disallowance_SRN else 0 end
                      ) as sales_return_pm
        from claims_mgmt.claims_mgmt_raw_report c
        left join
        dist_names d
        on c.Distributor=d.DistributorCode
        where RptMonthYear = {formatted_date}
        group by c.RptMonthYear,c.SiteName,c.distributor,d.Distributorname
        ) 
        -- select a.*,
        -- b.sales_return_cm, b.sales_return_pm, b.total_ihr,
        -- (a.wrong_rate+a.wrong_date+a.free_goods+a.wrong_channel+b.sales_return_cm+b.sales_return_pm+a.cn_ar_adjustment+a.cn_srn+a.other_cn_disallowance+a.cash_claims+a.untagged_srn_recovery) as disallowance
        select a.RptMonthYear, a.distributor as Distributor_Code, a.sitename as Site_Name, a.distributorname as Distributor_Name,
        b.total_ihr as Overall_IHR, a.amountdisbursed_ihr as Consider_Month_IHR,
        b.not_consider_for_calculation as Not_Consider_For_Calculation,a.cs_not_approved as CS_Not_Approved, 
        a.wrong_rate as Wrong_Rate,a.wrong_date as Wrong_Date,a.wrong_channel as Wrong_channel,a.free_goods as Free_Goods,a.Disallowance_Settlement,
        b.sales_return_cm as Sales_Return_CM, 
        --b.sales_return_pm as Sales_Return_PM, 
        (a.cs_not_approved+a.wrong_rate+a.wrong_date+a.free_goods+a.wrong_channel+a.Disallowance_Settlement+b.sales_return_cm+b.sales_return_pm+a.cn_ar_adjustment+a.cn_srn+a.other_cn_disallowance+a.cash_claims+a.untagged_srn_recovery) as Total_Disallowance,
        (a.amountdisbursed_ihr-(a.cs_not_approved+a.wrong_rate+a.wrong_date+a.free_goods+a.Disallowance_Settlement+a.wrong_channel+b.sales_return_cm+b.sales_return_pm+a.cn_ar_adjustment+a.cn_srn+a.other_cn_disallowance+a.cash_claims+a.untagged_srn_recovery)) as Net_IHR_Disbursement
        from df_base a
        left join sales_return_df b 
        on a.RptMonthYear=b.RptMonthYear
        and TRIM(UPPER(a.SiteName))=TRIM(UPPER(b.SiteName))
        and TRIM(UPPER(a.distributor))=TRIM(UPPER(b.distributor))

        """)
    result_month.createOrReplaceTempView('result_month')
    
    spark.sql(f'''delete from claims_mgmt.overall_dist_summ_monthly_v1 where rptmonthyear="{formatted_date}" ''').display()

    result_month.write.mode("append").option("mergeSchema", "true").saveAsTable("claims_mgmt.overall_dist_summ_monthly_v1")
    display(result_month)

# COMMAND ----------

df_month=spark.table('claims_mgmt.overall_dist_summ_monthly_v1').filter(col('rptmonthyear')==formatted_date)
df_month_pretty = (df_month
                   .select([round(col(c),2).alias(c) if isinstance(df_month.schema[c].dataType,NumericType) else col(c) for c in df_month.columns])
                  .select(
                      [col(c).alias(c.replace("_"," ")) for c in df_month.columns]
                  )
                  .drop('Disallowance_Settlement')
)
display(df_month_pretty)

# COMMAND ----------

df_month=spark.table('claims_mgmt.overall_dist_summ_monthly_v1').filter(col('rptmonthyear')==formatted_date)
df_month_pretty = (df_month
                   .select([round(col(c),2).alias(c) if isinstance(df_month.schema[c].dataType,NumericType) else col(c) for c in df_month.columns])
                  .select(
                      [col(c).alias(c.replace("_"," ")) for c in df_month.columns]
                  )
                  .drop('Disallowance_Settlement')
)
display(df_month_pretty)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Provide monthly summary from above code

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fortnight

# COMMAND ----------

# MAGIC %md
# MAGIC ### cs_not_approved cahnged to only %MRI

# COMMAND ----------

if end_date_value[-2:]=="14":
    result_fort=spark.sql(f"""
    with df_base as (
            with base as (
            select c.RptMonthYear,c.SiteName,c.Distributor,c.DistributorName, 
            sum(c.Amount_Disbursed_Fortnight) as AmountDisbursed_IHR,
            -- sum(CASE WHEN cs.initcode IS NOT NULL THEN c.Amount_Disbursed_Fortnight end) as approved_amount,
            -- sum(case when (c.initiativeCode is not null and cs.initcode is null) and (c.initiativeCode is not null and m.scheme_Code is null) and (c.initiativeCode is not null and dc.initcode is null) 
            -- and (Amount_Disbursed_fortnight>0) then Amount_Disbursed_fortnight else 0 end) as cs_not_approved,
            -- sum(case when (c.initiativeCode is not null and cs.initcode is null) and (c.initiativeCode is not null and dc.initcode is null) and (Amount_Disbursed_fortnight>0) then Amount_Disbursed_fortnight else 0 end) as cs_not_approved,
            sum(case when c.InitiativeCode is not null and upper(c.InitiativeCode) like '%MRI'and cs.initcode is null and dc.initcode is null and Amount_Disbursed_Fortnight > 0 then Amount_Disbursed_Fortnight else 0 end) as cs_not_approved,
            -- sum(case when c.InitiativeCode is not null and cs.initcode is null and dc.initcode is null and Amount_Disbursed_Fortnight > 0 then Amount_Disbursed_Fortnight else 0 end) as cs_not_approved,
            0 as tsoi_adjustment,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.settelement_check_Amount end) as Disallowance_Settlement,
            -- sum(c.Wrong_Rate) as wrong_rate,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.Wrong_Rate end) as wrong_Rate,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.Wrong_date end) as wrong_date,
            -- sum(c.Free_Goods) as free_goods,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.free_goods end) as free_goods,
            -- sum(c.Wrong_Channel) as wrong_channel,
            sum(case when (c.initiativeCode is not null and cs.initcode is not null) or (c.initiativeCode is not null and m.scheme_Code is not null) or (c.initiativeCode is not null and dc.initcode is not null) then c.Wrong_Channel end) as Wrong_Channel,
            0 as CN_AR_Adjustment,
            0 as CN_SRN,
            0 as Other_CN_Disallowance,
            0 as Cash_Claims,
            0 as UNTAGGED_SRN_RECOVERY
            from claims_mgmt.claims_mgmt_summary_fortnight c
            LEFT JOIN
            (select distinct initcode from stg.cs_combined where initcode in (select initiativeCode from claims_mgmt.claims_mgmt_summary_fortnight) and rptmonthyear = "{formatted_date}") cs
            ON c.InitiativeCode = cs.initcode
            left join (select distinct scheme_code from sd_science.trade_plan_dtls_mapping_V1 where scheme_code in (select initiativeCode from claims_mgmt.claims_mgmt_summary_fortnight)) m 
            on m.scheme_Code=c.InitiativeCode
            left join (select distinct initcode from claims_mgmt.dime_channel_summary where initcode in (select initiativeCode from claims_mgmt.claims_mgmt_summary_fortnight) and rptmonthyear = "{formatted_date}") dc on dc.initcode=c.InitiativeCode
            where c.RptMonthYear = {formatted_date}
            group by c.RptMonthYear,c.SiteName,c.Distributor,c.DistributorName
            )
            select 
            RptMonthYear,
            sitename,Distributor,distributorname,amountdisbursed_ihr,tsoi_adjustment,
            0 as total_approved_amount,
            cs_not_approved,
            wrong_rate,
            Disallowance_Settlement,
            wrong_date,
            free_goods,
            wrong_channel,
            CN_AR_Adjustment,
            CN_SRN,
            Other_CN_Disallowance,
            Cash_Claims,
            UNTAGGED_SRN_RECOVERY
            from base
            order by DistributorName
            ),
        sales_return_df as (
        select c.RptMonthYear,c.SiteName,c.Distributor,d.Distributorname,
        sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' then amountdisbursed end) as total_ihr,
        -- sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' and (matrix_check_fortnight!="Consider" or dayofmonth(shipdate) >14) then amountdisbursed end) as not_consider_for_calculation,
        sum(case when ShipDate>= '{start_date_value}' and ShipDate<= '{f1_end}' then amountdisbursed end) - sum(case when 
        (matrix_check_fortnight='Consider' and shipdate>='{start_date_value}' and shipdate<='{end_date_value}') 
        then First_FortNight_IHR end) as not_consider_for_calculation,
        sum(case when date_format(srn_shipdate,'yyyyMM')= {formatted_date} and 
                      date_format(shipdate,'yyyyMM')= {formatted_date}
                      and trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(shipdate) <=14
                      then Disallowance_SRN else 0 end
                      ) as sales_return_cm,
        sum(case when date_format(srn_shipdate,'yyyyMM')= {formatted_date} and 
                      date_format(shipdate,'yyyyMM')<> {formatted_date}
                      and trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(shipdate) <=14
                      then Disallowance_SRN else 0 end
                      ) as sales_return_pm  
        -- cast(0 as double) as sales_return_cm,
        -- cast(0 as double) as sales_return_pm       
        from claims_mgmt.claims_mgmt_raw_report c
        left join
        dist_names d
        on c.Distributor=d.DistributorCode
        where RptMonthYear = {formatted_date}
        group by c.RptMonthYear,c.SiteName,c.Distributor,d.Distributorname
        )
        -- select a.*,
        -- b.sales_return_cm, b.sales_return_pm, b.total_ihr,
        -- (a.wrong_rate+a.wrong_date+a.free_goods+a.wrong_channel+b.sales_return_cm+b.sales_return_pm+a.cn_ar_adjustment+a.cn_srn+a.other_cn_disallowance+a.cash_claims+a.untagged_srn_recovery) as disallowance
        select
        a.RptMonthYear, a.distributor as Distributor_Code, a.sitename as Site_Name, a.distributorname as Distributor_Name,
        b.total_ihr as Overall_IHR, a.amountdisbursed_ihr as Consider_Fortnight_IHR,
        b.not_consider_for_calculation as Not_Consider_For_Calculation, a.cs_not_approved as CS_Not_Approved, 
        a.wrong_rate as Wrong_Rate,a.wrong_date as Wrong_Date,a.wrong_channel as Wrong_Channel,a.free_goods as Free_Goods,a.Disallowance_Settlement,
        b.sales_return_cm as Sales_Return_CM,
        -- b.sales_return_pm as Sales_Return_PM, 
        -- Removing Disallowance_SRN (b.sales_return_cm+b.sales_return_pm) from the Final disallowance calculation for fortnight, just keeping it for display purposes, will be included in monthly. also removing it from net ihr disbursement
        (a.cs_not_approved+a.wrong_rate+a.wrong_date+a.free_goods+a.Disallowance_Settlement+a.wrong_channel+a.cn_ar_adjustment+a.cn_srn+a.other_cn_disallowance+a.cash_claims+a.untagged_srn_recovery+b.sales_return_cm) as Total_Disallowance,
        (a.amountdisbursed_ihr-(a.cs_not_approved+a.wrong_rate+a.wrong_date+a.free_goods+a.Disallowance_Settlement+a.wrong_channel+a.cn_ar_adjustment+a.cn_srn+a.other_cn_disallowance+a.cash_claims+a.untagged_srn_recovery+b.sales_return_cm)) as Net_IHR_Disbursement

        from df_base a
        left join sales_return_df b 
        on a.RptMonthYear=b.RptMonthYear
        and TRIM(UPPER(a.SiteName))=TRIM(UPPER(b.SiteName))
        and TRIM(UPPER(a.Distributor))=TRIM(UPPER(b.Distributor))

        """)
    result_fort.createOrReplaceTempView('result_fort')
    
    spark.sql(f'''delete from claims_mgmt.overall_dist_summ_fortnight_v1 where rptmonthyear="{formatted_date}" ''').display()

    result_fort.write.mode("append").option("mergeSchema", "true").saveAsTable("claims_mgmt.overall_dist_summ_fortnight_v1")
    # display(result_fort)

# COMMAND ----------

# %sql
# UPDATE claims_mgmt.overall_dist_summ_fortnight_v1 
# SET cs_not_approved = 0.00
# where cs_not_approved >0 and RptMonthYear = '202602'

# COMMAND ----------


df_fort=spark.table('claims_mgmt.overall_dist_summ_fortnight_v1').filter(col('RptMonthYear')==formatted_date)
df_fort_pretty = (df_fort
                  .select([round(col(c),2).alias(c) if isinstance(df_fort.schema[c].dataType,NumericType) else col(c) for c in df_fort.columns])
                  .select(
                      [col(c).alias(c.replace("_"," ")) for c in df_fort.columns]
                  )
                  .drop('Disallowance_Settlement')
                  
)
display(df_fort_pretty)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


# df_fort=spark.table('claims_mgmt.overall_dist_summ_fortnight_v1').filter(col('RptMonthYear')=='202604')
# df_fort_pretty = (df_fort
#                   .select([round(col(c),2).alias(c) if isinstance(df_fort.schema[c].dataType,NumericType) else col(c) for c in df_fort.columns])
#                   .select(
#                       [col(c).alias(c.replace("_"," ")) for c in df_fort.columns]
#                   )
#                   .drop('Disallowance_Settlement')
                  
# )
# display(df_fort_pretty)

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(Disallowance_SRN) from claims_mgmt.claims_mgmt_raw_report where ShipDate >= '2026-02-01' and ShipDate <= '2026-02-14'  and RptMonthYear = '202602' and InitiativeCode like '%MRI' 
# MAGIC -- and srn_shipdate like '2026-02%' 
# MAGIC -- and shipdate like '2026-02%'