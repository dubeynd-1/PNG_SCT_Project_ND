# Databricks notebook source
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

current_date = (datetime.now()-timedelta(days=0)).strftime('%Y-%m-%d')
print(current_date)
previous_date = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
print(previous_date)

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
formatted_date_ext = date_obj.strftime("%y%m")
print(formatted_date_ext)

# COMMAND ----------

if end_date_value[-2:]=="14":
    print("Fortnight")
else:
    print("Monthly")

# COMMAND ----------

# DBTITLE 1,claims_mgmt_report - raw data
spark.sql(f'''
          select * from claims_mgmt.claims_mgmt_raw_report where RptMonthYear='{formatted_date}'
          ''').createOrReplaceTempView("raw_data")

spark.sql(f''' select distinct Distributor_Code as DistributorCode, Distributor_Name as DistributorName from cdl_india_data_prod.india_distributordata_refined.tblbasemstr_locationhierarchy ''').createOrReplaceTempView("dist_names")

# COMMAND ----------

# DBTITLE 1,mapping data
spark.sql(f'''
          select distinct scheme_code,Distributor_code,Branch_code,retailer_Code,upper(channel_name) as mapping_channel,upper(type_name) as mapping_sub_channel, case  when retailer_code is not null then "Retailer Level Scheme" when Retailer_Code is null then "Channel Level Scheme" end as scheme_type
                   from sd_Science.trade_plan_Dtls_mapping_v1 where scheme_code in (select initiativecode from raw_data) 
          ''').createOrReplaceTempView("mapping")

spark.sql(f'''
          select * from(select *, row_number() over(partition by scheme_code order by case when scheme_type="Channel Level Scheme" then 1 else 0 end desc) as rn from (select distinct scheme_code,scheme_type from mapping ) )where rn=1
          ''').createOrReplaceTempView("scheme_Type")

spark.sql(f'''
          select scheme_Code,Distributor_code, collect_set(mapping_sub_channel) as sub_channels from mapping group by scheme_Code,Distributor_code
          ''').createOrReplaceTempView("mapping_sub_channel")

# COMMAND ----------

spark.sql(f'''
 select distinct invcode,DocNumber,Retailer_Code,Channel_Org ,Transaction_Customer_Type from cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails inner join 
      (select distinct ApplyToDocNum,Docnumber from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails where DocNumber in  (select distinct SalesInvoiceNo  from claims_mgmt.claims_mgmt_report where substring(split(SalesInvoiceNo,"-")[2],1,1)="R" and ApplyToDocNum is not null )) on invcode=ApplyToDocNum
      
 ''').createOrReplaceTempView("return_sub_channels")

# COMMAND ----------

spark.sql(f'''select initiativecode,`description_used_y/n` as desc_flg, what_is_used as desc,matrix_check_fortnight,matrix_check_monthly_release,scheme_code_consideration_remark_1,scheme_code_consideration_remark_2,type as type_of_brand from stg.scheme_master_conf''').createOrReplaceTempView("smt")

### filter schemes valid for this fortnight/month
spark.sql(f'''
          
          select distinct initiativecode,type_of_brand  from (select *,  row_number() over(partition by initiativecode order by case when upper(trim(matrix_check_fortnight))=upper("Not Consider") then 1 else 0 end desc) as rn from (
          select /*+ Broadcast(t2) */
 distinct t1.initiativecode,matrix_check_fortnight,scheme_code_consideration_remark_1,matrix_check_monthly_release,scheme_code_consideration_remark_2,desc_flg,desc,type_of_brand
  from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr t1 join smt t2 on t1.InitiativeCode like concat(t2.initiativecode,"%") where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and (t2.desc_flg="No" OR (t2.desc_flg='Yes' and locate(upper(t2.desc),(upper(t1.initiativename)))>0 ))
) )where rn=1
          ''').createOrReplaceTempView("type_of_brand")

# COMMAND ----------

# MAGIC %md
# MAGIC #### original code for FULL MONTHLY before sales_return_cm/pm inclusion

# COMMAND ----------

# DBTITLE 1,orig- fortnightly
# MAGIC %skip
# MAGIC if end_date_value[-2:]=="14":
# MAGIC     result=spark.sql(f'''
# MAGIC     with agg_data as (
# MAGIC         select Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
# MAGIC         sum(case when date_format(shipdate,"yyyyMM")="{formatted_date}" then AmountDisbursed else 0 end) as AmountDisbursed_IHR,
# MAGIC         sum(First_FortNight_IHR) as Amount_Disbursed_Fortnight,
# MAGIC         sum(Second_FortNight_IHR) as Amount_Disbursed_Monthly,
# MAGIC
# MAGIC         sum(case when date_format(srn_shipdate,'yyyyMM')="{formatted_date}"
# MAGIC                  and date_format(shipdate,'yyyyMM')="{formatted_date}"
# MAGIC                  and ((trim(upper(matrix_check_monthly_release))="CONSIDER"
# MAGIC                  and trim(upper(matrix_check_fortnight))="NOT CONSIDER")
# MAGIC                  or (trim(upper(matrix_check_fortnight))="CONSIDER"
# MAGIC                  and dayofmonth(shipdate)>14))
# MAGIC             then Disallowance_SRN else 0 end) as sales_return_cm,
# MAGIC
# MAGIC         sum(case when date_format(srn_shipdate,'yyyyMM')="{formatted_date}"
# MAGIC                  and date_format(shipdate,'yyyyMM')=date_format(add_months(to_date('{formatted_date}','yyyyMM'),-1),'yyyyMM')
# MAGIC                  and ((trim(upper(matrix_check_monthly_release))="CONSIDER"
# MAGIC                  and trim(upper(matrix_check_fortnight))="NOT CONSIDER")
# MAGIC                  or (trim(upper(matrix_check_fortnight))="CONSIDER"
# MAGIC                  and dayofmonth(shipdate)>14))
# MAGIC             then Disallowance_SRN else 0 end) as sales_return_pm,
# MAGIC
# MAGIC         sum(Wrong_Rate) as Wrong_Rate,
# MAGIC         sum(case when date_format(shipdate,"yyyyMM")="{formatted_date}" and left(InitiativeCode,3)="LFG" then AmountDisbursed else 0 end) as Free_Goods,
# MAGIC         sum(case when Remark_Channel_Validation is not null and AmountDisbursed>=0
# MAGIC                  and InitiativeCode NOT LIKE 'LTR____E%'
# MAGIC                  and left(InitiativeCode,3)!="LFG"
# MAGIC             then AmountDisbursed else 0 end) as Wrong_Channel,
# MAGIC         sum(case when Date_Check="Mismatch" then AmountDisbursed else 0 end) as Wrong_Date,
# MAGIC         sum(Disallowance_Settlement) as settelement_check_Amount
# MAGIC
# MAGIC         from (
# MAGIC             select t7.*except(SubChannelName),
# MAGIC                    coalesce(SubChannelName,Transaction_Customer_Type) as SubChannelName,
# MAGIC                    DistributorName
# MAGIC             from raw_data t7
# MAGIC             left join return_sub_channels t6
# MAGIC                 on t7.salesinvoiceno=t6.DocNumber
# MAGIC             left join dist_names t5
# MAGIC                 on t5.DistributorCode=t7.Distributor
# MAGIC             where matrix_check_fortnight="Consider"
# MAGIC               and shipdate>="{start_date_value}"
# MAGIC               and shipdate<="{end_date_value}"
# MAGIC         )
# MAGIC         group by Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
# MAGIC     ),
# MAGIC
# MAGIC     final_agg as (
# MAGIC         select *,
# MAGIC                sales_return_pm + sales_return_cm as Disallowance_SRN
# MAGIC         from agg_data
# MAGIC     )
# MAGIC
# MAGIC     select Distributor,DistributorName,SiteName,t1.InitiativeCode,
# MAGIC            t4.scheme_Type,InitiativeName,
# MAGIC            AmountDisbursed_IHR,Amount_Disbursed_Fortnight,
# MAGIC            Amount_Disbursed_Monthly,type_of_brand,
# MAGIC            Disallowance_SRN,sales_return_cm,sales_return_pm,
# MAGIC            Wrong_Rate,Free_Goods,Wrong_Channel,Wrong_Date,
# MAGIC            settelement_check_Amount,
# MAGIC            Wrong_Rate-Free_Goods+Wrong_channel+Wrong_Date+settelement_check_Amount
# MAGIC                as Final_Disallowance,
# MAGIC            '{formatted_date}' as RptMonthYear
# MAGIC
# MAGIC     from final_agg t1
# MAGIC     left join type_of_brand t2
# MAGIC         on t1.InitiativeCode=t2.InitiativeCode
# MAGIC     left join scheme_Type t4
# MAGIC         on t4.scheme_Code=t1.InitiativeCode
# MAGIC     ''')
# MAGIC
# MAGIC     spark.sql(f'''
# MAGIC         delete from claims_mgmt.claims_mgmt_summary_fortnight
# MAGIC         where rptmonthyear="{formatted_date}"
# MAGIC     ''').display()
# MAGIC
# MAGIC     result.write.mode("append").option("mergeSchema","true").saveAsTable(
# MAGIC         "claims_mgmt.claims_mgmt_summary_fortnight"
# MAGIC     )

# COMMAND ----------

# DBTITLE 1,orig monthyl
# if end_date_value[-2:]!="14":
#     result=spark.sql(f'''
#     WITH agg_data AS (
#         SELECT Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
#         sum(case when date_format(shipdate,"yyyyMM")="{formatted_date}" then AmountDisbursed else 0 end) as AmountDisbursed_IHR,
#         sum(First_FortNight_IHR) as Amount_Disbursed_Fortnight,
#         sum(Second_FortNight_IHR) as Amount_Disbursed_Monthly,
#         sum(case when date_format(srn_shipdate,'yyyyMM')="{formatted_date}" and date_format(shipdate,'yyyyMM')="{formatted_date}" and ((trim(upper(matrix_check_monthly_release))="CONSIDER" and trim(upper(matrix_check_fortnight))="NOT CONSIDER") or (trim(upper(matrix_check_fortnight))="CONSIDER" and dayofmonth(shipdate)>14)) then Disallowance_SRN else 0 end) as sales_return_cm,
#         sum(case when date_format(srn_shipdate,'yyyyMM')="{formatted_date}" and date_format(shipdate,'yyyyMM')=date_format(add_months(to_date('{formatted_date}','yyyyMM'),-1),'yyyyMM') and ((trim(upper(matrix_check_monthly_release))="CONSIDER" and trim(upper(matrix_check_fortnight))="NOT CONSIDER") or (trim(upper(matrix_check_fortnight))="CONSIDER" and dayofmonth(shipdate)>14)) then Disallowance_SRN else 0 end) as sales_return_pm
#         FROM (
#             select t7.*except(SubChannelName),coalesce(SubChannelName,Transaction_Customer_Type) as SubChannelName,DistributorName
#             from raw_data t7
#             left join return_sub_channels t6 on t7.salesinvoiceno=t6.DocNumber
#             left join dist_names t5 on t5.DistributorCode=t7.Distributor
#             where matrix_check_fortnight="Consider" or matrix_check_monthly_release="Consider"
#         )
#         GROUP BY Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
#     ),
#     agg_data_final AS (
#         SELECT *, sales_return_cm + sales_return_pm AS Disallowance_SRN
#         FROM agg_data
#     ),
#     agg_data_pc AS (
#         SELECT Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
#         sum(Wrong_Rate) as Wrong_Rate,
#         sum(case when date_format(shipdate,"yyyyMM")="{formatted_date}" and left(InitiativeCode,3)="LFG" then AmountDisbursed else 0 end) as Free_Goods,
#         sum(case when Remark_Channel_Validation is not null and AmountDisbursed>=0 and InitiativeCode NOT LIKE 'LTR____E%' and left(InitiativeCode,3)!="LFG" then AmountDisbursed else 0 end) as Wrong_Channel,
#         sum(case when Date_Check="Mismatch" then AmountDisbursed else 0 end) as Wrong_Date,
#         sum(Disallowance_Settlement) as settelement_check_Amount
#         FROM (
#             select t7.*except(SubChannelName),coalesce(SubChannelName,Transaction_Customer_Type) as SubChannelName,DistributorName
#             from raw_data t7
#             left join return_sub_channels t6 on t7.salesinvoiceno=t6.DocNumber
#             left join dist_names t5 on t5.DistributorCode=t7.Distributor
#             where ((trim(upper(matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider"))
#             or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(shipdate)>14))
#         )
#         GROUP BY Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
#     )
#     SELECT
#         t1.Distributor,t1.DistributorName,t1.SiteName,t1.InitiativeCode,t4.scheme_Type,t1.InitiativeName,
#         t1.AmountDisbursed_IHR,t1.Amount_Disbursed_Fortnight,t1.Amount_Disbursed_Monthly,type_of_brand,
#         t1.Disallowance_SRN,t1.sales_return_cm,t1.sales_return_pm,
#         Wrong_Rate,Free_Goods,Wrong_Channel,Wrong_Date,settelement_check_Amount,
#         t1.Disallowance_SRN+Wrong_Rate-Free_Goods+Wrong_Channel+Wrong_Date+settelement_check_Amount as Final_Disallowance,
#         '{formatted_date}' as RptMonthYear
#     FROM agg_data_final t1
#     LEFT JOIN agg_data_pc t6 ON t1.Distributor=t6.Distributor
#         AND upper(trim(t1.SiteName))=upper(trim(t6.SiteName))
#         AND t1.InitiativeCode=t6.InitiativeCode
#     LEFT JOIN type_of_brand t2 ON t1.InitiativeCode=t2.InitiativeCode
#     LEFT JOIN scheme_Type t4 ON t4.scheme_Code=t1.InitiativeCode
#     ''')
    
#     spark.sql(f'''DELETE FROM claims_mgmt.claims_mgmt_summary_monthly WHERE rptmonthyear="{formatted_date}"''')
#     result.write.mode("append").option("mergeSchema","true").saveAsTable("claims_mgmt.claims_mgmt_summary_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### MONTHLY

# COMMAND ----------

# if end_date_value[-2:]!="14":
#     result=spark.sql(f'''
#     WITH all_schemes AS (
#         SELECT DISTINCT CASE WHEN TRIM(initcode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(initcode),1,LENGTH(TRIM(initcode))-1) ELSE TRIM(initcode) END initcode FROM stg.cs_combined WHERE rptmonthyear="{formatted_date}"
#         UNION
#         SELECT DISTINCT CASE WHEN TRIM(initcode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(initcode),1,LENGTH(TRIM(initcode))-1) ELSE TRIM(initcode) END initcode FROM claims_mgmt.dime_channel_summary WHERE rptmonthyear="{formatted_date}"
#     ),
#     failed_schemes AS (
#         SELECT DISTINCT TRIM(cs.initcode) initcode FROM stg.cs_combined cs LEFT JOIN stg.masters_auditex s ON TRIM(cs.initcode)=TRIM(s.scheme_code) WHERE cs.rptmonthyear="{formatted_date}" AND cs.disbursement_count!=s.Retailer_Apply_Count
#         UNION
#         SELECT DISTINCT TRIM(dc.initcode) initcode FROM claims_mgmt.dime_channel_summary dc LEFT JOIN stg.masters_auditex s ON TRIM(dc.initcode)=TRIM(s.scheme_code) WHERE dc.rptmonthyear="{formatted_date}" AND dc.disbursement_count!=s.Retailer_Apply_Count
#     ),
#     validation_schemes AS (
#         SELECT DISTINCT TRIM(scheme_code) initcode FROM stg.masters_auditex
#     ),
# cs_validation AS (
#     SELECT DISTINCT TRIM(c.InitiativeCode) InitiativeCode,
#     CASE WHEN f.initcode IS NOT NULL THEN 'CHECK FAILED IN CS'
#          WHEN v.initcode IS NOT NULL AND a.initcode IS NULL THEN 'MISSING IN CS'
#          WHEN a.initcode IS NOT NULL THEN 'SCHEME EXIST IN CS' END scheme_status
#     FROM raw_data c
#     LEFT JOIN validation_schemes v 
#         ON TRIM(c.InitiativeCode)=v.initcode
#     LEFT JOIN all_schemes a 
#         ON CASE WHEN TRIM(c.InitiativeCode) RLIKE '^LTR....N.*[A-Z]$'
#                 THEN SUBSTRING(TRIM(c.InitiativeCode),1,LENGTH(TRIM(c.InitiativeCode))-1)
#                 ELSE TRIM(c.InitiativeCode) END=a.initcode
#     LEFT JOIN failed_schemes f 
#         ON CASE WHEN TRIM(c.InitiativeCode) RLIKE '^LTR....N.*[A-Z]$'
#                 THEN SUBSTRING(TRIM(c.InitiativeCode),1,LENGTH(TRIM(c.InitiativeCode))-1)
#                 ELSE TRIM(c.InitiativeCode) END=f.initcode
#     WHERE c.InitiativeCode IS NOT NULL
# ),
#     agg_data AS (
#         SELECT Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
#         SUM(CASE WHEN date_format(shipdate,"yyyyMM")="{formatted_date}" THEN AmountDisbursed ELSE 0 END) AmountDisbursed_IHR,
#         SUM(First_FortNight_IHR) Amount_Disbursed_Fortnight,SUM(Second_FortNight_IHR) Amount_Disbursed_Monthly,
#         SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}" AND date_format(shipdate,'yyyyMM')="{formatted_date}" AND ((TRIM(UPPER(matrix_check_monthly_release))="CONSIDER" AND TRIM(UPPER(matrix_check_fortnight))="NOT CONSIDER") OR (TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)>14)) THEN Disallowance_SRN ELSE 0 END) sales_return_cm,
#         SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}" AND date_format(shipdate,'yyyyMM')=date_format(add_months(to_date('{formatted_date}','yyyyMM'),-1),'yyyyMM') AND ((TRIM(UPPER(matrix_check_monthly_release))="CONSIDER" AND TRIM(UPPER(matrix_check_fortnight))="NOT CONSIDER") OR (TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)>14)) THEN Disallowance_SRN ELSE 0 END) sales_return_pm
#         FROM (SELECT t7.* EXCEPT(SubChannelName),COALESCE(SubChannelName,Transaction_Customer_Type) SubChannelName,DistributorName FROM raw_data t7 LEFT JOIN return_sub_channels t6 ON t7.salesinvoiceno=t6.DocNumber LEFT JOIN dist_names t5 ON t5.DistributorCode=t7.Distributor WHERE matrix_check_fortnight="Consider" OR matrix_check_monthly_release="Consider")
#         GROUP BY Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
#     ),
#     agg_data_final AS (
#         SELECT *,sales_return_cm+sales_return_pm Disallowance_SRN FROM agg_data
#     ),
#     agg_data_pc AS (
#         SELECT Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
#         SUM(Wrong_Rate) Wrong_Rate,
#         SUM(CASE WHEN date_format(shipdate,"yyyyMM")="{formatted_date}" AND LEFT(InitiativeCode,3)="LFG" THEN AmountDisbursed ELSE 0 END) Free_Goods,
#         SUM(CASE WHEN Remark_Channel_Validation IS NOT NULL AND AmountDisbursed>=0 AND InitiativeCode NOT LIKE 'LTR____E%' AND LEFT(InitiativeCode,3)!="LFG" THEN AmountDisbursed ELSE 0 END) Wrong_Channel,
#         SUM(CASE WHEN Date_Check="Mismatch" THEN AmountDisbursed ELSE 0 END) Wrong_Date,
#         SUM(Disallowance_Settlement) settelement_check_Amount
#         FROM (SELECT t7.* EXCEPT(SubChannelName),COALESCE(SubChannelName,Transaction_Customer_Type) SubChannelName,DistributorName FROM raw_data t7 LEFT JOIN return_sub_channels t6 ON t7.salesinvoiceno=t6.DocNumber LEFT JOIN dist_names t5 ON t5.DistributorCode=t7.Distributor WHERE (TRIM(UPPER(matrix_check_monthly_release))=UPPER("Consider") AND TRIM(UPPER(matrix_check_fortnight))=UPPER("Not Consider")) OR (TRIM(UPPER(matrix_check_fortnight))=UPPER("Consider") AND dayofmonth(shipdate)>14))
#         GROUP BY Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
#     )
#     SELECT t1.Distributor,t1.DistributorName,t1.SiteName,t1.InitiativeCode,t4.scheme_Type,t1.InitiativeName,
#     t1.AmountDisbursed_IHR,t1.Amount_Disbursed_Fortnight,
#     CASE WHEN cv.scheme_status IN ('CHECK FAILED IN CS','MISSING IN CS') AND t1.Amount_Disbursed_Monthly >= 0
#      THEN 0
#      ELSE t1.Amount_Disbursed_Monthly
# END Amount_Disbursed_Monthly,type_of_brand,
#     t1.Disallowance_SRN,t1.sales_return_cm,t1.sales_return_pm,
#     Wrong_Rate,Free_Goods,Wrong_Channel,Wrong_Date,settelement_check_Amount,
#     t1.Disallowance_SRN+Wrong_Rate-Free_Goods+Wrong_Channel+Wrong_Date+settelement_check_Amount Final_Disallowance,
# --     CASE WHEN cv.scheme_status IN ('CHECK FAILED IN CS','MISSING IN CS')
# --      THEN GREATEST(t1.Amount_Disbursed_Monthly,0)
# --      ELSE 0
# -- END CS_NOT_APPROVED,
#     COALESCE(cv.scheme_status,'SCHEME EXIST IN CS') scheme_status,
#     '{formatted_date}' RptMonthYear
#     FROM agg_data_final t1
#     LEFT JOIN agg_data_pc t6 ON t1.Distributor=t6.Distributor AND UPPER(TRIM(t1.SiteName))=UPPER(TRIM(t6.SiteName)) AND t1.InitiativeCode=t6.InitiativeCode
#     LEFT JOIN type_of_brand t2 ON t1.InitiativeCode=t2.InitiativeCode
#     LEFT JOIN scheme_Type t4 ON t4.scheme_Code=t1.InitiativeCode
#     LEFT JOIN cs_validation cv ON TRIM(t1.InitiativeCode)=cv.InitiativeCode
#     ''')

#     spark.sql(f'''DELETE FROM claims_mgmt.claims_mgmt_summary_monthly WHERE rptmonthyear="{formatted_date}"''')
#     result.write.mode("append").option("mergeSchema","true").saveAsTable("claims_mgmt.claims_mgmt_summary_monthly")

# COMMAND ----------

if end_date_value[-2:]!="14":
    result=spark.sql(f'''
    WITH all_schemes AS (
        SELECT DISTINCT CASE WHEN TRIM(initcode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(initcode),1,LENGTH(TRIM(initcode))-1) ELSE TRIM(initcode) END initcode
        FROM stg.cs_combined WHERE rptmonthyear="{formatted_date}" AND TO_DATE(COALESCE(TRY_TO_TIMESTAMP(end_date,'M/d/yy'),TRY_TO_TIMESTAMP(end_date,'d-MMM-yy'), to_date(end_date, 'yyyy-MM-dd HH:mm:ss'), to_date(end_date, 'yyyy/MM/dd') ))>=DATE('{start_date_value}')
        UNION
        SELECT DISTINCT CASE WHEN TRIM(initcode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(initcode),1,LENGTH(TRIM(initcode))-1) ELSE TRIM(initcode) END
        FROM claims_mgmt.dime_channel_summary WHERE rptmonthyear="{formatted_date}" AND TO_DATE(end_date)>=DATE('{start_date_value}')
    ),
failed_schemes AS (
    SELECT DISTINCT TRIM(cs.initcode) initcode FROM stg.cs_combined cs LEFT JOIN stg.masters_auditex s ON TRIM(cs.initcode)=TRIM(s.scheme_code)
    WHERE cs.rptmonthyear="{formatted_date}" AND cs.disbursement_count!=s.Retailer_Apply_Count
    AND TO_DATE(COALESCE(TRY_TO_TIMESTAMP(cs.end_date,'M/d/yy'),TRY_TO_TIMESTAMP(cs.end_date,'d-MMM-yy'), to_date(cs.end_date, 'yyyy-MM-dd HH:mm:ss'), to_date(cs.end_date, 'yyyy/MM/dd') ))>=DATE('{start_date_value}')
    
    UNION
    
    SELECT DISTINCT TRIM(dc.initcode) initcode FROM claims_mgmt.dime_channel_summary dc LEFT JOIN stg.masters_auditex s ON TRIM(dc.initcode)=TRIM(s.scheme_code)
    WHERE dc.rptmonthyear="{formatted_date}" AND dc.disbursement_count!=s.Retailer_Apply_Count
    AND TO_DATE(dc.end_date)>=DATE('{start_date_value}')
),
    validation_schemes AS (
        SELECT DISTINCT TRIM(scheme_code) initcode FROM stg.masters_auditex
    ),
    cs_validation AS (
        SELECT DISTINCT TRIM(c.InitiativeCode) InitiativeCode,
        CASE WHEN f.initcode IS NOT NULL THEN 'CHECK FAILED IN CS'
             WHEN v.initcode IS NOT NULL AND a.initcode IS NULL THEN 'MISSING IN CS'
             WHEN a.initcode IS NOT NULL THEN 'SCHEME EXIST IN CS' END scheme_status
        FROM raw_data c
        LEFT JOIN validation_schemes v ON TRIM(c.InitiativeCode)=v.initcode
        LEFT JOIN all_schemes a ON CASE WHEN TRIM(c.InitiativeCode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(c.InitiativeCode),1,LENGTH(TRIM(c.InitiativeCode))-1) ELSE TRIM(c.InitiativeCode) END=a.initcode
        LEFT JOIN failed_schemes f ON CASE WHEN TRIM(c.InitiativeCode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(c.InitiativeCode),1,LENGTH(TRIM(c.InitiativeCode))-1) ELSE TRIM(c.InitiativeCode) END=f.initcode
        WHERE c.InitiativeCode IS NOT NULL
    ),
    agg_data AS (
        SELECT Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
        SUM(CASE WHEN date_format(shipdate,"yyyyMM")="{formatted_date}" THEN AmountDisbursed ELSE 0 END) AmountDisbursed_IHR,
        SUM(First_FortNight_IHR) Amount_Disbursed_Fortnight,
        SUM(Second_FortNight_IHR) Amount_Disbursed_Monthly,
        SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}" AND date_format(shipdate,'yyyyMM')="{formatted_date}" AND ((TRIM(UPPER(matrix_check_monthly_release))="CONSIDER" AND TRIM(UPPER(matrix_check_fortnight))="NOT CONSIDER") OR (TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)>14)) THEN Disallowance_SRN ELSE 0 END) sales_return_cm,
        SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}" AND date_format(shipdate,'yyyyMM')=date_format(add_months(to_date('{formatted_date}','yyyyMM'),-1),'yyyyMM') AND ((TRIM(UPPER(matrix_check_monthly_release))="CONSIDER" AND TRIM(UPPER(matrix_check_fortnight))="NOT CONSIDER") OR (TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)>14)) THEN Disallowance_SRN ELSE 0 END) sales_return_pm
        FROM (SELECT t7.* EXCEPT(SubChannelName),COALESCE(SubChannelName,Transaction_Customer_Type) SubChannelName,DistributorName FROM raw_data t7 LEFT JOIN return_sub_channels t6 ON t7.salesinvoiceno=t6.DocNumber LEFT JOIN dist_names t5 ON t5.DistributorCode=t7.Distributor WHERE matrix_check_fortnight="Consider" OR matrix_check_monthly_release="Consider")
        GROUP BY Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
    ),
    agg_data_final AS (
        SELECT *,sales_return_cm+sales_return_pm Disallowance_SRN FROM agg_data
    ),
    agg_data_pc AS (
        SELECT Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
        SUM(Wrong_Rate) Wrong_Rate,
        SUM(CASE WHEN date_format(shipdate,"yyyyMM")="{formatted_date}" AND LEFT(InitiativeCode,3)="LFG" THEN AmountDisbursed ELSE 0 END) Free_Goods,
        SUM(CASE WHEN Remark_Channel_Validation IS NOT NULL AND AmountDisbursed>=0 AND InitiativeCode NOT LIKE 'LTR____E%' AND LEFT(InitiativeCode,3)!="LFG" THEN AmountDisbursed ELSE 0 END) Wrong_Channel,
        SUM(CASE WHEN Date_Check="Mismatch" THEN AmountDisbursed ELSE 0 END) Wrong_Date,
        SUM(Disallowance_Settlement) settelement_check_Amount
        FROM (SELECT t7.* EXCEPT(SubChannelName),COALESCE(SubChannelName,Transaction_Customer_Type) SubChannelName,DistributorName FROM raw_data t7 LEFT JOIN return_sub_channels t6 ON t7.salesinvoiceno=t6.DocNumber LEFT JOIN dist_names t5 ON t5.DistributorCode=t7.Distributor WHERE (TRIM(UPPER(matrix_check_monthly_release))=UPPER("Consider") AND TRIM(UPPER(matrix_check_fortnight))=UPPER("Not Consider")) OR (TRIM(UPPER(matrix_check_fortnight))=UPPER("Consider") AND dayofmonth(shipdate)>14))
        GROUP BY Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
    ),
    cs_calc AS (
        SELECT t1.*,t6.Wrong_Rate,t6.Free_Goods,t6.Wrong_Channel,t6.Wrong_Date,t6.settelement_check_Amount,
        t2.type_of_brand,t4.scheme_Type,
        COALESCE(cv.scheme_status,'SCHEME EXIST IN CS') scheme_status,
        CASE WHEN cv.scheme_status IN ('CHECK FAILED IN CS','MISSING IN CS')
             THEN GREATEST(t1.Amount_Disbursed_Monthly,0) ELSE 0 END CS_NOT_APPROVED
        FROM agg_data_final t1
        LEFT JOIN agg_data_pc t6 ON t1.Distributor=t6.Distributor AND UPPER(TRIM(t1.SiteName))=UPPER(TRIM(t6.SiteName)) AND t1.InitiativeCode=t6.InitiativeCode
        LEFT JOIN type_of_brand t2 ON t1.InitiativeCode=t2.InitiativeCode
        LEFT JOIN scheme_Type t4 ON t4.scheme_Code=t1.InitiativeCode
        LEFT JOIN cs_validation cv ON TRIM(t1.InitiativeCode)=cv.InitiativeCode
    )
    SELECT Distributor,DistributorName,SiteName,InitiativeCode,scheme_Type,InitiativeName,
    AmountDisbursed_IHR,Amount_Disbursed_Fortnight,
    CASE WHEN scheme_status IN ('CHECK FAILED IN CS','MISSING IN CS') AND Amount_Disbursed_Monthly>=0 THEN 0 ELSE Amount_Disbursed_Monthly END Amount_Disbursed_Monthly,
    type_of_brand,
    CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE Disallowance_SRN END Disallowance_SRN,
    CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE sales_return_cm END sales_return_cm,
    CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE sales_return_pm END sales_return_pm,
    Wrong_Rate,Free_Goods,Wrong_Channel,Wrong_Date,settelement_check_Amount,
    CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE Disallowance_SRN END+Wrong_Rate+Free_Goods+Wrong_Channel+Wrong_Date+settelement_check_Amount Final_Disallowance,
    CS_NOT_APPROVED,
    scheme_status,'{formatted_date}' RptMonthYear
    FROM cs_calc
    ''')

    spark.sql(f'''DELETE FROM claims_mgmt.claims_mgmt_summary_monthly WHERE rptmonthyear="{formatted_date}"''')
    result.write.mode("append").option("mergeSchema","true").saveAsTable("claims_mgmt.claims_mgmt_summary_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FORTNGHTLY

# COMMAND ----------

# if end_date_value[-2:]=="14":
#     result=spark.sql(f'''
#     WITH all_schemes AS (
#         SELECT DISTINCT CASE WHEN TRIM(initcode) NOT RLIKE '_NC$' AND TRIM(initcode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(initcode),1,LENGTH(TRIM(initcode))-1) ELSE TRIM(initcode) END initcode
#         FROM stg.cs_combined WHERE rptmonthyear="{formatted_date}" AND  TO_DATE(COALESCE(TRY_TO_TIMESTAMP(end_date,'M/d/yy'),TRY_TO_TIMESTAMP(end_date,'d-MMM-yy'), to_date(end_date, 'yyyy-MM-dd HH:mm:ss'), to_date(end_date, 'yyyy/MM/dd') ))>=DATE('{start_date_value}')
#         UNION
#         SELECT DISTINCT CASE WHEN TRIM(initcode) NOT RLIKE '_NC$' AND TRIM(initcode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(initcode),1,LENGTH(TRIM(initcode))-1) ELSE TRIM(initcode) END
#         FROM claims_mgmt.dime_channel_summary WHERE rptmonthyear="{formatted_date}"  AND  TO_DATE(COALESCE(TRY_TO_TIMESTAMP(end_date,'M/d/yy'),TRY_TO_TIMESTAMP(end_date,'d-MMM-yy'), to_date(end_date, 'yyyy-MM-dd HH:mm:ss'), to_date(end_date, 'yyyy/MM/dd') ))>=DATE('{start_date_value}')
#     ),
# failed_schemes AS (
#     SELECT DISTINCT TRIM(cs.initcode) initcode FROM stg.cs_combined cs LEFT JOIN stg.masters_auditex s ON TRIM(cs.initcode)=TRIM(s.scheme_code)
#     WHERE cs.rptmonthyear="{formatted_date}" AND cs.disbursement_count!=s.Retailer_Apply_Count
#     AND TO_DATE(COALESCE(TRY_TO_TIMESTAMP(cs.end_date,'M/d/yy'),TRY_TO_TIMESTAMP(cs.end_date,'d-MMM-yy'), to_date(cs.end_date, 'yyyy-MM-dd HH:mm:ss'), to_date(cs.end_date, 'yyyy/MM/dd') ))>=DATE('{start_date_value}') AND TRIM(cs.initcode) NOT LIKE 'LFG%'
    
#     UNION
    
#     SELECT DISTINCT TRIM(dc.initcode) initcode FROM claims_mgmt.dime_channel_summary dc LEFT JOIN stg.masters_auditex s ON TRIM(dc.initcode)=TRIM(s.scheme_code)
#     WHERE dc.rptmonthyear="{formatted_date}" AND dc.disbursement_count!=s.Retailer_Apply_Count
#     AND TO_DATE(COALESCE(TRY_TO_TIMESTAMP(end_date,'M/d/yy'),TRY_TO_TIMESTAMP(end_date,'d-MMM-yy'), to_date(end_date, 'yyyy-MM-dd HH:mm:ss'), to_date(end_date, 'yyyy/MM/dd') ))>=DATE('{start_date_value}') AND TRIM(dc.initcode) NOT LIKE 'LFG%'
# ),
#     validation_schemes AS (
#         SELECT DISTINCT TRIM(scheme_code) initcode FROM stg.masters_auditex WHERE scheme_code NOT LIKE 'LFG%'
#     ),
#     cs_validation AS (
#         SELECT DISTINCT TRIM(c.InitiativeCode) InitiativeCode,
#         CASE WHEN f.initcode IS NOT NULL THEN 'CHECK FAILED IN CS'
#              WHEN v.initcode IS NOT NULL AND a.initcode IS NULL THEN 'MISSING IN CS'
#              WHEN a.initcode IS NOT NULL THEN 'SCHEME EXIST IN CS' END scheme_status
#         FROM raw_data c
#         LEFT JOIN validation_schemes v ON TRIM(c.InitiativeCode)=v.initcode
#         LEFT JOIN all_schemes a ON CASE WHEN TRIM(c.InitiativeCode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(c.InitiativeCode),1,LENGTH(TRIM(c.InitiativeCode))-1) ELSE TRIM(c.InitiativeCode) END=a.initcode
#         LEFT JOIN failed_schemes f ON CASE WHEN TRIM(c.InitiativeCode) RLIKE '^LTR....N.*[A-Z]$' THEN SUBSTRING(TRIM(c.InitiativeCode),1,LENGTH(TRIM(c.InitiativeCode))-1) ELSE TRIM(c.InitiativeCode) END=f.initcode
#         WHERE c.InitiativeCode IS NOT NULL
#     ),
#     agg_data AS (
#         SELECT Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
#         SUM(CASE WHEN date_format(shipdate,"yyyyMM")="{formatted_date}" THEN AmountDisbursed ELSE 0 END) AmountDisbursed_IHR,
#         SUM(First_FortNight_IHR) Amount_Disbursed_Fortnight,
#         SUM(Second_FortNight_IHR) Amount_Disbursed_Monthly,
#         -- SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}" AND date_format(shipdate,'yyyyMM')="{formatted_date}" AND ((TRIM(UPPER(matrix_check_monthly_release))="CONSIDER" AND TRIM(UPPER(matrix_check_fortnight))="NOT CONSIDER") OR (TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)>14)) THEN Disallowance_SRN ELSE 0 END) sales_return_cm,
#         -- SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}" AND date_format(shipdate,'yyyyMM')=date_format(add_months(to_date('{formatted_date}','yyyyMM'),-1),'yyyyMM') AND ((TRIM(UPPER(matrix_check_monthly_release))="CONSIDER" AND TRIM(UPPER(matrix_check_fortnight))="NOT CONSIDER") OR (TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)>14)) THEN Disallowance_SRN ELSE 0 END) sales_return_pm,
#         SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}" AND date_format(shipdate,'yyyyMM')="{formatted_date}" AND TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)<=14 THEN Disallowance_SRN ELSE 0 END) sales_return_cm,
#         SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}" AND date_format(shipdate,'yyyyMM')=date_format(add_months(to_date('{formatted_date}','yyyyMM'),-1),'yyyyMM') AND TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)<=14 THEN Disallowance_SRN ELSE 0 END) sales_return_pm,
#         SUM(Wrong_Rate) Wrong_Rate,
#         SUM(CASE WHEN date_format(shipdate,"yyyyMM")="{formatted_date}" AND LEFT(InitiativeCode,3)="LFG" THEN AmountDisbursed ELSE 0 END) Free_Goods,
#         SUM(CASE WHEN Remark_Channel_Validation IS NOT NULL AND AmountDisbursed>=0 AND InitiativeCode NOT LIKE 'LTR____E%' AND LEFT(InitiativeCode,3)!="LFG" THEN AmountDisbursed ELSE 0 END) Wrong_Channel,
#         SUM(CASE WHEN Date_Check="Mismatch" THEN AmountDisbursed ELSE 0 END) Wrong_Date,
#         SUM(Disallowance_Settlement) settelement_check_Amount
#         FROM (
#             SELECT t7.* EXCEPT(SubChannelName),COALESCE(SubChannelName,Transaction_Customer_Type) SubChannelName,DistributorName
#             FROM raw_data t7
#             LEFT JOIN return_sub_channels t6 ON t7.salesinvoiceno=t6.DocNumber
#             LEFT JOIN dist_names t5 ON t5.DistributorCode=t7.Distributor
#             WHERE matrix_check_fortnight="Consider" AND shipdate>="{start_date_value}" AND shipdate<="{end_date_value}"
#         )
#         GROUP BY Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
#     ),
#     final_agg AS (
#     SELECT *,sales_return_pm+sales_return_cm Disallowance_SRN FROM agg_data
# ),
# cs_calc AS (
#     SELECT t1.*,t2.type_of_brand,t4.scheme_Type,
#     COALESCE(cv.scheme_status,'SCHEME EXIST IN CS') scheme_status,
#     CASE WHEN cv.scheme_status IN ('CHECK FAILED IN CS','MISSING IN CS')
#          THEN GREATEST(Amount_Disbursed_Fortnight,0) ELSE 0 END CS_NOT_APPROVED
#     FROM final_agg t1
#     LEFT JOIN type_of_brand t2 ON t1.InitiativeCode=t2.InitiativeCode
#     LEFT JOIN scheme_Type t4 ON t4.scheme_Code=t1.InitiativeCode
#     LEFT JOIN cs_validation cv ON TRIM(t1.InitiativeCode)=cv.InitiativeCode
# )
# SELECT Distributor,DistributorName,SiteName,InitiativeCode,scheme_Type,InitiativeName,
# AmountDisbursed_IHR,
# CASE WHEN scheme_status IN ('CHECK FAILED IN CS','MISSING IN CS') AND Amount_Disbursed_Fortnight>=0 THEN 0 ELSE Amount_Disbursed_Fortnight END Amount_Disbursed_Fortnight,
# Amount_Disbursed_Monthly,type_of_brand,
# -- CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE Disallowance_SRN END Disallowance_SRN,
# CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE sales_return_cm END sales_return_cm,
# CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE sales_return_pm END sales_return_pm,
# Wrong_Rate,Free_Goods,Wrong_Channel,Wrong_Date,settelement_check_Amount,
# Wrong_Rate-Free_Goods+Wrong_Channel+Wrong_Date+settelement_check_Amount+sales_return_cm+sales_return_pm Final_Disallowance,
# CS_NOT_APPROVED,
# scheme_status,"{formatted_date}" RptMonthYear
# FROM cs_calc
#     ''')

# spark.sql(f'''DELETE FROM claims_mgmt.claims_mgmt_summary_fortnight WHERE rptmonthyear="{formatted_date}"''')
# result.write.mode("append").option("mergeSchema","true").saveAsTable("claims_mgmt.claims_mgmt_summary_fortnight")

# COMMAND ----------

if end_date_value[-2:] == "14":
    result = spark.sql(f'''
    WITH all_schemes AS (
        SELECT DISTINCT CASE WHEN TRIM(initcode) NOT RLIKE '_NC$' AND TRIM(initcode) RLIKE '^LTR....N.*[A-Z]$'
            THEN SUBSTRING(TRIM(initcode),1,LENGTH(TRIM(initcode))-1) ELSE TRIM(initcode) END initcode
        FROM stg.cs_combined
        WHERE rptmonthyear="{formatted_date}"
          AND TO_DATE(COALESCE(TRY_TO_TIMESTAMP(end_date,'M/d/yy'),TRY_TO_TIMESTAMP(end_date,'d-MMM-yy'),
              TO_DATE(end_date,'yyyy-MM-dd HH:mm:ss'),TO_DATE(end_date,'yyyy/MM/dd'))) >= DATE('{start_date_value}')

        UNION

        SELECT DISTINCT CASE WHEN TRIM(initcode) NOT RLIKE '_NC$' AND TRIM(initcode) RLIKE '^LTR....N.*[A-Z]$'
            THEN SUBSTRING(TRIM(initcode),1,LENGTH(TRIM(initcode))-1) ELSE TRIM(initcode) END initcode
        FROM claims_mgmt.dime_channel_summary
        WHERE rptmonthyear="{formatted_date}"
          AND TO_DATE(COALESCE(TRY_TO_TIMESTAMP(end_date,'M/d/yy'),TRY_TO_TIMESTAMP(end_date,'d-MMM-yy'),
              TO_DATE(end_date,'yyyy-MM-dd HH:mm:ss'),TO_DATE(end_date,'yyyy/MM/dd'))) >= DATE('{start_date_value}')
    ),

    failed_schemes AS (
        SELECT DISTINCT TRIM(cs.initcode) initcode
        FROM stg.cs_combined cs
        LEFT JOIN stg.masters_auditex s ON TRIM(cs.initcode)=TRIM(s.scheme_code)
        WHERE cs.rptmonthyear="{formatted_date}"
          AND cs.disbursement_count!=s.Retailer_Apply_Count
          AND TO_DATE(COALESCE(TRY_TO_TIMESTAMP(cs.end_date,'M/d/yy'),TRY_TO_TIMESTAMP(cs.end_date,'d-MMM-yy'),
              TO_DATE(cs.end_date,'yyyy-MM-dd HH:mm:ss'),TO_DATE(cs.end_date,'yyyy/MM/dd'))) >= DATE('{start_date_value}')
          AND TRIM(cs.initcode) NOT LIKE 'LFG%'

        UNION

        SELECT DISTINCT TRIM(dc.initcode) initcode
        FROM claims_mgmt.dime_channel_summary dc
        LEFT JOIN stg.masters_auditex s ON TRIM(dc.initcode)=TRIM(s.scheme_code)
        WHERE dc.rptmonthyear="{formatted_date}"
          AND dc.disbursement_count!=s.Retailer_Apply_Count
          AND TO_DATE(COALESCE(TRY_TO_TIMESTAMP(dc.end_date,'M/d/yy'),TRY_TO_TIMESTAMP(dc.end_date,'d-MMM-yy'),
              TO_DATE(dc.end_date,'yyyy-MM-dd HH:mm:ss'),TO_DATE(dc.end_date,'yyyy/MM/dd'))) >= DATE('{start_date_value}')
          AND TRIM(dc.initcode) NOT LIKE 'LFG%'
    ),

    validation_schemes AS (
        SELECT DISTINCT TRIM(scheme_code) initcode
        FROM stg.masters_auditex
        WHERE scheme_code NOT LIKE 'LFG%'
    ),

    cs_validation AS (
        SELECT DISTINCT TRIM(c.InitiativeCode) InitiativeCode,
            CASE WHEN f.initcode IS NOT NULL THEN 'CHECK FAILED IN CS'
                 WHEN v.initcode IS NOT NULL AND a.initcode IS NULL THEN 'MISSING IN CS'
                 WHEN a.initcode IS NOT NULL THEN 'SCHEME EXIST IN CS' END scheme_status
        FROM raw_data c
        LEFT JOIN validation_schemes v ON TRIM(c.InitiativeCode)=v.initcode
        LEFT JOIN all_schemes a ON CASE WHEN TRIM(c.InitiativeCode) RLIKE '^LTR....N.*[A-Z]$'
            THEN SUBSTRING(TRIM(c.InitiativeCode),1,LENGTH(TRIM(c.InitiativeCode))-1) ELSE TRIM(c.InitiativeCode) END=a.initcode
        LEFT JOIN failed_schemes f ON CASE WHEN TRIM(c.InitiativeCode) RLIKE '^LTR....N.*[A-Z]$'
            THEN SUBSTRING(TRIM(c.InitiativeCode),1,LENGTH(TRIM(c.InitiativeCode))-1) ELSE TRIM(c.InitiativeCode) END=f.initcode
        WHERE c.InitiativeCode IS NOT NULL
    ),

    agg_data AS (
        SELECT Distributor,DistributorName,SiteName,InitiativeCode,InitiativeName,
            SUM(CASE WHEN date_format(shipdate,"yyyyMM")="{formatted_date}" THEN AmountDisbursed ELSE 0 END) AmountDisbursed_IHR,
            SUM(First_FortNight_IHR) Amount_Disbursed_Fortnight,
            SUM(Second_FortNight_IHR) Amount_Disbursed_Monthly,
            SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}"
                AND date_format(shipdate,'yyyyMM')="{formatted_date}"
                AND TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)<=14
                THEN Disallowance_SRN ELSE 0 END) sales_return_cm,
            SUM(CASE WHEN date_format(srn_shipdate,'yyyyMM')="{formatted_date}"
                AND date_format(shipdate,'yyyyMM')=date_format(add_months(to_date('{formatted_date}','yyyyMM'),-1),'yyyyMM')
                AND TRIM(UPPER(matrix_check_fortnight))="CONSIDER" AND dayofmonth(shipdate)<=14
                THEN Disallowance_SRN ELSE 0 END) sales_return_pm,
            SUM(Wrong_Rate) Wrong_Rate,
            SUM(CASE WHEN date_format(shipdate,"yyyyMM")="{formatted_date}" AND LEFT(InitiativeCode,3)="LFG"
                THEN AmountDisbursed ELSE 0 END) Free_Goods,
            SUM(CASE WHEN Remark_Channel_Validation IS NOT NULL AND AmountDisbursed>=0
                AND InitiativeCode NOT LIKE 'LTR____E%' AND LEFT(InitiativeCode,3)!="LFG"
                THEN AmountDisbursed ELSE 0 END) Wrong_Channel,
            SUM(CASE WHEN Date_Check="Mismatch" THEN AmountDisbursed ELSE 0 END) Wrong_Date,
            SUM(Disallowance_Settlement) settelement_check_Amount
        FROM (
            SELECT t7.* EXCEPT(SubChannelName),COALESCE(SubChannelName,Transaction_Customer_Type) SubChannelName,DistributorName
            FROM raw_data t7
            LEFT JOIN return_sub_channels t6 ON t7.salesinvoiceno=t6.DocNumber
            LEFT JOIN dist_names t5 ON t5.DistributorCode=t7.Distributor
            WHERE matrix_check_fortnight="Consider" AND shipdate>="{start_date_value}" AND shipdate<="{end_date_value}"
        )
        GROUP BY Distributor,DistributorName,InitiativeCode,InitiativeName,SiteName
    ),

    final_agg AS (
        SELECT *,sales_return_pm+sales_return_cm Disallowance_SRN
        FROM agg_data
    ),

    cs_calc AS (
        SELECT t1.*,t2.type_of_brand,t4.scheme_Type,
            COALESCE(cv.scheme_status,'SCHEME EXIST IN CS') scheme_status,
            CASE WHEN cv.scheme_status IN ('CHECK FAILED IN CS','MISSING IN CS')
                THEN GREATEST(Amount_Disbursed_Fortnight,0) ELSE 0 END CS_NOT_APPROVED
        FROM final_agg t1
        LEFT JOIN type_of_brand t2 ON t1.InitiativeCode=t2.InitiativeCode
        LEFT JOIN scheme_Type t4 ON t4.scheme_Code=t1.InitiativeCode
        LEFT JOIN cs_validation cv ON TRIM(t1.InitiativeCode)=cv.InitiativeCode
    )

    SELECT Distributor,DistributorName,SiteName,InitiativeCode,scheme_Type,InitiativeName,
        AmountDisbursed_IHR,
        CASE WHEN scheme_status IN ('CHECK FAILED IN CS','MISSING IN CS') AND Amount_Disbursed_Fortnight>=0
            THEN 0 ELSE Amount_Disbursed_Fortnight END Amount_Disbursed_Fortnight,
        Amount_Disbursed_Monthly,type_of_brand,
        Wrong_Rate,Free_Goods,Wrong_Channel,settelement_check_Amount,Wrong_Date,
        CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE sales_return_cm END sales_return_cm,
        CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE sales_return_pm END sales_return_pm,
        Wrong_Rate+Free_Goods+Wrong_Channel+settelement_check_Amount+Wrong_Date+
        CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE sales_return_cm END+
        CASE WHEN CS_NOT_APPROVED>0 THEN 0 ELSE sales_return_pm END Final_Disallowance,
        "{formatted_date}" RptMonthYear,
        CS_NOT_APPROVED,
        scheme_status
    FROM cs_calc
    ''')

    spark.sql(f'''DELETE FROM claims_mgmt.claims_mgmt_summary_fortnight WHERE rptmonthyear="{formatted_date}"''')

    result.write.mode("append").option("mergeSchema","true").saveAsTable(
        "claims_mgmt.claims_mgmt_summary_fortnight"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SHARE BELOW DF FOR SCHEME WISE SUMMARY

# COMMAND ----------

if end_date_value[-2:]=="14":
    spark.sql(f'''
        SELECT Distributor,DistributorName,SiteName,InitiativeCode,scheme_Type,InitiativeName,
               AmountDisbursed_IHR,Amount_Disbursed_Fortnight,Amount_Disbursed_Monthly,type_of_brand,
               Wrong_Rate,Free_Goods,Wrong_Channel,settelement_check_Amount,Wrong_Date,
               sales_return_cm,sales_return_pm,Final_Disallowance,RptMonthYear,
               CS_NOT_APPROVED,scheme_status
        FROM claims_mgmt.claims_mgmt_summary_fortnight
        WHERE RptMonthYear="{formatted_date}"
    ''').display()

else:
    spark.sql(f'''
        SELECT Distributor,DistributorName,SiteName,InitiativeCode,scheme_Type,InitiativeName,
               AmountDisbursed_IHR,Amount_Disbursed_Fortnight,Amount_Disbursed_Monthly,type_of_brand,
               Wrong_Rate,Free_Goods,Wrong_Channel,settelement_check_Amount,Wrong_Date,
               sales_return_cm,sales_return_pm,Final_Disallowance,RptMonthYear,
               CS_NOT_APPROVED,scheme_status
        FROM claims_mgmt.claims_mgmt_summary_monthly
        WHERE RptMonthYear="{formatted_date}"
    ''').display()

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select scheme_status,sum(AmountDisbursed_IHR) from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear ='202607' 
# MAGIC and AmountDisbursed_IHR>0
# MAGIC group by scheme_status

# COMMAND ----------

# %sql

# ALTER TABLE claims_mgmt.claims_mgmt_summary_fortnight
# SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

# COMMAND ----------

# %sql

# ALTER TABLE claims_mgmt.claims_mgmt_summary_fortnight
# DROP COLUMN CS_NOT_APPROVED;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct InitiativeCode,Remarks_Fortnight,matrix_check_fortnight,matrix_check_monthly_release from claims_mgmt.claims_mgmt_raw_report where InitiativeCode 
# MAGIC -- = 'LSS2606LTR00003194'
# MAGIC -- in(
# MAGIC -- 'LTR2605N00004389'
# MAGIC IN (
# MAGIC     'LSS2606LTR00003194',
# MAGIC     'LSS2605N3264_BC3_NC',
# MAGIC     'LSS2605N3262_BC3_NC',
# MAGIC     'LTR2605N00004650',
# MAGIC     'LSS2605N3268_BC3_NC',
# MAGIC     'LTT2511N00000133',
# MAGIC     'LSS2605N3267_BC3_NC',
# MAGIC     'LTR2603N00000589',
# MAGIC     'LSS2605N3258_BC3',
# MAGIC     'LSS2605N3254_BC3',
# MAGIC     'LSS2605N3266_BC3',
# MAGIC     'LTR2512N00000580',
# MAGIC     'LSS2605N3262_BC3',
# MAGIC     'LTR2512N00000604',
# MAGIC     'LSS2605N3265_BC3',
# MAGIC     'LSS2605N3260_BC3_NC',
# MAGIC     'LSS2605N3264_BC3',
# MAGIC     'LTR2512N00000583',
# MAGIC     'LTR2509N3765',
# MAGIC     'LTR2605N00003920',
# MAGIC     'LTR2605N00003926',
# MAGIC     'LSS2605N3261_BC3',
# MAGIC     'LSS2605N3263_BC3',
# MAGIC     'LSS2605N3256_BC3',
# MAGIC     'LSS2605N3255_BC3',
# MAGIC     'LSS2605N3269_BC3',
# MAGIC     'LTR2605N00003919',
# MAGIC     'LSS2605N3257_BC3',
# MAGIC     'LSS2605N3268_BC3',
# MAGIC     'LTR2605N00003918',
# MAGIC     'LTR2605N00003925',
# MAGIC     'LTR2605N00003859',
# MAGIC     'LSS2605N3260_BC3',
# MAGIC     'LTR2605N00007970',
# MAGIC     'LTR2605N00003862',
# MAGIC     'LSS2605N3267_BC3',
# MAGIC     'LTR2605N00003924',
# MAGIC     'LTR2605N00003863',
# MAGIC     'LTR2605N00003911',
# MAGIC     'LTR2605N00003912',
# MAGIC     'LTR2605N00003913',
# MAGIC     'LTR2605N00003917',
# MAGIC     'LSS2605LTR00004560',
# MAGIC     'LTR2605N00003916',
# MAGIC     'LTR2605N00003923',
# MAGIC     'LTR2605N00003922',
# MAGIC     'LTR2605N00003921',
# MAGIC     'LTR2605N00003852',
# MAGIC     'LTR2605N00003915',
# MAGIC     'LTR2605N00003853',
# MAGIC     'LTR2605N00003914',
# MAGIC     'LTR2605N00003851',
# MAGIC     'LTR2605N00003848',
# MAGIC     'LTR2605N00004389',
# MAGIC     'LTR2605N00003850',
# MAGIC     'LTR2605N00004383',
# MAGIC     'LTR2605N00004382',
# MAGIC     'LTR2605N00004388',
# MAGIC     'LTR2605N00004381'
# MAGIC )and ShipDate >= '2026-06-01'

# COMMAND ----------

