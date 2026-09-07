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

# %sql
# UPDATE claims_mgmt.claims_mgmt_raw_report
# SET Remarks_Fortnight = 'Consider',
#     Remarks_Monthly = 'Consider',
#     matrix_check_fortnight = 'Consider',
#     matrix_check_monthly_release = 'Consider',
#     Scheme_Code_Consideration_Remark_Fortnight = 'Consider',
#     Scheme_Code_Consideration_Remark_Monthly = 'Consider'
# WHERE InitiativeCode IN ('LHG2602N4925', 'LHG2602N4924') and RptMonthYear = '202602'

# COMMAND ----------

# %sql
# select Remarks_Fortnight,Remarks_Monthly,matrix_check_fortnight,matrix_check_monthly_release,Scheme_Code_Consideration_Remark_Fortnight,Scheme_Code_Consideration_Remark_Monthly from raw_data 
# where InitiativeCode in ('LHG2602N4925','LHG2602N4924')
# -- Remark_Channel_Validation is not null and matrix_check_fortnight="Consider" 
# --and RptMonthYear = '202602'

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

# DBTITLE 1,Channel excluded in final result
if end_date_value[-2:]=="14":
    result=spark.sql(f'''
    with agg_data as
    (
        select Distributor,DistributorName, SiteName ,InitiativeCode, InitiativeName,sum(case when date_format(shipdate,"yyyyMM")="{formatted_date}" then AmountDisbursed else 0 end) as AmountDisbursed_IHR, sum(First_FortNight_IHR) as Amount_Disbursed_Fortnight,sum(Second_FortNight_IHR) as Amount_Disbursed_Monthly, sum(Disallowance_SRN) as Disallowance_SRN, sum(Wrong_Rate) as Wrong_Rate, sum(case when date_format(shipdate,"yyyyMM")=="{formatted_date}" and left(InitiativeCode,3)="LFG" then AmountDisbursed else 0 end) as Free_Goods, sum(case when Remark_Channel_Validation is not null then AmountDisbursed else 0 end) as Wrong_Channel,sum(case when Date_Check = "Mismatch" then AmountDisbursed else 0 end) as Wrong_Date,sum(Disallowance_Settlement) as settelement_check_Amount
    from 
    (select  t7.*except(SubChannelName), coalesce(SubChannelName,Transaction_Customer_Type) as SubChannelName,DistributorName
    from raw_data t7
    left join return_sub_channels t6 on t7.salesinvoiceno=t6.DocNumber
    left join dist_names t5 on t5.DistributorCode=t7.Distributor 
    where matrix_check_fortnight="Consider" and shipdate>="{start_date_value}" and shipdate<="{end_date_value}")
    group by Distributor, DistributorName,InitiativeCode, InitiativeName,SiteName
    )
    select 
    Distributor, DistributorName, SiteName,t1.InitiativeCode, t4.scheme_Type, InitiativeName, AmountDisbursed_IHR, Amount_Disbursed_Fortnight, Amount_Disbursed_Monthly, type_of_brand ,Disallowance_SRN , Wrong_Rate, Free_Goods, 
    -- Removing Disallowance_SRN from the Final disallowance calculation for fortnight, just keeping it for display purposes, will be included in monthly
    Wrong_Channel , Wrong_Date,settelement_check_Amount, Wrong_Rate-Free_Goods+Wrong_channel+Wrong_Date+settelement_check_Amount as Final_Disallowance ,'{formatted_date}' as RptMonthYear
    from agg_data t1
    left join type_of_brand t2 on t1.InitiativeCode=t2.InitiativeCode
    -- left join mapping_sub_channel t3 on t1.InitiativeCode=t3.scheme_code and t3.Distributor_code=t1.Distributor 
    left join scheme_Type t4 on t4.scheme_Code=t1.InitiativeCode
    ''')
    spark.sql(f'''delete from claims_mgmt.claims_mgmt_summary_fortnight where rptmonthyear="{formatted_date}" ''').display()

    result.write.mode("append").saveAsTable("claims_mgmt.claims_mgmt_summary_fortnight")

# COMMAND ----------

# %sql
# select SiteName,
# sum(AmountDisbursed_IHR),sum(Amount_Disbursed_Fortnight),sum(Wrong_Channel)
# -- * 
# from claims_mgmt.claims_mgmt_summary_fortnight 
# where RptMonthYear = '202602'
# and SiteName in ('Pune','Mumbai')
# group by SiteName

# COMMAND ----------

# %sql
# select 
# sum(AmountDisbursed_IHR),sum(Amount_Disbursed_Fortnight),sum(Wrong_Channel)
# -- * 
# from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear = '202602'
# -- and Wrong_Channel != 0 
# --InitiativeCode in ('LTR2602E00060016')

# COMMAND ----------

# DBTITLE 1,all performace metrics were shown at month level in this cell
# if end_date_value[-2:]!="14":
#     result=spark.sql(f'''
#     with agg_data as
#     (
#         select Distributor,DistributorName, SiteName ,InitiativeCode, InitiativeName,sum(case when date_format(shipdate,"yyyyMM")="{formatted_date}" then AmountDisbursed else 0 end) as AmountDisbursed_IHR, sum(First_FortNight_IHR) as Amount_Disbursed_Fortnight,sum(Second_FortNight_IHR) as Amount_Disbursed_Monthly, sum(Disallowance_SRN) as Disallowance_SRN,  sum(Wrong_Rate) as Wrong_Rate, sum(case when date_format(shipdate,"yyyyMM")=="{formatted_date}" and left(InitiativeCode,3)="LFG" then AmountDisbursed else 0 end) as Free_Goods, sum(case when Remark_Channel_Validation is not null then AmountDisbursed else 0 end) as Wrong_Channel, sum(Disallowance_Settlement) as settelement_check_Amount
#     from 
#     (select  t7.*except(SubChannelName), coalesce(SubChannelName,Transaction_Customer_Type) as SubChannelName,DistributorName
#     from raw_data t7
#     left join return_sub_channels t6 on t7.salesinvoiceno=t6.DocNumber
#     left join dist_names t5 on t5.DistributorCode=t7.Distributor 
#     where matrix_check_fortnight="Consider" or matrix_check_monthly_release="Consider")
#     group by Distributor, DistributorName,InitiativeCode, InitiativeName,SiteName
#     )
#  select 
#     Distributor, DistributorName, SiteName,t1.InitiativeCode, t4.scheme_Type, InitiativeName, AmountDisbursed_IHR, Amount_Disbursed_Fortnight, Amount_Disbursed_Monthly, type_of_brand ,Disallowance_SRN , Wrong_Rate, Free_Goods, Wrong_Channel , settelement_check_Amount, Disallowance_SRN+Wrong_Rate-Free_Goods+Wrong_channel+settelement_check_Amount as Final_Disallowance ,'{formatted_date}' as RptMonthYear
#     from agg_data t1
#     left join type_of_brand t2 on t1.InitiativeCode=t2.InitiativeCode
#     -- left join mapping_sub_channel t3 on t1.InitiativeCode=t3.scheme_code and t3.Distributor_code=t1.Distributor 
#     left join scheme_Type t4 on t4.scheme_Code=t1.InitiativeCode
#     ''')
#     spark.sql(f'''delete from claims_mgmt.claims_mgmt_summary_monthly where rptmonthyear="{formatted_date}" ''').display()

#     result.write.mode("append").saveAsTable("claims_mgmt.claims_mgmt_summary_monthly")

# COMMAND ----------

# all performace metrics were shown as per second fortnight level and for srn we calculate whole month in below cell

# COMMAND ----------

if end_date_value[-2:]!="14":
    result=spark.sql(f'''
    with agg_data as
    (
        select Distributor,DistributorName, SiteName ,InitiativeCode, InitiativeName,sum(case when date_format(shipdate,"yyyyMM")="{formatted_date}" then AmountDisbursed else 0 end) as AmountDisbursed_IHR, sum(First_FortNight_IHR) as Amount_Disbursed_Fortnight,sum(Second_FortNight_IHR) as Amount_Disbursed_Monthly, sum(Disallowance_SRN) as Disallowance_SRN
    from 
    (select  t7.*except(SubChannelName), coalesce(SubChannelName,Transaction_Customer_Type) as SubChannelName,DistributorName
    from raw_data t7
    left join return_sub_channels t6 on t7.salesinvoiceno=t6.DocNumber
    left join dist_names t5 on t5.DistributorCode=t7.Distributor 
    where matrix_check_fortnight="Consider" or matrix_check_monthly_release="Consider")
    group by Distributor, DistributorName,InitiativeCode, InitiativeName,SiteName
    ),
    agg_data_pc as
    (
        select Distributor,DistributorName, SiteName ,InitiativeCode, InitiativeName, sum(Wrong_Rate) as Wrong_Rate, sum(case when date_format(shipdate,"yyyyMM")=="{formatted_date}" and left(InitiativeCode,3)="LFG"  then AmountDisbursed else 0 end) as Free_Goods, sum(case when Remark_Channel_Validation is not null then AmountDisbursed else 0 end) as Wrong_Channel,sum(case when Date_Check = "Mismatch" then AmountDisbursed else 0 end) as Wrong_Date, sum(Disallowance_Settlement) as settelement_check_Amount
    from 
    (select  t7.*except(SubChannelName), coalesce(SubChannelName,Transaction_Customer_Type) as SubChannelName,DistributorName
    from raw_data t7
    left join return_sub_channels t6 on t7.salesinvoiceno=t6.DocNumber
    left join dist_names t5 on t5.DistributorCode=t7.Distributor 
    where 
    -- ((matrix_check_fortnight="Consider" and date_format(shipdate,'yyyyMM')='{formatted_date}' and dayofmonth(shipdate) >14) or matrix_check_monthly_release="Consider")
    ((trim(upper(matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(shipdate) >14))
    )
    group by Distributor, DistributorName,InitiativeCode, InitiativeName,SiteName
    )
 select 
    t1.Distributor, t1.DistributorName, t1.SiteName,t1.InitiativeCode, t4.scheme_Type, t1.InitiativeName, t1.AmountDisbursed_IHR, t1.Amount_Disbursed_Fortnight, t1.Amount_Disbursed_Monthly, type_of_brand ,t1.Disallowance_SRN , Wrong_Rate, Free_Goods, Wrong_Channel ,Wrong_Date, settelement_check_Amount, Disallowance_SRN+Wrong_Rate-Free_Goods+Wrong_channel+Wrong_Date+settelement_check_Amount as Final_Disallowance ,'{formatted_date}' as RptMonthYear
    from agg_data t1
    left join agg_data_pc t6 on t1.Distributor=t6.Distributor and upper(trim(t1.SiteName))=upper(trim(t6.SiteName)) and t1.InitiativeCode=t6.InitiativeCode
    left join type_of_brand t2 on t1.InitiativeCode=t2.InitiativeCode
    -- left join mapping_sub_channel t3 on t1.InitiativeCode=t3.scheme_code and t3.Distributor_code=t1.Distributor 
    left join scheme_Type t4 on t4.scheme_Code=t1.InitiativeCode
    ''')
    spark.sql(f'''delete from claims_mgmt.claims_mgmt_summary_monthly where rptmonthyear="{formatted_date}" ''').display()

    result.write.mode("append").option("mergeSchema","true").saveAsTable("claims_mgmt.claims_mgmt_summary_monthly")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC claims_mgmt.claims_mgmt_raw_report 
# MAGIC where RptMonthYear = '202601'
# MAGIC and InitiativeCode = 'LTR2601N00003925' 
# MAGIC -- in ('LTR2601E00060036')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg.cs_combined
# MAGIC where rptmonthyear = '202601'
# MAGIC and initcode in 
# MAGIC -- ('LTR2601E00060036')
# MAGIC ('LTR2601N00003925')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_mgmt.claims_mgmt_summary_fortnight
# MAGIC WHERE RptMonthYear = '202604'
# MAGIC -- and Wrong_Channel>0

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(AmountDisbursed_IHR),sum(Amount_Disbursed_Fortnight),sum(Amount_Disbursed_Monthly),sum(Final_Disallowance),sum(Wrong_Rate),sum(Wrong_Channel)
# MAGIC from claims_mgmt.claims_mgmt_summary_monthly
# MAGIC -- where InitiativeCode = 'LTT2512N00000482'
# MAGIC WHERE RptMonthYear = '202604'
# MAGIC -- and InitiativeCode like '%MRI'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(AmountDisbursed_IHR),sum(Amount_Disbursed_Fortnight),sum(Amount_Disbursed_Monthly),sum(Final_Disallowance),sum(Wrong_Rate)
# MAGIC from claims_mgmt.claims_mgmt_summary_monthly
# MAGIC -- where InitiativeCode = 'LTT2512N00000482'
# MAGIC WHERE RptMonthYear = '202601'
# MAGIC and InitiativeCode like '%MRI'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct * from stg.cs_combined where initcode = 'LSS2601N6024_TC6'
# MAGIC select distinct * from claims_mgmt.dime_channel_summary where initcode = 'LSS2601N6024_TC6'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from claims_mgmt.claims_mgmt_summary_monthly c
# MAGIC left anti join  
# MAGIC (select distinct initcode from stg.cs_combined where initcode in (select initiativeCode from claims_mgmt.claims_mgmt_summary_monthly) and rptmonthyear = "202601") cs
# MAGIC ON c.InitiativeCode = cs.initcode
# MAGIC left anti join (select distinct initcode from claims_mgmt.dime_channel_summary where initcode in (select initiativeCode from claims_mgmt.claims_mgmt_summary_monthly) and rptmonthyear = "202601") dc 
# MAGIC on dc.initcode=c.InitiativeCode
# MAGIC -- on m.scheme_Code=c.InitiativeCode
# MAGIC where RptMonthYear = '202601' 
# MAGIC and InitiativeCode LIKE '%2601%'
# MAGIC -- and 
# MAGIC --and InitiativeCode = 'LTT2601N00003506'

# COMMAND ----------

spark.sql(f"""
select * 
from claims_mgmt.claims_mgmt_summary_monthly 
where RptMonthYear = '202601' 
-- where RptMonthYear = '{formatted_date}' 
""").display()

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# display(spark.sql(f"""select sum(AmountDisbursed_IHR),sum(Amount_Disbursed_Fortnight) from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear=202602 and initiativecode in ('LTR2602N00000011','LTR2602N00000012','LTR2602N00000021','LTR2602N00000022','LTR2602N00004612','LTR2602N00004993','LTR2602N00004994')"""))

# COMMAND ----------

display(spark.sql(f"""select * from claims_mgmt.claims_mgmt_summary_fortnight where RptMonthYear={formatted_date}"""))

# COMMAND ----------

# %sql
# drop table claims_mgmt.claims_mgmt_report_at_distributorXscheme_code_fortnight

# COMMAND ----------

# MAGIC %sql
# MAGIC select Distributor,DistributorName, SiteName ,InitiativeCode, InitiativeName,sum(case when date_format(shipdate,"yyyyMM")="{formatted_date}" then AmountDisbursed else 0 end) as AmountDisbursed_IHR, sum(First_FortNight_IHR) as Amount_Disbursed_Fortnight,sum(Second_FortNight_IHR) as Amount_Disbursed_Monthly, sum(Disallowance_SRN)  as Disallowance_SRN, sum(Wrong_Rate) as Wrong_Rate, sum(case when date_format(shipdate,"yyyyMM")=="{formatted_date}" and left(InitiativeCode,3)="LFG" then AmountDisbursed else 0 end) as Free_Goods, sum(case when Remark_Channel_Validation is not null then AmountDisbursed else 0 end) as Wrong_Channel,sum(Disallowance_Settlement) as settelement_check_Amount
# MAGIC     from 
# MAGIC     (select  t7.*except(SubChannelName), coalesce(SubChannelName,Transaction_Customer_Type) as SubChannelName,DistributorName
# MAGIC     from raw_data t7
# MAGIC     left join return_sub_channels t6 on t7.salesinvoiceno=t6.DocNumber
# MAGIC     left join dist_names t5 on t5.DistributorCode=t7.Distributor 
# MAGIC     where matrix_check_fortnight="Consider" and sitename="Punjab")
# MAGIC     group by Distributor, DistributorName,InitiativeCode, InitiativeName,SiteName