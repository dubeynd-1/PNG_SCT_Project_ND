# Databricks notebook source
#%skip
spark.conf.set("spark.sql.adaptive.enabled","true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled","true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitions","300")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",str(256*1024*1024))
spark.conf.set("spark.sql.adaptive.skewJoin.enabled","true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled","true")
spark.conf.set("spark.sql.shuffle.partitions","auto")
spark.conf.set("spark.sql.mapSideCombine","true")
spark.conf.set("spark.sql.execution.useObjectHashAggregareExec","true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold","10737418420")
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.io.compresssion.codec","lz4")
spark.conf.set("spark.shuffle.consolidatedFiles","true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor","3")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",str(512*1024*1024))
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")

# COMMAND ----------

#%skip
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

#%skip
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

#%skip
spark.sql(f'''select initiativecode,`description_used_y/n` as desc_flg, what_is_used as desc,matrix_check_fortnight,matrix_check_monthly_release,scheme_code_consideration_remark_1,scheme_code_consideration_remark_2 from stg.scheme_master_conf''').createOrReplaceTempView("smt")

### filter schemes valid for this fortnight/month
spark.sql(f'''
          select *except(rn) from (select *,  row_number() over(partition by initiativecode order by case when upper(trim(matrix_check_fortnight))=upper("Not Consider") then 1 else 0 end desc) as rn from (
          select /*+ Broadcast(t2) */
 distinct t1.initiativecode,matrix_check_fortnight,scheme_code_consideration_remark_1,matrix_check_monthly_release,scheme_code_consideration_remark_2,desc_flg,desc
  from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr t1 join smt t2 on t1.InitiativeCode like concat(t2.initiativecode,"%") where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and (t2.desc_flg="No" OR (t2.desc_flg='Yes' and locate(upper(t2.desc),(upper(t1.initiativename)))>0 ))
) )where rn=1
          ''').createOrReplaceTempView("vs_list")


# COMMAND ----------

# %sql
# select * from vs_list where initiativecode in ('LHG2602N4925','LHG2602N4924','LTR2512N00000317_MRI','LTR2512N00000334_MRI','LTR2512N00000371_MRI','LTR2601N00003096_MRI','LTR2601N00003103_MRI','LTR2601N00003114_MRI','LTR2601N00003120_MRI','LTR2601N00003156_MRI','LTR2601N00003161_MRI','LTR2601N00003292_MRI','LTR2601N00003303_MRI','LTR2601N00003538_MRI','LTR2601N00003712_MRI','LTR2601N00003713_MRI','LTR2602N00000490_MRI','LTR2602N00000493_MRI','LTR2602N00000502_MRI','LTR2602N00000505_MRI','LTR2602N00000511_MRI','LTR2602N00000513_MRI','LTR2602N00000527_MRI','LTR2602N00000539_MRI','LTR2602N00000541_MRI','LTR2602N00000543_MRI','LTR2602N00001038_MRI','LTR2602N00001047_MRI','LTR2602N00004600_MRI','LTR2602N00004606_MRI','LTR2511N00004076_MRI')

# COMMAND ----------

#%skip

# result=spark.sql(f'''
#                  with ihr_raw as (
#   select distinct * from 
#   (
#   select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
#    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where shipdate>="{start_date_value}" and shipdate<="{end_date_value}"
#   union
#   select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
#    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where concat(salesinvoiceno,InitiativeCode) in (select concat(invoice_number,scheme_code) from claims_mgmt.srn_calcs where RptMonthYear="{formatted_date}")
#   )
# ),
# psr as(
#   select distinct invcode,InvDate,Retailer_Code,Channel_Org ,Transaction_Customer_Type from cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails where invcode in (select salesinvoiceno from ihr_raw)
# ),
# srn as (
#    select distinct invoice_number,return_invoice_number,t3.scheme_code,t3.srn_date,disallowance 
#    from claims_mgmt.srn_calcs t3 where RptMonthYear="{formatted_date}"
# ),
# retailer_master as (
#   select *except(rn) from (select distinct RtrCode,RtrName,BranchCode, BranchName,row_number() over(partition by RtrCode order by BranchCode) as rn from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_retailermaster where RptMonthYear="{formatted_date}") where rn=1
# ),
# ihr_base as 
# (
#   select /*+ Broadcast(vs) */
#     i.SalesInvoiceNo,i.ShipDate,s.return_invoice_number, s.srn_date as srn_shipdate, substring(i.initiativename,1,6) as InitiativeMonthyear ,i.Distributor, rm.BranchCode, rm.BranchName, rm.RtrCode as RetailerCode, rm.Rtrname as RetailerName, coalesce(i.Channel_Name,p.channel_org) as ChannelName,p.Transaction_Customer_Type as SubChannelName, i.InitiativeCode, i.InitiativeName, 
#   cv.REMARK as Remark_Channel_Validation,
#   case when (i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and m.scheme_Code is not null) or (i.initiativeCode is not null and dc.initcode is not null) then "Present in CS / Mapping" else "Not Present In CS / Mapping" end as Remark_Channel_Summary, i.AmountDisbursed,

# case when (((i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and sr.scheme_code is not null)) and (i.AmountDisbursed>0)) then
# case when to_date(cs.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(cs.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
# when to_date(cs.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(cs.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate)
# then case when p.InvDate<=date_add(to_date(sr.Required_Valid_To,"yyyyMMdd"),2) then "Match" else "Mismatch" end
# end else null end as Date_Check,

#   case when trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' and dayofmonth(i.shipdate) <=14 then i.amountdisbursed  else null end as First_FortNight_IHR,

#   case when ((trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then i.amountdisbursed  else null end as  Second_FortNight_IHR, 

#   coalesce(wr.Wrong_Rate,0) as Wrong_Rate, 
#   coalesce(round(s.disallowance,2),0) as Disallowance_SRN,  

#   case when upper(trim(vs.matrix_check_fortnight))=upper("Not Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then vs.scheme_code_consideration_remark_1
#   when i.amountdisbursed<0  and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Negative Claim" 
#   when coalesce(round(s.disallowance,2),0) =0 and coalesce(wr.Wrong_Rate,0)=0 and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Verified" 
#   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))=0 and trim(upper(matrix_check_fortnight))=upper("Consider")  and date_format(s.srn_date,'yyyyMM')='{formatted_date}' then "Sales return excluding partial return"
#   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))>0  and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Wrong Rate"  
#   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))>0 and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(s.srn_date,'yyyyMM')='{formatted_date}' then "Sales return excluding partial return/ Wrong_Rate" else "#N/A" end as Remarks_Fortnight,

#   case when upper(trim(vs.matrix_check_monthly_release))=upper("Not Consider")  and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then vs.scheme_code_consideration_remark_2
#   when i.amountdisbursed<0  and ((trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Negative Claim" 
  
#   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))=0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Verified" 

#   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))=0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(s.srn_date,'yyyyMM')='{formatted_date}'  then "Sales return excluding partial return"

#   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))>0  and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Wrong Rate"  
  
#   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))>0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(s.srn_date,'yyyyMM')='{formatted_date}'  then "Sales return excluding partial return/ Wrong_Rate" else "#N/A" end as Remarks_Monthly,

#   vs.matrix_check_fortnight,vs.matrix_check_monthly_release, vs.scheme_code_consideration_remark_1 as Scheme_Code_Consideration_Remark_Fortnight, vs.scheme_code_consideration_remark_2 as Scheme_Code_Consideration_Remark_Monthly,


#   --case when i.shipdate>to_date(sr.Required_Valid_To,"yyyyMMdd") and i.AmountDisbursed>0 then 1 else 0 end as settelement_check,
#   0 as settelement_check,
  
#   "{formatted_date}" as RptMonthYear
#   from ihr_Raw i 
#   left join vs_list vs on vs.initiativecode=i.initiativecode
#   left join psr p on i.salesinvoiceno=p.invcode and p.Retailer_Code=i.retailercode
#   left join retailer_master rm on i.retailercode=rm.rtrcode
#   left join (select salesinvoiceno, InitiativeCode, Remark from claims_mgmt.channel_lvl_check where REMARK  in ("CHANNEL/SUB-CHANNEL MISMATCH")) cv on cv.salesinvoiceno=i.salesinvoiceno and cv.InitiativeCode=i.InitiativeCode
#   left join (select * from stg.cs_combined where initcode in (select distinct InitiativeCode from ihr_raw ) and rptmonthyear = "{formatted_date}") cs on i.InitiativeCode=cs.initcode
#   left join (select distinct scheme_code from sd_science.trade_plan_dtls_mapping_V1 where scheme_code in (select initiativeCode from ihr_raw)) m on m.scheme_Code=i.InitiativeCode
#   left join (select distinct initcode from claims_mgmt.dime_channel_summary where initcode in (select initiativeCode from ihr_raw) and rptmonthyear = "{formatted_date}") dc on dc.initcode=i.InitiativeCode
#   left join (select *, amountdisbursed-Amnt_disbursed_calculated as wrong_rate from claims_mgmt.worng_rate_calcs where RptMonthYear="{formatted_date}") wr on i.InitiativeCode = wr.InitiativeCode and i.salesinvoiceno = wr.invoice_number
#   --left join claims_mgmt.srn_calcs srn on i.InitiativeCode = srn.scheme_code and i.salesinvoiceno = srn.invoice_number
#   left join srn s on s.invoice_number=i.salesinvoiceno and i.InitiativeCode=s.scheme_code
#   left join (select * from claims_mgmt.settelment_report where rptmonthyear="{formatted_date}") sr on sr.scheme_code=i.InitiativeCode
# )
# select distinct  i8.*except(Remarks_Fortnight, Remarks_Monthly, matrix_check_fortnight , matrix_check_monthly_release, Scheme_Code_Consideration_Remark_Fortnight, Scheme_Code_Consideration_Remark_Monthly) ,
# case when ls.initcode is not null then "Not Consider - Laundry Plan" when upper(left(i8.initiativecode,3))=="LFG" then "Free Goods" else Remarks_Fortnight end as Remarks_Fortnight,
# case when ls.initcode is not null then "Not Consider - Laundry Plan" when upper(left(i8.initiativecode,3))=="LFG" then "Free Goods" else Remarks_Monthly end as Remarks_Monthly,
# case when ls.initcode is not null then "Not Consider" else matrix_check_fortnight end as matrix_check_fortnight,
# case when ls.initcode is not null then "Not Consider" else matrix_check_monthly_release end as matrix_check_monthly_release,
# case when ls.initcode is not null then "Not Consider - Laundry Plan" else Scheme_Code_Consideration_Remark_Fortnight end as Scheme_Code_Consideration_Remark_Fortnight,
# case when ls.initcode is not null then "Not Consider - Laundry Plan" else Scheme_Code_Consideration_Remark_Monthly end as Scheme_Code_Consideration_Remark_Monthly,
#  m4.SiteName
# from ihr_base i8
# left join 
# --(select distinct Distributor_Code,PrimaryBranchCode,Site_Name as SiteName from cdl_india_data_prod.india_distributordata_refined.tblbasemstr_locationhierarchy) m4 ON i8.Distributor = m4.Distributor_Code and i8.BranchCode = m4.PrimaryBranchCode  
# (select distinct ParentBranchCode,SiteName from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy) m4 ON i8.BranchCode = m4.ParentBranchCode
# left join (select * from claims_mgmt.laundry_schemes where rptmonthyear="{formatted_date}") ls on ls.INITCode=i8.initiativecode
# ''')
# result.createOrReplaceTempView("result")

# COMMAND ----------

# %sql
# select * from stg.dist_branch_site_mapping where BranchCode = '2002291944'

# COMMAND ----------

# %sql
# select distinct branchcode,Sitename from stg.retailermastersct where 
# -- SiteName = 'Pune'
# -- SiteName = 'Mumbai'
# branchcode in ('2002291944')


# COMMAND ----------

# %sql
# select sum(amountdisbursed) from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = '202602' and matrix_check_fortnight = 'Consider'
# and BranchCode in ('2002291944')

# COMMAND ----------

# %sql
# select distinct SiteName,sum(AmountDisbursed) from claims_mgmt.claims_mgmt_raw_report where BranchCode = '2002291944' 
# and RptMonthYear = '202602' 
# -- and matrix_check_fortnight = 'Consider'
# group by  SiteName

# COMMAND ----------

# # %sql
# -- select SiteName,sum(AmountDisbursed) from claims_mgmt.claims_mgmt_raw_report where BranchCode = '2002291944' and RptMonthYear = '202602' and matrix_check_fortnight = 'Consider'
# -- group by  SiteName

# COMMAND ----------

# %sql
# select distinct r.Sitename as retailer_Site,r.BranchCode,rm.branchcode,rm.Sitename as raw_site
# from claims_mgmt.claims_mgmt_raw_report r 
# -- left join stg.retailermastersct rm on rm.branchcode = r.BranchCode
# -- where r.SiteName = 'Pune'

# COMMAND ----------

# %sql
# select distinct r.Sitename,r.BranchCode,rm.branchcode,rm.Sitename from claims_mgmt.claims_mgmt_raw_report r 
# left join stg.retailermastersct rm on rm.branchcode = r.BranchCode
# where r.SiteName = 'Mumbai'

# COMMAND ----------

# %sql
# with temp as (
# select r.Sitename,r.BranchCode,rm.branchcode,rm.Sitename from claims_mgmt.claims_mgmt_raw_report r where r.SiteName = 'Mumbai'
# left join stg.retailermastersct rm on rm.branchcode = r.BranchCode
# )
# select distinct BranchCode from temp

# COMMAND ----------

# %sql
# -- describe history cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy
# -- where operation = 'OPTIMIZE'

# COMMAND ----------

# %sql
# SELECT DISTINCT 3039 AS version, Sitename
# FROM cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy VERSION AS OF 3063
# WHERE ParentBranchCode = '2002291944'

# UNION ALL

# SELECT DISTINCT 3031, Sitename
# FROM cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy VERSION AS OF 3027
# WHERE ParentBranchCode = '2002291944'

# -- UNION ALL

# -- SELECT DISTINCT 2939, Sitename
# -- FROM cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy VERSION AS OF 2939
# -- WHERE ParentBranchCode = '2002291944';

# -- Continue down to version 3020

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table claims_mgmt.location as
# MAGIC -- select distinct ParentBranchCode,SiteName from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy version as of 3027
# MAGIC -- where ParentBranchCode = '2002291944'
# MAGIC -- select * from claims_mgmt.location
# MAGIC -- where ParentBranchCode = '2002291944'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orig ihr_raw - original

# COMMAND ----------

# DBTITLE 1,ihr_raw - original
# MAGIC %skip
# MAGIC result=spark.sql(f'''
# MAGIC                  with ihr_raw as (
# MAGIC   select distinct * from 
# MAGIC   (
# MAGIC   select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
# MAGIC    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where shipdate>="{start_date_value}" and shipdate<="{end_date_value}"
# MAGIC   union
# MAGIC   select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
# MAGIC    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where concat(salesinvoiceno,InitiativeCode) in (select concat(invoice_number,scheme_code) from claims_mgmt.srn_calcs where RptMonthYear="{formatted_date}")
# MAGIC   )
# MAGIC ),
# MAGIC psr as(
# MAGIC   select distinct invcode,InvDate,Retailer_Code,Channel_Org ,Transaction_Customer_Type from cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails where invcode in (select salesinvoiceno from ihr_raw)
# MAGIC ),
# MAGIC srn as (
# MAGIC    select distinct invoice_number,return_invoice_number,t3.scheme_code,t3.srn_date,disallowance 
# MAGIC    from claims_mgmt.srn_calcs t3 where RptMonthYear="{formatted_date}"
# MAGIC ),
# MAGIC retailer_master as (
# MAGIC   select *except(rn) from (select distinct RtrCode,RtrName,BranchCode, BranchName,row_number() over(partition by RtrCode order by BranchCode) as rn from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_retailermaster where RptMonthYear="{formatted_date}") where rn=1
# MAGIC ),
# MAGIC ihr_base as 
# MAGIC (
# MAGIC   select /*+ Broadcast(vs) */
# MAGIC     i.SalesInvoiceNo,i.ShipDate,s.return_invoice_number, s.srn_date as srn_shipdate, substring(i.initiativename,1,6) as InitiativeMonthyear ,i.Distributor, rm.BranchCode, rm.BranchName, rm.RtrCode as RetailerCode, rm.Rtrname as RetailerName, coalesce(i.Channel_Name,p.channel_org) as ChannelName,p.Transaction_Customer_Type as SubChannelName, i.InitiativeCode, i.InitiativeName, 
# MAGIC   cv.REMARK as Remark_Channel_Validation,
# MAGIC   case when (i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and m.scheme_Code is not null) or (i.initiativeCode is not null and dc.initcode is not null) then "Present in CS / Mapping" else "Not Present In CS / Mapping" end as Remark_Channel_Summary, i.AmountDisbursed,
# MAGIC
# MAGIC case when (((i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and sr.scheme_code is not null) or (i.initiativeCode is not null and dc.initcode is not null)) and (i.AmountDisbursed>0)) then
# MAGIC case when to_date(cs.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(cs.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
# MAGIC when to_date(dc.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(dc.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
# MAGIC when to_date(dc.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate) and to_date(cs.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate)
# MAGIC then case when p.InvDate<=date_add(to_date(sr.Required_Valid_To,"yyyyMMdd"),2) then "Match" else "Mismatch" end
# MAGIC end else null end as Date_Check,
# MAGIC
# MAGIC   case when trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' and dayofmonth(i.shipdate) <=14 then i.amountdisbursed  else null end as First_FortNight_IHR,
# MAGIC
# MAGIC   case when ((trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then i.amountdisbursed  else null end as  Second_FortNight_IHR, 
# MAGIC
# MAGIC   coalesce(wr.Wrong_Rate,0) as Wrong_Rate, 
# MAGIC   coalesce(round(s.disallowance,2),0) as Disallowance_SRN,  
# MAGIC
# MAGIC   case when upper(trim(vs.matrix_check_fortnight))=upper("Not Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then vs.scheme_code_consideration_remark_1
# MAGIC   when i.amountdisbursed<0  and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Negative Claim" 
# MAGIC   when coalesce(round(s.disallowance,2),0) =0 and coalesce(wr.Wrong_Rate,0)=0 and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Verified" 
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))=0 and trim(upper(matrix_check_fortnight))=upper("Consider")  and date_format(s.srn_date,'yyyyMM')='{formatted_date}' then "Sales return excluding partial return"
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))>0  and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Wrong Rate"  
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))>0 and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(s.srn_date,'yyyyMM')='{formatted_date}' then "Sales return excluding partial return/ Wrong_Rate" else "#N/A" end as Remarks_Fortnight,
# MAGIC
# MAGIC   case when upper(trim(vs.matrix_check_monthly_release))=upper("Not Consider")  and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then vs.scheme_code_consideration_remark_2
# MAGIC   when i.amountdisbursed<0  and ((trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Negative Claim" 
# MAGIC   
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))=0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Verified" 
# MAGIC
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))=0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(s.srn_date,'yyyyMM')='{formatted_date}'  then "Sales return excluding partial return"
# MAGIC
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))>0  and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Wrong Rate"  
# MAGIC   
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))>0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(s.srn_date,'yyyyMM')='{formatted_date}'  then "Sales return excluding partial return/ Wrong_Rate" else "#N/A" end as Remarks_Monthly,
# MAGIC
# MAGIC   vs.matrix_check_fortnight,vs.matrix_check_monthly_release, vs.scheme_code_consideration_remark_1 as Scheme_Code_Consideration_Remark_Fortnight, vs.scheme_code_consideration_remark_2 as Scheme_Code_Consideration_Remark_Monthly,
# MAGIC
# MAGIC
# MAGIC   --case when i.shipdate>to_date(sr.Required_Valid_To,"yyyyMMdd") and i.AmountDisbursed>0 then 1 else 0 end as settelement_check,
# MAGIC   0 as settelement_check,
# MAGIC   
# MAGIC   "{formatted_date}" as RptMonthYear
# MAGIC   from ihr_Raw i 
# MAGIC   left join vs_list vs on vs.initiativecode=i.initiativecode
# MAGIC   left join psr p on i.salesinvoiceno=p.invcode and p.Retailer_Code=i.retailercode
# MAGIC   left join retailer_master rm on i.retailercode=rm.rtrcode
# MAGIC   left join (select salesinvoiceno, InitiativeCode, Remark from claims_mgmt.channel_lvl_check where REMARK  in ("CHANNEL/SUB-CHANNEL MISMATCH")) cv on cv.salesinvoiceno=i.salesinvoiceno and cv.InitiativeCode=i.InitiativeCode
# MAGIC   left join (select * from stg.cs_combined where initcode in (select distinct InitiativeCode from ihr_raw ) and rptmonthyear = "{formatted_date}") cs on i.InitiativeCode=cs.initcode
# MAGIC   left join (select distinct scheme_code from sd_science.trade_plan_dtls_mapping_V1 where scheme_code in (select initiativeCode from ihr_raw)) m on m.scheme_Code=i.InitiativeCode
# MAGIC   left join (select distinct initcode, start_date, end_date from claims_mgmt.dime_channel_summary where initcode in (select initiativeCode from ihr_raw) and rptmonthyear = "{formatted_date}") dc on dc.initcode=i.InitiativeCode
# MAGIC   left join (select *, amountdisbursed-Amnt_disbursed_calculated as wrong_rate from claims_mgmt.worng_rate_calcs where RptMonthYear="{formatted_date}") wr on i.InitiativeCode = wr.InitiativeCode and i.salesinvoiceno = wr.invoice_number
# MAGIC   --left join claims_mgmt.srn_calcs srn on i.InitiativeCode = srn.scheme_code and i.salesinvoiceno = srn.invoice_number
# MAGIC   left join srn s on s.invoice_number=i.salesinvoiceno and i.InitiativeCode=s.scheme_code
# MAGIC   left join (select * from claims_mgmt.settelment_report where rptmonthyear="{formatted_date}") sr on sr.scheme_code=i.InitiativeCode
# MAGIC )
# MAGIC select distinct  i8.*except(Remarks_Fortnight, Remarks_Monthly, matrix_check_fortnight , matrix_check_monthly_release, Scheme_Code_Consideration_Remark_Fortnight, Scheme_Code_Consideration_Remark_Monthly) ,
# MAGIC case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Not Consider - Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Not Consider - Multi Brand Scheme" when upper(left(i8.initiativecode,3))=="LFG" then "Free Goods" else Remarks_Fortnight end as Remarks_Fortnight,
# MAGIC case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Multi Brand Scheme" when upper(left(i8.initiativecode,3))=="LFG" then "Free Goods" else Remarks_Monthly end as Remarks_Monthly,
# MAGIC -- Changing this to include laundry and multi brand schemes in monthly and not fortnight
# MAGIC case when ls.initcode is not null then "Not Consider" else matrix_check_fortnight end as matrix_check_fortnight,
# MAGIC case when ls.initcode is not null then "Consider" else matrix_check_monthly_release end as matrix_check_monthly_release,
# MAGIC case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Not Consider - Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Not Consider - Multi Brand Scheme" else Scheme_Code_Consideration_Remark_Fortnight end as Scheme_Code_Consideration_Remark_Fortnight,
# MAGIC case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Multi Brand Scheme" else Scheme_Code_Consideration_Remark_Monthly end as Scheme_Code_Consideration_Remark_Monthly,
# MAGIC  m4.SiteName
# MAGIC from ihr_base i8
# MAGIC left join 
# MAGIC --(select distinct Distributor_Code,PrimaryBranchCode,Site_Name as SiteName from cdl_india_data_prod.india_distributordata_refined.tblbasemstr_locationhierarchy) m4 ON i8.Distributor = m4.Distributor_Code and i8.BranchCode = m4.PrimaryBranchCode  
# MAGIC (select distinct ParentBranchCode,SiteName from claims_mgmt.location) m4 ON i8.BranchCode = m4.ParentBranchCode
# MAGIC left join (select * from claims_mgmt.laundry_multi_brand_schemes where rptmonthyear="{formatted_date}") ls on ls.INITCode=i8.initiativecode
# MAGIC ''')
# MAGIC result.createOrReplaceTempView("result")

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC # %sql
# MAGIC # select * from result where Date_Check='Mismatch'
# MAGIC df = spark.sql("""
# MAGIC SELECT *
# MAGIC FROM result
# MAGIC WHERE Date_Check = 'Mismatch'
# MAGIC """)
# MAGIC df.display()

# COMMAND ----------

# DBTITLE 1,ihr_raw-revmaped with first forntight
# MAGIC %skip
# MAGIC result=spark.sql(f'''
# MAGIC                  with ihr_raw as (
# MAGIC   select distinct * from 
# MAGIC   (
# MAGIC   select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
# MAGIC    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where shipdate>="{start_date_value}" and shipdate<="{end_date_value}"
# MAGIC   union
# MAGIC   select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
# MAGIC    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where concat(salesinvoiceno,InitiativeCode) in (select concat(invoice_number,scheme_code) from claims_mgmt.srn_calcs where RptMonthYear="{formatted_date}")
# MAGIC   )
# MAGIC ),
# MAGIC psr as(
# MAGIC   select distinct invcode,InvDate,Retailer_Code,Channel_Org ,Transaction_Customer_Type from cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails where invcode in (select salesinvoiceno from ihr_raw)
# MAGIC ),
# MAGIC srn as (
# MAGIC    select distinct invoice_number,return_invoice_number,t3.scheme_code,t3.srn_date,disallowance 
# MAGIC    from claims_mgmt.srn_calcs t3 where RptMonthYear="{formatted_date}"
# MAGIC ),
# MAGIC retailer_master as (
# MAGIC   select *except(rn) from (select distinct RtrCode,RtrName,BranchCode, BranchName,row_number() over(partition by RtrCode order by BranchCode) as rn from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_retailermaster where RptMonthYear="{formatted_date}") where rn=1
# MAGIC ),
# MAGIC ihr_base as 
# MAGIC (
# MAGIC   select /*+ Broadcast(vs) */
# MAGIC     i.SalesInvoiceNo,i.ShipDate,s.return_invoice_number, s.srn_date as srn_shipdate, substring(i.initiativename,1,6) as InitiativeMonthyear ,i.Distributor, rm.BranchCode, rm.BranchName, rm.RtrCode as RetailerCode, rm.Rtrname as RetailerName, coalesce(i.Channel_Name,p.channel_org) as ChannelName,p.Transaction_Customer_Type as SubChannelName, i.InitiativeCode, i.InitiativeName, 
# MAGIC   cv.REMARK as Remark_Channel_Validation,
# MAGIC   case when (i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and m.scheme_Code is not null) or (i.initiativeCode is not null and dc.initcode is not null) then "Present in CS / Mapping" else "Not Present In CS / Mapping" end as Remark_Channel_Summary, i.AmountDisbursed,
# MAGIC
# MAGIC case when (((i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and sr.scheme_code is not null) or (i.initiativeCode is not null and dc.initcode is not null)) and (i.AmountDisbursed>0)) then
# MAGIC case when to_date(cs.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(cs.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
# MAGIC when to_date(dc.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(dc.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
# MAGIC when to_date(dc.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate) and to_date(cs.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate)
# MAGIC then case when p.InvDate<=date_add(to_date(sr.Required_Valid_To,"yyyyMMdd"),2) then "Match" else "Mismatch" end
# MAGIC end else null end as Date_Check,
# MAGIC
# MAGIC   -- FIX: exclude Laundry/Multi Brand scheme codes at calculation time so First_FortNight_IHR
# MAGIC   -- can never carry an amount for a scheme that will later be marked "Not Consider" via ls join
# MAGIC   case when trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' and dayofmonth(i.shipdate) <=14
# MAGIC        and not exists (select 1 from claims_mgmt.laundry_multi_brand_schemes ls_chk where ls_chk.rptmonthyear="{formatted_date}" and ls_chk.INITCode=i.InitiativeCode)
# MAGIC   then i.amountdisbursed  else null end as First_FortNight_IHR,
# MAGIC
# MAGIC   case when ((trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then i.amountdisbursed  else null end as  Second_FortNight_IHR, 
# MAGIC
# MAGIC   coalesce(wr.Wrong_Rate,0) as Wrong_Rate, 
# MAGIC   coalesce(round(s.disallowance,2),0) as Disallowance_SRN,  
# MAGIC
# MAGIC   case when upper(trim(vs.matrix_check_fortnight))=upper("Not Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then vs.scheme_code_consideration_remark_1
# MAGIC   when i.amountdisbursed<0  and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Negative Claim" 
# MAGIC   when coalesce(round(s.disallowance,2),0) =0 and coalesce(wr.Wrong_Rate,0)=0 and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Verified" 
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))=0 and trim(upper(matrix_check_fortnight))=upper("Consider")  and date_format(s.srn_date,'yyyyMM')='{formatted_date}' then "Sales return excluding partial return"
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))>0  and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Wrong Rate"  
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))>0 and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(s.srn_date,'yyyyMM')='{formatted_date}' then "Sales return excluding partial return/ Wrong_Rate" else "#N/A" end as Remarks_Fortnight,
# MAGIC
# MAGIC   case when upper(trim(vs.matrix_check_monthly_release))=upper("Not Consider")  and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then vs.scheme_code_consideration_remark_2
# MAGIC   when i.amountdisbursed<0  and ((trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Negative Claim" 
# MAGIC   
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))=0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Verified" 
# MAGIC
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))=0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(s.srn_date,'yyyyMM')='{formatted_date}'  then "Sales return excluding partial return"
# MAGIC
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))>0  and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Wrong Rate"  
# MAGIC   
# MAGIC   when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))>0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(s.srn_date,'yyyyMM')='{formatted_date}'  then "Sales return excluding partial return/ Wrong_Rate" else "#N/A" end as Remarks_Monthly,
# MAGIC
# MAGIC   vs.matrix_check_fortnight,vs.matrix_check_monthly_release, vs.scheme_code_consideration_remark_1 as Scheme_Code_Consideration_Remark_Fortnight, vs.scheme_code_consideration_remark_2 as Scheme_Code_Consideration_Remark_Monthly,
# MAGIC
# MAGIC
# MAGIC   --case when i.shipdate>to_date(sr.Required_Valid_To,"yyyyMMdd") and i.AmountDisbursed>0 then 1 else 0 end as settelement_check,
# MAGIC   0 as settelement_check,
# MAGIC   
# MAGIC   "{formatted_date}" as RptMonthYear
# MAGIC   from ihr_Raw i 
# MAGIC   left join vs_list vs on vs.initiativecode=i.initiativecode
# MAGIC   left join psr p on i.salesinvoiceno=p.invcode and p.Retailer_Code=i.retailercode
# MAGIC   left join retailer_master rm on i.retailercode=rm.rtrcode
# MAGIC   left join (select salesinvoiceno, InitiativeCode, Remark from claims_mgmt.channel_lvl_check where REMARK  in ("CHANNEL/SUB-CHANNEL MISMATCH")) cv on cv.salesinvoiceno=i.salesinvoiceno and cv.InitiativeCode=i.InitiativeCode
# MAGIC   left join (select * from stg.cs_combined where initcode in (select distinct InitiativeCode from ihr_raw ) and rptmonthyear = "{formatted_date}") cs on i.InitiativeCode=cs.initcode
# MAGIC   left join (select distinct scheme_code from sd_science.trade_plan_dtls_mapping_V1 where scheme_code in (select initiativeCode from ihr_raw)) m on m.scheme_Code=i.InitiativeCode
# MAGIC   left join (select distinct initcode, start_date, end_date from claims_mgmt.dime_channel_summary where initcode in (select initiativeCode from ihr_raw) and rptmonthyear = "{formatted_date}") dc on dc.initcode=i.InitiativeCode
# MAGIC   left join (select *, amountdisbursed-Amnt_disbursed_calculated as wrong_rate from claims_mgmt.worng_rate_calcs where RptMonthYear="{formatted_date}") wr on i.InitiativeCode = wr.InitiativeCode and i.salesinvoiceno = wr.invoice_number
# MAGIC   --left join claims_mgmt.srn_calcs srn on i.InitiativeCode = srn.scheme_code and i.salesinvoiceno = srn.invoice_number
# MAGIC   left join srn s on s.invoice_number=i.salesinvoiceno and i.InitiativeCode=s.scheme_code
# MAGIC   left join (select * from claims_mgmt.settelment_report where rptmonthyear="{formatted_date}") sr on sr.scheme_code=i.InitiativeCode
# MAGIC )
# MAGIC select distinct  i8.*except(Remarks_Fortnight, Remarks_Monthly, matrix_check_fortnight , matrix_check_monthly_release, Scheme_Code_Consideration_Remark_Fortnight, Scheme_Code_Consideration_Remark_Monthly) ,
# MAGIC case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Not Consider - Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Not Consider - Multi Brand Scheme" when upper(left(i8.initiativecode,3))=="LFG" then "Free Goods" else Remarks_Fortnight end as Remarks_Fortnight,
# MAGIC case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Multi Brand Scheme" when upper(left(i8.initiativecode,3))=="LFG" then "Free Goods" else Remarks_Monthly end as Remarks_Monthly,
# MAGIC -- Changing this to include laundry and multi brand schemes in monthly and not fortnight
# MAGIC case when ls.initcode is not null then "Not Consider" else matrix_check_fortnight end as matrix_check_fortnight,
# MAGIC case when ls.initcode is not null then "Consider" else matrix_check_monthly_release end as matrix_check_monthly_release,
# MAGIC case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Not Consider - Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Not Consider - Multi Brand Scheme" else Scheme_Code_Consideration_Remark_Fortnight end as Scheme_Code_Consideration_Remark_Fortnight,
# MAGIC case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Multi Brand Scheme" else Scheme_Code_Consideration_Remark_Monthly end as Scheme_Code_Consideration_Remark_Monthly,
# MAGIC  m4.SiteName
# MAGIC from ihr_base i8
# MAGIC left join 
# MAGIC --(select distinct Distributor_Code,PrimaryBranchCode,Site_Name as SiteName from cdl_india_data_prod.india_distributordata_refined.tblbasemstr_locationhierarchy) m4 ON i8.Distributor = m4.Distributor_Code and i8.BranchCode = m4.PrimaryBranchCode  
# MAGIC (select distinct ParentBranchCode,SiteName from claims_mgmt.location) m4 ON i8.BranchCode = m4.ParentBranchCode
# MAGIC left join (select * from claims_mgmt.laundry_multi_brand_schemes where rptmonthyear="{formatted_date}") ls on ls.INITCode=i8.initiativecode
# MAGIC ''')
# MAGIC result.createOrReplaceTempView("result")

# COMMAND ----------

result=spark.sql(f'''
                 with ihr_raw as (
  select distinct * from 
  (
  select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
   from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where shipdate>="{start_date_value}" and shipdate<="{end_date_value}" 
   --and  InitiativeEndDate >= '{start_date_value}'
  union
  select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
   from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where concat(salesinvoiceno,InitiativeCode) in (select concat(invoice_number,scheme_code) from claims_mgmt.srn_calcs where RptMonthYear="{formatted_date}") 
   --and  InitiativeEndDate >= '{start_date_value}'
  )
),
psr as(
  select distinct invcode,InvDate,Retailer_Code,Channel_Org ,Transaction_Customer_Type from cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails where invcode in (select salesinvoiceno from ihr_raw)
),
srn as (
   select distinct invoice_number,return_invoice_number,t3.scheme_code,t3.srn_date,disallowance 
   from claims_mgmt.srn_calcs t3 where RptMonthYear="{formatted_date}"
),
retailer_master as (
  select *except(rn) from (select distinct RtrCode,RtrName,BranchCode, BranchName,row_number() over(partition by RtrCode order by BranchCode) as rn from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_retailermaster where RptMonthYear="{formatted_date}") where rn=1
),
ihr_base as 
(
  select /*+ Broadcast(vs) */
    i.SalesInvoiceNo,i.ShipDate,s.return_invoice_number, s.srn_date as srn_shipdate, substring(i.initiativename,1,6) as InitiativeMonthyear ,i.Distributor, rm.BranchCode, rm.BranchName, rm.RtrCode as RetailerCode, rm.Rtrname as RetailerName, coalesce(i.Channel_Name,p.channel_org) as ChannelName,p.Transaction_Customer_Type as SubChannelName, i.InitiativeCode, i.InitiativeName, 
  cv.REMARK as Remark_Channel_Validation,
  case when (i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and m.scheme_Code is not null) or (i.initiativeCode is not null and dc.initcode is not null) then "Present in CS / Mapping" else "Not Present In CS / Mapping" end as Remark_Channel_Summary, i.AmountDisbursed,

case when (((i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and sr.scheme_code is not null) or (i.initiativeCode is not null and dc.initcode is not null)) and (i.AmountDisbursed>0)) then
case when to_date(cs.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(cs.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
when to_date(dc.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(dc.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
when to_date(dc.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate) and to_date(cs.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate)
then case when p.InvDate<=date_add(to_date(sr.Required_Valid_To,"yyyyMMdd"),2) then "Match" else "Mismatch" end
end else null end as Date_Check,

  -- FIX 1: exclude Laundry/Multi Brand scheme codes at calculation time so First_FortNight_IHR
  -- can never carry an amount for a scheme that will later be marked "Not Consider" via ls join
  case when trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' and dayofmonth(i.shipdate) <=14
       and not exists (select 1 from claims_mgmt.laundry_multi_brand_schemes ls_chk where ls_chk.rptmonthyear="{formatted_date}" and ls_chk.INITCode=i.InitiativeCode)
  then i.amountdisbursed  else null end as First_FortNight_IHR,

  -- FIX 2: laundry/multi-brand schemes always land here (outer query forces their
  -- matrix_check_monthly_release to "Consider"), regardless of ship day or raw vs flags,
  -- so they don't fall into the gap between First_FortNight_IHR and Second_FortNight_IHR
  case when (
       (trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider"))
    or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)
    or exists (select 1 from claims_mgmt.laundry_multi_brand_schemes ls_chk2 where ls_chk2.rptmonthyear="{formatted_date}" and ls_chk2.INITCode=i.InitiativeCode)
     )
     and date_format(i.shipDate,'yyyyMM')='{formatted_date}' 
  then i.amountdisbursed  else null end as Second_FortNight_IHR, 

  coalesce(wr.Wrong_Rate,0) as Wrong_Rate, 
  coalesce(round(s.disallowance,2),0) as Disallowance_SRN,  

  case when upper(trim(vs.matrix_check_fortnight))=upper("Not Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then vs.scheme_code_consideration_remark_1
  when i.amountdisbursed<0  and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Negative Claim" 
  when coalesce(round(s.disallowance,2),0) =0 and coalesce(wr.Wrong_Rate,0)=0 and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Verified" 
  when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))=0 and trim(upper(matrix_check_fortnight))=upper("Consider")  and date_format(s.srn_date,'yyyyMM')='{formatted_date}' then "Sales return excluding partial return"
  when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))>0  and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Wrong Rate"  
  when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))>0 and trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(s.srn_date,'yyyyMM')='{formatted_date}' then "Sales return excluding partial return/ Wrong_Rate" else "#N/A" end as Remarks_Fortnight,

  case when upper(trim(vs.matrix_check_monthly_release))=upper("Not Consider")  and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then vs.scheme_code_consideration_remark_2
  when i.amountdisbursed<0  and ((trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider")) or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Negative Claim" 
  
  when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))=0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Verified" 

  when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))=0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(s.srn_date,'yyyyMM')='{formatted_date}'  then "Sales return excluding partial return"

  when abs(coalesce(round(s.disallowance,2),0)) =0 and abs(coalesce(wr.Wrong_Rate,0))>0  and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(i.shipDate,'yyyyMM')='{formatted_date}' then "Wrong Rate"  
  
  when abs(coalesce(round(s.disallowance,2),0)) >0 and abs(coalesce(wr.Wrong_Rate,0))>0 and (trim(upper(vs.matrix_check_monthly_release))=upper("Consider")  or  trim(upper(matrix_check_fortnight))=upper("Consider") ) and date_format(s.srn_date,'yyyyMM')='{formatted_date}'  then "Sales return excluding partial return/ Wrong_Rate" else "#N/A" end as Remarks_Monthly,

  vs.matrix_check_fortnight,vs.matrix_check_monthly_release, vs.scheme_code_consideration_remark_1 as Scheme_Code_Consideration_Remark_Fortnight, vs.scheme_code_consideration_remark_2 as Scheme_Code_Consideration_Remark_Monthly,


  --case when i.shipdate>to_date(sr.Required_Valid_To,"yyyyMMdd") and i.AmountDisbursed>0 then 1 else 0 end as settelement_check,
  0 as settelement_check,
  
  "{formatted_date}" as RptMonthYear
  from ihr_Raw i 
  left join vs_list vs on vs.initiativecode=i.initiativecode
  left join psr p on i.salesinvoiceno=p.invcode and p.Retailer_Code=i.retailercode
  left join retailer_master rm on i.retailercode=rm.rtrcode
  left join (select salesinvoiceno, InitiativeCode, Remark from claims_mgmt.channel_lvl_check where REMARK  in ("CHANNEL/SUB-CHANNEL MISMATCH")) cv on cv.salesinvoiceno=i.salesinvoiceno and cv.InitiativeCode=i.InitiativeCode
  left join (select * from stg.cs_combined where initcode in (select distinct InitiativeCode from ihr_raw ) and rptmonthyear = "{formatted_date}") cs on i.InitiativeCode=cs.initcode
  left join (select distinct scheme_code from sd_science.trade_plan_dtls_mapping_V1 where scheme_code in (select initiativeCode from ihr_raw)) m on m.scheme_Code=i.InitiativeCode
  left join (select distinct initcode, start_date, end_date from claims_mgmt.dime_channel_summary where initcode in (select initiativeCode from ihr_raw) and rptmonthyear = "{formatted_date}") dc on dc.initcode=i.InitiativeCode
  left join (select *, amountdisbursed-Amnt_disbursed_calculated as wrong_rate from claims_mgmt.worng_rate_calcs where RptMonthYear="{formatted_date}") wr on i.InitiativeCode = wr.InitiativeCode and i.salesinvoiceno = wr.invoice_number
  --left join claims_mgmt.srn_calcs srn on i.InitiativeCode = srn.scheme_code and i.salesinvoiceno = srn.invoice_number
  left join srn s on s.invoice_number=i.salesinvoiceno and i.InitiativeCode=s.scheme_code
  left join (select * from claims_mgmt.settelment_report where rptmonthyear="{formatted_date}") sr on sr.scheme_code=i.InitiativeCode
)
select distinct  i8.*except(Remarks_Fortnight, Remarks_Monthly, matrix_check_fortnight , matrix_check_monthly_release, Scheme_Code_Consideration_Remark_Fortnight, Scheme_Code_Consideration_Remark_Monthly) ,
case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Not Consider - Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Not Consider - Multi Brand Scheme" when upper(left(i8.initiativecode,3))=="LFG" then "Free Goods" else Remarks_Fortnight end as Remarks_Fortnight,
case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Multi Brand Scheme" when upper(left(i8.initiativecode,3))=="LFG" then "Free Goods" else Remarks_Monthly end as Remarks_Monthly,
-- Changing this to include laundry and multi brand schemes in monthly and not fortnight
case when ls.initcode is not null then "Not Consider" else matrix_check_fortnight end as matrix_check_fortnight,
case when ls.initcode is not null then "Consider" else matrix_check_monthly_release end as matrix_check_monthly_release,
case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Not Consider - Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Not Consider - Multi Brand Scheme" else Scheme_Code_Consideration_Remark_Fortnight end as Scheme_Code_Consideration_Remark_Fortnight,
case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Multi Brand Scheme" else Scheme_Code_Consideration_Remark_Monthly end as Scheme_Code_Consideration_Remark_Monthly,
 m4.SiteName
from ihr_base i8
left join 
--(select distinct Distributor_Code,PrimaryBranchCode,Site_Name as SiteName from cdl_india_data_prod.india_distributordata_refined.tblbasemstr_locationhierarchy) m4 ON i8.Distributor = m4.Distributor_Code and i8.BranchCode = m4.PrimaryBranchCode  
(select distinct ParentBranchCode,SiteName from claims_mgmt.location) m4 ON i8.BranchCode = m4.ParentBranchCode
left join (select * from claims_mgmt.laundry_multi_brand_schemes where rptmonthyear="{formatted_date}") ls on ls.INITCode=i8.initiativecode
''')
result.createOrReplaceTempView("result")

# COMMAND ----------

# DBTITLE 1,update disallowance_srn to 0 for settlement scheme codes and calculate disallowance_settlement
updt=spark.sql(f'''
               with updt_srn as (
               select * except(Disallowance_SRN,Remarks_Monthly), case when settelement_check=1 then 0 else Disallowance_SRN end as Disallowance_SRN,
               case when settelement_check=1 then "Settlement check Disallowance" else Remarks_Monthly end as Remarks_Monthly
               from result 
               )
               , return_inv_exploded as (
                   select SalesInvoiceNo, InitiativeCode, AmountDisbursed, return_invoice from updt_srn lateral view explode_outer(return_invoice_number) t as return_invoice where settelement_check=1
               )
               ,settlement_calc as (
                   select *, AmountDisbursed+negative_claim as Disallowance_Settlement from 
                   (select SalesInvoiceNo,InitiativeCode, max(AmountDisbursed) as AmountDisbursed,sum(negative_claim) as negative_claim from  (
                   select t13.SalesInvoiceNo, t13.InitiativeCode ,t13.AmountDisbursed,coalesce(t12.amountdisbursed,0) as negative_claim from return_inv_exploded t13 left join  (select SalesInvoiceNo, InitiativeCode, amountdisbursed from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where shipdate>="{start_date_value}" and shipdate<="{end_date_value}") t12
                   on t13.return_invoice=t12.SalesInvoiceNo and t12.InitiativeCode=t13.InitiativeCode) group by SalesInvoiceNo,InitiativeCode)
               )
               select t22.*,t23.Disallowance_Settlement from updt_srn t22 left join settlement_calc t23 on t23.SalesInvoiceNo =t22.SalesInvoiceNo and t22.InitiativeCode=t23.InitiativeCode
               ''')
updt.createOrReplaceTempView('updt')

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPDATE BELOW SCHEMES FOR REMOVAL FROM FIRST FORTNIGHT

# COMMAND ----------

# %sql
# select sum(First_FortNight_IHR) from updt where InitiativeCode like '%MRI'

# COMMAND ----------

# MAGIC %md
# MAGIC ### MRI CODE FIRST FORTNIGHT REMOVAL

# COMMAND ----------

if formatted_date == '202602':
    except_mri_codes = ['LTR2512N00000317_MRI','LTR2512N00000334_MRI','LTR2512N00000371_MRI','LTR2601N00003096_MRI','LTR2601N00003103_MRI','LTR2601N00003114_MRI','LTR2601N00003120_MRI','LTR2601N00003156_MRI','LTR2601N00003161_MRI','LTR2601N00003292_MRI','LTR2601N00003303_MRI','LTR2601N00003538_MRI','LTR2601N00003712_MRI','LTR2601N00003713_MRI','LTR2602N00000490_MRI','LTR2602N00000493_MRI','LTR2602N00000502_MRI','LTR2602N00000505_MRI','LTR2602N00000511_MRI','LTR2602N00000513_MRI','LTR2602N00000527_MRI','LTR2602N00000539_MRI','LTR2602N00000541_MRI','LTR2602N00000543_MRI','LTR2602N00001038_MRI','LTR2602N00001047_MRI','LTR2602N00004600_MRI','LTR2602N00004606_MRI','LTR2511N00004076_MRI','LTR2601N00003145_MRI','LTR2601N00003157_MRI','LTR2601N00003166_MRI','LTR2601N00003179_MRI','LTR2601N00003321_MRI','LTR2601N00003512_MRI','LTR2601N00003521_MRI']

    updt = updt.withColumn("First_FortNight_IHR", when(col("InitiativeCode").like("%MRI") & ~col("InitiativeCode").isin(except_mri_codes), lit(0).cast("decimal(18,6)")).otherwise(col  ("First_FortNight_IHR")))

    updt.createOrReplaceTempView("updt")

# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql.functions import col, when, lit
# MAGIC updt = updt.withColumn( "First_FortNight_IHR", when(col("InitiativeCode").like("%MRI"),lit(0).cast("decimal(18,6)")).otherwise(col("First_FortNight_IHR")))
# MAGIC updt.createOrReplaceTempView("updt")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### MANUAL 10 schemes removal

# COMMAND ----------

if formatted_date == '202602':
  from pyspark.sql import functions as F
  codes = ["LTR2602N00000011","LTR2602N00000012","LTR2602N00000021","LTR2602N00000022","LTR2602N00004612","LTR2602N00004993","LTR2602N00004994","LTR2601T00003118","LTR2602N00004990",  "LTT2602N00001067"]
  updt = (spark.table("updt").withColumn("First_FortNight_IHR",F.when(F.col("InitiativeCode").isin(codes),F.lit(0).cast("decimal(18,6)")).otherwise(F.col("First_FortNight_IHR"))))
  updt.createOrReplaceTempView("updt")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Manual 50+ codes removal

# COMMAND ----------

if formatted_date == '202602':
    from pyspark.sql import functions as F
    codes = ["LSS2602N3033_BC2_L1","LSS2602N3033_BC2_L3","LSS2602N3034_BC2_L1","LSS2602N3034_BC2_L3","LSS2602N4785_BC2_L1","LSS2602N4785_BC2_L3","LSS2602N3029_BC2_L1","LSS2602N3029_BC2_L3","LSS2602N3030_BC2_L1","LSS2602N3030_BC2_L3","LSS2602N3023_BC2_L1","LSS2602N3023_BC2_L3","LSS2602N3024_BC2_L1","LSS2602N3024_BC2_L3","LSS2602N4780_BC2_L1","LSS2602N4780_BC2_L3","LSS2602N3017_BC2_L1","LSS2602N3017_BC2_L3","LSS2602N3018_BC2_L1","LSS2602N3018_BC2_L3","LSS2602N3019_BC2_L1","LSS2602N3019_BC2_L3","LSS2602N3020_BC2_L1","LSS2602N3020_BC2_L3","LSS2602N3021_BC2_L1","LSS2602N3021_BC2_L3","LSS2602N3022_BC2_L1","LSS2602N3022_BC2_L3","LSS2602N4777_BC2_L1","LSS2602N4777_BC2_L3","LSS2602N4778_BC2_L1","LSS2602N4778_BC2_L3","LSS2602N4779_BC2_L1","LSS2602N4779_BC2_L3","LSS2602N3031_BC2_L1","LSS2602N3031_BC2_L3","LSS2602N3032_BC2_L1","LSS2602N3032_BC2_L3","LSS2602N4784_BC2_L1","LSS2602N4784_BC2_L3","LSS2602N3027_BC2_L1","LSS2602N3027_BC2_L3","LSS2602N3028_BC2_L1","LSS2602N3028_BC2_L3","LSS2602N4782_BC2_L1","LSS2602N4782_BC2_L3","LSS2602N4792_BC2","LSS2602N4792_BC2_NC","LSS2602N3025_BC2_L1","LSS2602N3025_BC2_L3","LSS2602N3026_BC2_L1","LSS2602N3026_BC2_L3","LSS2602N4781_BC2_L1","LSS2602N4781_BC2_L3","LSS2602N4791_BC2"
    ]

    updt = (
        spark.table("updt")
        .withColumn(
            "First_FortNight_IHR",
            F.when(
                F.col("InitiativeCode").isin(codes),
                F.lit(0).cast("decimal(18,6)")
            ).otherwise(F.col("First_FortNight_IHR"))
        )
    )

    updt.createOrReplaceTempView("updt")

# COMMAND ----------

# consider fortnight
# ChennaiLHG2602N4925
# CoimbatoreLHG2602N4925
# HyderabadLHG2602N4924
# HyderabadLHG2602N4925
# MaduraiLHG2602N4925
# MumbaiLHG2602N4924
# MumbaiLHG2602N4925
# VijayawadaLHG2602N4925

# MumbaiLTR2512N00000317_MRI
# MumbaiLTR2512N00000334_MRI
# MumbaiLTR2512N00000371_MRI
# MumbaiLTR2601N00003096_MRI
# MumbaiLTR2601N00003103_MRI
# MumbaiLTR2601N00003114_MRI
# MumbaiLTR2601N00003120_MRI
# MumbaiLTR2601N00003156_MRI
# MumbaiLTR2601N00003161_MRI
# MumbaiLTR2601N00003292_MRI
# MumbaiLTR2601N00003303_MRI
# MumbaiLTR2601N00003538_MRI
# MumbaiLTR2601N00003712_MRI
# MumbaiLTR2601N00003713_MRI
# MumbaiLTR2602N00000490_MRI
# MumbaiLTR2602N00000493_MRI
# MumbaiLTR2602N00000502_MRI
# MumbaiLTR2602N00000505_MRI
# MumbaiLTR2602N00000511_MRI
# MumbaiLTR2602N00000513_MRI
# MumbaiLTR2602N00000527_MRI
# MumbaiLTR2602N00000539_MRI
# MumbaiLTR2602N00000541_MRI
# MumbaiLTR2602N00000543_MRI
# MumbaiLTR2602N00001038_MRI
# MumbaiLTR2602N00004600_MRI
# MumbaiLTR2602N00004606_MRI
# PuneLTR2511N00004076_MRI
# PuneLTR2512N00000317_MRI
# PuneLTR2601N00003114_MRI
# PuneLTR2601N00003120_MRI
# PuneLTR2601N00003145_MRI
# PuneLTR2601N00003157_MRI
# PuneLTR2601N00003166_MRI
# PuneLTR2601N00003179_MRI
# PuneLTR2601N00003321_MRI
# PuneLTR2601N00003512_MRI
# PuneLTR2601N00003521_MRI
# PuneLTR2601N00003538_MRI
# PuneLTR2601N00003712_MRI
# PuneLTR2601N00003713_MRI
# PuneLTR2602N00000490_MRI
# PuneLTR2602N00000493_MRI
# PuneLTR2602N00000502_MRI
# PuneLTR2602N00000505_MRI
# PuneLTR2602N00000511_MRI
# PuneLTR2602N00000513_MRI
# PuneLTR2602N00000527_MRI
# PuneLTR2602N00000539_MRI
# PuneLTR2602N00000541_MRI
# PuneLTR2602N00001047_MRI
# PuneLTR2602N00004606_MRI


# COMMAND ----------

updt=updt.select(["SalesInvoiceNo","ShipDate","return_invoice_number","srn_shipdate","InitiativeMonthyear","Distributor","BranchCode","BranchName","SiteName","RetailerCode","RetailerName","ChannelName","SubChannelName","InitiativeCode","InitiativeName","Remark_Channel_Validation","Remark_Channel_Summary","Date_Check","AmountDisbursed",
                  coalesce("First_FortNight_IHR",lit(0)).alias("First_FortNight_IHR"),coalesce("Second_FortNight_IHR",lit(0)).alias("Second_FortNight_IHR"),coalesce("Wrong_Rate",lit(0)).alias("Wrong_Rate"),coalesce("Disallowance_SRN",lit(0)).alias("Disallowance_SRN"),"settelement_check",coalesce("Disallowance_Settlement",lit(0)).alias("Disallowance_Settlement")
                  ,"Remarks_Fortnight","Remarks_Monthly","matrix_check_fortnight","matrix_check_monthly_release","Scheme_Code_Consideration_Remark_Fortnight","Scheme_Code_Consideration_Remark_Monthly","RptMonthYear"])

spark.sql(f''' delete from claims_mgmt.claims_mgmt_raw_report where rptmonthyear='{formatted_date}' ''').display()
updt.write.mode('append').saveAsTable("claims_mgmt.claims_mgmt_raw_report")

# COMMAND ----------

#%skip

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   sum(AmountDisbursed) AS total_amount_disbursed,
# MAGIC   sum(Second_FortNight_IHR) AS total_second_fortnight_ihr,
# MAGIC   
# MAGIC   -- Amount shipped within the reporting month (202601)
# MAGIC   sum(CASE WHEN date_format(ShipDate, 'yyyyMM') = '202601' THEN AmountDisbursed ELSE 0 END) AS in_month_amount,
# MAGIC   
# MAGIC   -- Amount shipped OUTSIDE the reporting month (Pulled via SRN Union in ihr_raw)
# MAGIC   sum(CASE WHEN date_format(ShipDate, 'yyyyMM') != '202601' OR ShipDate IS NULL THEN AmountDisbursed ELSE 0 END) AS out_of_month_gap
# MAGIC
# MAGIC FROM claims_mgmt.claims_mgmt_raw_report 
# MAGIC WHERE RptMonthYear = '202601' 
# MAGIC   AND matrix_check_fortnight = 'Not Consider'
# MAGIC   AND matrix_check_monthly_release = 'Consider';

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(AmountDisbursed),sum(First_FortNight_IHR),sum(Second_FortNight_IHR),sum(First_FortNight_IHR+Second_FortNight_IHR) as total 
# MAGIC from claims_mgmt.claims_mgmt_raw_report 
# MAGIC where RptMonthYear = '202601' 
# MAGIC -- and ShipDate >='2026-01-01' and ShipDate<= '2026-01-31'
# MAGIC and matrix_check_fortnight = 'Not Consider'
# MAGIC and matrix_check_monthly_release = 'Consider'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(AmountDisbursed),sum(First_FortNight_IHR),sum(Second_FortNight_IHR),sum(First_FortNight_IHR+Second_FortNight_IHR) as total 
# MAGIC from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = '202601' 
# MAGIC and matrix_check_fortnight = 'Not Consider'
# MAGIC and matrix_check_monthly_release = 'Not Consider'

# COMMAND ----------

# %sql
# select Remark_Channel_Validation, Count(*) from claims_mgmt.claims_mgmt_raw_report
# group by Remark_Channel_Validation

# COMMAND ----------

# %sql
# select *
# from claims_mgmt.claims_mgmt_raw_report
# WHERE Remark_Channel_Validation is not null
# AND RptMonthYear = '202601';

# COMMAND ----------

if formatted_date == '202602':
    spark.sql("""
        UPDATE claims_mgmt.claims_mgmt_raw_report
        SET Remark_Channel_Validation = NULL
        WHERE InitiativeCode NOT IN (
            'LSS2602N1458_BC10',
            'LTT2602N00004539'
        )
        AND Remark_Channel_Validation = 'CHANNEL/SUB-CHANNEL MISMATCH'
        AND RptMonthYear = '202602'
    """)

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC with 
# MAGIC cte as (
# MAGIC select Distributor,InitiativeCode,ShipDate,sum(AmountDisbursed) as amountdisbursed, sum(First_FortNight_IHR)
# MAGIC from claims_mgmt.claims_mgmt_raw_report 
# MAGIC where 
# MAGIC RptMonthYear = '202601' 
# MAGIC and InitiativeCode = 'LTR2601N00003562'
# MAGIC group by Distributor,InitiativeCode,ShipDate)
# MAGIC select c.*,i.amountdisbursed as ihr_amount_check from cte c
# MAGIC left join ihr_data_scheme_check i
# MAGIC on c.Distributor = i.DistCode
# MAGIC and c.InitiativeCode = i.InitiativeCode
# MAGIC and c.ShipDate = i.ShipDate

# COMMAND ----------

#%skip

dbutils.notebook.exit("success")

# COMMAND ----------

#%skip

%sql
create or replace temporary view ihr_data_scheme_check as
with cte as(
select DistCode,InitiativeCode,ShipDate,sum(amountdisbursed) as amountdisbursed 
from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where InitiativeCode = 'LTR2601N00003562' 
group by DistCode,InitiativeStartDate,InitiativeEndDate,InitiativeCode,ShipDate)
select * from cte
-- select sum(amountdisbursed) from cte where ShipDate between '2026-01-15' and '2026-01-31'

# COMMAND ----------

#%skip

%sql
select sum(AmountDisbursed),sum(First_FortNight_IHR),sum(Second_FortNight_IHR),sum(Wrong_Rate),sum(Disallowance_SRN) from claims_mgmt.claims_mgmt_raw_report 
where ShipDate >= '2026-01-15' and ShipDate <= '2026-01-31'  and
-- where ShipDate >= '2026-01-01' and ShipDate <= '2026-01-14'  and RptMonthYear = '202601' 
 RptMonthYear = '202601' 
--and Second_FortNight_IHR= 0 and First_FortNight_IHR<0
--and Remarks_Fortnight = 'Not Consider - Laundry Plan'
and InitiativeCode = 'LTR2601N00003562'
-- and InitiativeCode like '%MRI'
-- and SiteName = 'Jaipur'
-- NOT PRESENT IN CS/MAPPING



# COMMAND ----------

#%skip

%sql
select sum(Disallowance_SRN) from claims_mgmt.claims_mgmt_raw_report where ShipDate >= '2026-02-01' and ShipDate <= '2026-02-14'  and RptMonthYear = '202602' 
and InitiativeCode like '%MRI'
-- and SiteName = 'Vijayawada'
-- and Remarks_Fortnight != 'Sales return excluding partial return'
-- and Remarks_Fortnight = 'Verified'

# COMMAND ----------

#%skip

%sql
-- select distinct initcode from stg.cs_combined
select distinct initcode as initcode from claims_mgmt.dime_channel_summary

# COMMAND ----------

#%skip

%sql
select * from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where InitiativeCode like '%MRI' 
and 
ShipDate>='2026-02-01' and ShipDate<='2026-02-14'

-- ShipDate between '2026-02-01' and '2026-02-14'

# COMMAND ----------

#%skip

%sql
select * from claims_mgmt.claims_mgmt_raw_report where InitiativeCode = 'LTR2602N00004932'

# COMMAND ----------

#%skip

%sql
with cte as(
select distinct initcode as initcode from stg.cs_combined
union 
select distinct initcode as initcode from claims_mgmt.dime_channel_summary
)
select distinct InitiativeCode from claims_mgmt.claims_mgmt_raw_report where ShipDate >= '2026-02-01' and ShipDate <= '2026-02-14'  and RptMonthYear = '202602' and InitiativeCode not in (select initcode from cte)

# COMMAND ----------

#%skip

# %sql
# select distinct scheme_code from claims_mgmt.final_df where scheme_category = 'Multi Brand Scheme'

# COMMAND ----------

#%skip

# %sql
# select distinct scheme_code from claims_mgmt.final_df where scheme_category = 'Multi Brand Scheme'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg.dist_branch_site_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(amountdisbursed) as amountdisbursed_2300
# MAGIC from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr version as of 2502
# MAGIC where DistCode= '2002920967' and InitiativeCode in ('LTR2601N00003082') and ShipDate<='2026-01-14'
# MAGIC union all
# MAGIC select sum(amountdisbursed) as amountdisbursed_new
# MAGIC from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr
# MAGIC where DistCode= '2002920967' and InitiativeCode in ('LTR2601N00003082') and ShipDate<='2026-01-14'

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select sum(i.amountdisbursed) as amountdisbursed,m.SiteName as 
# MAGIC from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr i
# MAGIC left join cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy m
# MAGIC on i.branchcode = m.ParentBranchCode
# MAGIC and i.DistCode = m.DistributorCode
# MAGIC where i.InitiativeCode in ('LTR2601N00003082') and i.ShipDate<='2026-01-14'
# MAGIC group by i.DistCode,m.SiteName
# MAGIC having i.DistCode= '2002920967'
# MAGIC -- group by m.SiteName
# MAGIC )
# MAGIC select * from cte

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC --distinct ParentBranchCode,SiteName 
# MAGIC from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_locationhierarchy
# MAGIC where Primarybranchcode != ParentBranchCode
# MAGIC and ParentBranchCode = '2002921810'

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select sum(i.amountdisbursed) as amountdisbursed,m.SiteName as 
# MAGIC from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr i
# MAGIC left join stg.dist_branch_site_mapping m
# MAGIC on i.branchcode = m.BranchCode
# MAGIC and i.DistCode = m.DistributorCode
# MAGIC where i.InitiativeCode in ('LTR2601N00003082') and i.ShipDate<='2026-01-14'
# MAGIC group by m.SiteName
# MAGIC )
# MAGIC select * from cte

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC sum(i.amountdisbursed) as amountdisbursed,m.SiteName
# MAGIC from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr i
# MAGIC left join stg.dist_branch_site_mapping m
# MAGIC on i.branchcode = m.BranchCode
# MAGIC and i.DistCode = m.DistributorCode
# MAGIC where i.InitiativeCode in ('LTR2601N00003685')
# MAGIC and i.ShipDate<='2026-01-14'
# MAGIC group by m.SiteName
# MAGIC having m.SiteName = 'Pune'
# MAGIC --and InitiativeMonthyear = 'Jan-26'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC describe history claims_mgmt.claims_mgmt_raw_report

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(AmountDisbursed) from claims_mgmt.claims_mgmt_raw_report version as of 81 where InitiativeCode in ('LTR2601N00003082') and InitiativeMonthyear = 'Jan-26' and SHipdate <='2026-01-14' and Sitename = 'Bhopal'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(AmountDisbursed) from claims_mgmt.claims_mgmt_raw_report where InitiativeCode in ('LTR2601N00003685') and InitiativeMonthyear = 'Jan-26' and SHipdate <='2026-01-14' and Sitename = 'Pune'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(AmountDisbursed) from claims_mgmt.claims_mgmt_raw_report where InitiativeCode in ('LTR2601N00003685') and InitiativeMonthyear = 'Jan-26' and SHipdate <='2026-01-14'

# COMMAND ----------

#%skip

# %sql
# select sum(AmountDisbursed),sum(First_FortNight_IHR),sum(Disallowance_SRN) from claims_mgmt.claims_mgmt_raw_report where InitiativeCode in (select distinct scheme_code from claims_mgmt.final_df where scheme_category = 'Multi Brand Scheme') ---- # --= 'LTR2602N00000851'
#  and ShipDate >= '2026-02-01' and ShipDate <= '2026-02-14'

# COMMAND ----------

#%skip

# %sql
# UPDATE claims_mgmt.claims_mgmt_raw_report
# SET First_FortNight_IHR = CAST(0 AS DECIMAL(18,6)), Disallowance_SRN    = CAST(0 AS DECIMAL(18,6))
# WHERE InitiativeCode IN (
#     SELECT DISTINCT scheme_code
#     FROM claims_mgmt.final_df
#     WHERE scheme_category = 'Multi Brand Scheme'
# )
# AND ShipDate BETWEEN DATE '2026-02-01' AND DATE '2026-02-14';

# COMMAND ----------

# DBTITLE 1,duplicate check
#%skip

# %sql
# select salesinvoiceno,initiativecode,count(*)  from claims_mgmt.claims_mgmt_raw_report  group by salesinvoiceno,initiativecode having count(*)>1

# COMMAND ----------

# DBTITLE 1,To check an ihr disbursed leaked in both fortnight
#%skip

# %sql
# select * from claims_mgmt.claims_mgmt_raw_report where First_FortNight_IHR=Second_FortNight_IHR  and First_FortNight_IHR>0

# COMMAND ----------

# DBTITLE 1,Validate amount disbursed is tagged to either one of the fortnight
#%skip

# %sql
# select * from claims_mgmt.claims_mgmt_raw_report where abs(AmountDisbursed)>0 and First_FortNight_IHR=0.000000 and second_FortNight_IHR=0.000000 and shipdate like "2025-11-%" and (matrix_check_fortnight="Consider" or matrix_check_monthly_release="Consider")

# COMMAND ----------

# DBTITLE 1,check settelement codes
#%skip

# %sql
# select * from claims_mgmt.claims_mgmt_raw_report where settelement_check=1 and return_invoice_number is not null --and Second_FortNight_IHR!=Disallowance_Settlement

# COMMAND ----------

#%skip

%sql
with cte as (
select Distributor,SiteName,InitiativeCode,InitiativeName,sum(AmountDisbursed) as Total_scheme_AmountDisbursed,sum(First_FortNight_IHR) as Total_scheme_First_FortNight_IHR,Remarks_Fortnight,matrix_check_fortnight,Scheme_Code_Consideration_Remark_Fortnight from claims_mgmt.claims_mgmt_raw_report where RptMonthYear="202602" 
and ShipDate between '2026-02-01' and '2026-02-14'
group by Distributor,SiteName,InitiativeCode,InitiativeName,Remarks_Fortnight,matrix_check_fortnight,Scheme_Code_Consideration_Remark_Fortnight)
-- select count(*) from cte
select * from cte
--and initiativecode in ('LTR2602N00000011','LTR2602N00000012','LTR2602N00000021','LTR2602N00000022','LTR2602N00004612','LTR2602N00004993','LTR2602N00004994')

# COMMAND ----------

#%skip

