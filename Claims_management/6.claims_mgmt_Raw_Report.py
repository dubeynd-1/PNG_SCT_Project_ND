# Databricks notebook source
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
from dateutil.relativedelta import relativedelta

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
last_month_start = (datetime.strptime(month_start, "%Y-%m-%d")- relativedelta(months=1)).strftime("%Y-%m-%d")
print(formatted_date)
print(month_start)
print(last_month_start)
print(ihr_date_format)
print(start_date_value,end_date_value)

# COMMAND ----------

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

# remove srn from raw report where invoice is neagtvie and srn_ship_date should > last month start

# COMMAND ----------

# result=spark.sql(f'''
#                  with ihr_raw as (
#   select distinct * from 
#   (
#   select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
#    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where shipdate>="{start_date_value}" and shipdate<="{end_date_value}" 
#    --and  InitiativeEndDate >= '{start_date_value}'
#   union
#   select SalesInvoiceNo,ShipDate,DistCode as Distributor, retailercode, channel as Channel_Name, InitiativeCode,InitiativeStartDate,InitiativeEndDate,InitiativeName,amountdisbursed
#    from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where concat(salesinvoiceno,InitiativeCode) in (select concat(invoice_number,scheme_code) from claims_mgmt.srn_calcs where RptMonthYear="{formatted_date}") 
#    --and  InitiativeEndDate >= '{start_date_value}'
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

# case when (((i.initiativeCode is not null and cs.initcode is not null) or (i.initiativeCode is not null and sr.scheme_code is not null) or (i.initiativeCode is not null and dc.initcode is not null)) and (i.AmountDisbursed>0)) then
# case when to_date(cs.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(cs.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
# when to_date(dc.start_date,'M/d/yy')=to_date(i.InitiativeStartDate) and to_date(dc.end_date,'M/d/yy')=to_date(i.InitiativeEndDate) then "Match"
# when to_date(dc.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate) and to_date(cs.end_date,'M/d/yy')!=to_date(i.InitiativeEndDate)
# then case when p.InvDate<=date_add(to_date(sr.Required_Valid_To,"yyyyMMdd"),2) then "Match" else "Mismatch" end
# end else null end as Date_Check,

#   -- FIX 1: exclude Laundry/Multi Brand scheme codes at calculation time so First_FortNight_IHR
#   -- can never carry an amount for a scheme that will later be marked "Not Consider" via ls join
#   case when trim(upper(matrix_check_fortnight))=upper("Consider") and date_format(i.shipDate,'yyyyMM')='{formatted_date}' and dayofmonth(i.shipdate) <=14
#        and not exists (select 1 from claims_mgmt.laundry_multi_brand_schemes ls_chk where ls_chk.rptmonthyear="{formatted_date}" and ls_chk.INITCode=i.InitiativeCode)
#   then i.amountdisbursed  else null end as First_FortNight_IHR,

#   -- FIX 2: laundry/multi-brand schemes always land here (outer query forces their
#   -- matrix_check_monthly_release to "Consider"), regardless of ship day or raw vs flags,
#   -- so they don't fall into the gap between First_FortNight_IHR and Second_FortNight_IHR
#   case when (
#        (trim(upper(vs.matrix_check_monthly_release))=upper("Consider") and trim(upper(matrix_check_fortnight))=upper("Not Consider"))
#     or (trim(upper(matrix_check_fortnight))=upper("Consider") and dayofmonth(i.shipdate) >14)
#     or exists (select 1 from claims_mgmt.laundry_multi_brand_schemes ls_chk2 where ls_chk2.rptmonthyear="{formatted_date}" and ls_chk2.INITCode=i.InitiativeCode)
#      )
#      and date_format(i.shipDate,'yyyyMM')='{formatted_date}' 
#   then i.amountdisbursed  else null end as Second_FortNight_IHR, 

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


#   --case when i.shipdate>date_add(to_date(sr.Required_Valid_To,"yyyyMMdd"),2) and i.AmountDisbursed>0 then 1 else 0 end as settelement_check,
#   case when i.shipdate>to_date(sr.Required_Valid_To,"yyyyMMdd") and i.AmountDisbursed>0 then 1 else 0 end as settelement_check,
#   -- 0 as settelement_check,
  
#   "{formatted_date}" as RptMonthYear
#   from ihr_Raw i 
#   left join vs_list vs on vs.initiativecode=i.initiativecode
#   left join psr p on i.salesinvoiceno=p.invcode and p.Retailer_Code=i.retailercode
#   left join retailer_master rm on i.retailercode=rm.rtrcode
#   left join (select salesinvoiceno, InitiativeCode, Remark from claims_mgmt.channel_lvl_check where REMARK  in ("CHANNEL/SUB-CHANNEL MISMATCH")) cv on cv.salesinvoiceno=i.salesinvoiceno and cv.InitiativeCode=i.InitiativeCode
#   left join (select * from stg.cs_combined where initcode in (select distinct InitiativeCode from ihr_raw ) and rptmonthyear = "{formatted_date}") cs on i.InitiativeCode=cs.initcode
#   left join (select distinct scheme_code from sd_science.trade_plan_dtls_mapping_V1 where scheme_code in (select initiativeCode from ihr_raw)) m on m.scheme_Code=i.InitiativeCode
#   left join (select distinct initcode, start_date, end_date from claims_mgmt.dime_channel_summary where initcode in (select initiativeCode from ihr_raw) and rptmonthyear = "{formatted_date}") dc on dc.initcode=i.InitiativeCode
#   left join (select *, amountdisbursed-Amnt_disbursed_calculated as wrong_rate from claims_mgmt.worng_rate_calcs where RptMonthYear="{formatted_date}") wr on i.InitiativeCode = wr.InitiativeCode and i.salesinvoiceno = wr.invoice_number
#   --left join claims_mgmt.srn_calcs srn on i.InitiativeCode = srn.scheme_code and i.salesinvoiceno = srn.invoice_number
#   left join srn s on s.invoice_number=i.salesinvoiceno and i.InitiativeCode=s.scheme_code
#   left join (select * from claims_mgmt.settelment_report where rptmonthyear="{formatted_date}") sr on sr.scheme_code=i.InitiativeCode
# )
# select * from ihr_base 
# where srn_shipdate is null 
# ''')
# result.display()

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
   from claims_mgmt.srn_calcs t3 
   where RptMonthYear="{formatted_date}"
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


  --case when i.shipdate>date_add(to_date(sr.Required_Valid_To,"yyyyMMdd"),2) and i.AmountDisbursed>0 then 1 else 0 end as settelement_check,
  case when i.shipdate>to_date(sr.Required_Valid_To,"yyyyMMdd") and i.AmountDisbursed>0 then 1 else 0 end as settelement_check,
  -- 0 as settelement_check,
  
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
case when (ls.initcode is not null and ls.scheme_category=="Laundry Scheme") then "Laundry Plan" when (ls.initcode is not null and ls.scheme_category=="Multi Brand Scheme") then "Multi Brand Scheme" else Scheme_Code_Consideration_Remark_Monthly end as Scheme_Code_Consideration_Remark_Monthly,m4.SiteName
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
               select * except(Disallowance_SRN,Remarks_Monthly), 
               CASE WHEN settelement_check = 1 THEN 0 WHEN ShipDate < trunc(add_months(to_date(concat('{formatted_date}', '01'), 'yyyyMMdd'), -1),'MM') THEN 0 ELSE Disallowance_SRN
                END AS Disallowance_SRN,
            --    case when settelement_check=1 then 0 else Disallowance_SRN end as Disallowance_SRN,
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

# %sql
# select sum(AmountDisbursed),sum(First_FortNight_IHR),from updt where InitiativeCode = 'LTT2605N00004652' and RptMonthYear = '202606' and matrix_check_fortnight = 'Consider'

# COMMAND ----------

# MAGIC %md
# MAGIC ### MRI CODE FIRST FORTNIGHT REMOVAL

# COMMAND ----------

if formatted_date == '202602':
    except_mri_codes = ['LTR2512N00000317_MRI','LTR2512N00000334_MRI','LTR2512N00000371_MRI','LTR2601N00003096_MRI','LTR2601N00003103_MRI','LTR2601N00003114_MRI','LTR2601N00003120_MRI','LTR2601N00003156_MRI','LTR2601N00003161_MRI','LTR2601N00003292_MRI','LTR2601N00003303_MRI','LTR2601N00003538_MRI','LTR2601N00003712_MRI','LTR2601N00003713_MRI','LTR2602N00000490_MRI','LTR2602N00000493_MRI','LTR2602N00000502_MRI','LTR2602N00000505_MRI','LTR2602N00000511_MRI','LTR2602N00000513_MRI','LTR2602N00000527_MRI','LTR2602N00000539_MRI','LTR2602N00000541_MRI','LTR2602N00000543_MRI','LTR2602N00001038_MRI','LTR2602N00001047_MRI','LTR2602N00004600_MRI','LTR2602N00004606_MRI','LTR2511N00004076_MRI','LTR2601N00003145_MRI','LTR2601N00003157_MRI','LTR2601N00003166_MRI','LTR2601N00003179_MRI','LTR2601N00003321_MRI','LTR2601N00003512_MRI','LTR2601N00003521_MRI']

    updt = updt.withColumn("First_FortNight_IHR", when(col("InitiativeCode").like("%MRI") & ~col("InitiativeCode").isin(except_mri_codes), lit(0).cast("decimal(18,6)")).otherwise(col  ("First_FortNight_IHR")))

    updt.createOrReplaceTempView("updt")

# COMMAND ----------

if formatted_date == '202602':
  from pyspark.sql import functions as F
  codes = ["LTR2602N00000011","LTR2602N00000012","LTR2602N00000021","LTR2602N00000022","LTR2602N00004612","LTR2602N00004993","LTR2602N00004994","LTR2601T00003118","LTR2602N00004990",  "LTT2602N00001067"]
  updt = (spark.table("updt").withColumn("First_FortNight_IHR",F.when(F.col("InitiativeCode").isin(codes),F.lit(0).cast("decimal(18,6)")).otherwise(F.col("First_FortNight_IHR"))))
  updt.createOrReplaceTempView("updt")

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

updt=updt.select(["SalesInvoiceNo","ShipDate","return_invoice_number","srn_shipdate","InitiativeMonthyear","Distributor","BranchCode","BranchName","SiteName","RetailerCode","RetailerName","ChannelName","SubChannelName","InitiativeCode","InitiativeName","Remark_Channel_Validation","Remark_Channel_Summary","Date_Check","AmountDisbursed",
                  coalesce("First_FortNight_IHR",lit(0)).alias("First_FortNight_IHR"),coalesce("Second_FortNight_IHR",lit(0)).alias("Second_FortNight_IHR"),coalesce("Wrong_Rate",lit(0)).alias("Wrong_Rate"),coalesce("Disallowance_SRN",lit(0)).alias("Disallowance_SRN"),"settelement_check",coalesce("Disallowance_Settlement",lit(0)).alias("Disallowance_Settlement")
                  ,"Remarks_Fortnight","Remarks_Monthly","matrix_check_fortnight","matrix_check_monthly_release","Scheme_Code_Consideration_Remark_Fortnight","Scheme_Code_Consideration_Remark_Monthly","RptMonthYear"])

spark.sql(f''' delete from claims_mgmt.claims_mgmt_raw_report where rptmonthyear='{formatted_date}' ''').display()
updt.write.mode('append').saveAsTable("claims_mgmt.claims_mgmt_raw_report")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# %sql
# select sum(AmountDisbursed), sum(First_FortNight_IHR) from claims_mgmt.claims_mgmt_raw_report 
# where InitiativeCode = 'LSS2606N0206_BC1' and ShipDate < '2026-06-07'
# group by SiteName
# union
# select sum(AmountDisbursed), sum(First_FortNight_IHR) from claims_mgmt.claims_mgmt_raw_report where InitiativeCode = 'LSS2606N0206_BC1' and RptMonthYear = '202606' and ShipDate > '2026-06-06'
# group by SiteName

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC -- sum(AmountDisbursed),sum(First_FortNight_IHR),sum(Second_FortNight_IHR)
# MAGIC * 
# MAGIC from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = '202606' 
# MAGIC and InitiativeCode = 'LTR2606N00004394'
# MAGIC -- and Scheme_Code_Consideration_Remark_Fortnight = 'Consider'
# MAGIC -- and ShipDate >= '2026-06-01' and ShipDate <= '2026-06-14'
# MAGIC and distributor= '2002340719'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC -- sum(AmountDisbursed_IHR),sum(Amount_Disbursed_Fortnight)
# MAGIC from claims_mgmt.claims_mgmt_summary_fortnight
# MAGIC where InitiativeCode = 'LMP2606N4217'
# MAGIC -- and ShipDate >= '2026-06-01' and ShipDate <= '2026-06-14' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC -- InitiativeCode,SiteName,
# MAGIC sum(AmountDisbursed),sum(First_FortNight_IHR),sum(Second_FortNight_IHR) from claims_mgmt.claims_mgmt_raw_report where RptMonthYear = '202606' 
# MAGIC and InitiativeCode = 'LTT2605N00004652'
# MAGIC and Scheme_Code_Consideration_Remark_Fortnight = 'Consider'
# MAGIC and ShipDate >= '2026-06-01' and ShipDate <= '2026-06-14'
# MAGIC -- and ShipDate >= '2026-06-15' 
# MAGIC -- and ShipDate <= '2026-06-01'
# MAGIC -- group by InitiativeCode,SiteName

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(AmountDisbursed_IHR),sum(Amount_Disbursed_Fortnight)
# MAGIC from claims_mgmt.claims_mgmt_summary_fortnight 
# MAGIC where RptMonthYear = '202606'  and
# MAGIC InitiativeCode = 'LTT2605N00004652'

# COMMAND ----------

# %sql
# select distinct ShipDate from claims_mgmt.claims_mgmt_raw_report 
# where RptMonthYear = '202606' 
# and Scheme_Code_Consideration_Remark_Fortnight = 'Consider'
# and AmountDisbursed<0

# COMMAND ----------

# %sql
# select * from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails 
# where 
# -- DocNumber = 'CGVAS-26-R004823'
# DocNumber = 'CGVAS-25-I094572'