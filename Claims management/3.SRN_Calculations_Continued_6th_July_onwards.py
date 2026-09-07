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
from dateutil.relativedelta import relativedelta

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
date_obj_end = datetime.strptime(end_date_value, "%Y-%m-%d")
month_start = date_obj.replace(day=1).strftime("%Y-%m-%d")
formatted_date = date_obj.strftime("%Y%m")
ihr_date_format=month_start[:-2]+"%"
three_mnths_before=(date_obj_end-relativedelta(months=3)).strftime("%Y-%m-%d")
print(formatted_date)
print(month_start)
print(ihr_date_format)
print(start_date_value,end_date_value)
print(three_mnths_before)

# COMMAND ----------

Master_sdf=spark.sql(f'''select * from sd_science.trade_plan_dtls_master where Scheme_Code in (select distinct InitiativeCode from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where ShipDate like '{ihr_date_format}') and left(scheme_code,4)!="RKWI" ''')
Master_sdf=Master_sdf.withColumn("Level_Desc",explode(split(col("subfNameList"),"\\|")))
Master_sdf=Master_sdf.drop("RptMonthYear","subfNameList")
Master_sdf=Master_sdf.dropDuplicates()
Master_sdf.createOrReplaceTempView("Master_sdf")

# COMMAND ----------

# MAGIC %skip
# MAGIC prod_master=spark.sql('Select productCode,CategoryName,BrandName,BrandformName,SubbfName from cdl_india_data_prod.india_sdm_refined.productmaster')
# MAGIC prod_master.createOrReplaceTempView("prod_master")

# COMMAND ----------

psr_new=spark.sql(f'''Select DocNumber as invoice_number,case when ApplyToDocNum=="" then null else ApplyToDocNum end as return_of_invoice_number,rptMonthYear as RptMonthYear,Date as time_period_end_date,
Pcode as ProductCode,PrimaryBranchCode as Branch_code,RtrCode as retailer_code,
upper(ChnlName) as channel_name,SubbfCode,SubbfName,
Qty as item_unit_qty,GrossValue as gross_transact_amt,
null as tax_amt,
Retailing as net_transact_amt,
upper(trim(TransactionType)) as transaction_type
 from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails
 --cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails 
 where RptMonthYear='{formatted_date}'
--and upper(trim(TransactionType)) != 'DAMAGED RETURN'      Have to remove for IHR Level
''')
psr_new.createOrReplaceTempView("psr_new")
psr_modified=spark.sql('''
Select s.*,
CategoryName as Category,BrandName as Brand,BrandformName as BrandForm from psr_new s
left join cdl_india_data_prod.india_sdm_refined.productmaster p
on s.ProductCode=p.ProductCode ''')
psr_modified.createOrReplaceTempView("psr_modified")
ihr=spark.sql(f'''
Select salesInvoiceNo,ShipDate,retailercode,InitiativeCode,channel,CAST(amountdisbursed AS double) AS amountdisbursed,
branchcode from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and amountdisbursed<0 ''')
ihr.createOrReplaceTempView("ihr")

# COMMAND ----------

# DBTITLE 1,checkeing IHR
# ihr_test=spark.sql(f'''
# Select * from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and InitiativeCode in ('LTR2602N00000011','LTR2602N00000012','LTR2602N00000021','LTR2602N00000022','LTR2602N00004612','LTR2602N00004993','LTR2602N00004994') ''')
# ihr_test.createOrReplaceTempView("ihr_testing")
# # -- salesInvoiceNo ='CGVIR-25-R046088' and

# COMMAND ----------

# %sql
# select distinct InitiativeCode,ShipDate,InitiativeStartDate,InitiativeEndDate from ihr_testing 

# COMMAND ----------

# DBTITLE 1,prod master validation
# %skip
# display(psr_modified.count())
# #before: 14367155
# #after: 14367155
# print
# display(psr_modified.agg(sum('gross_transact_amt')))
# #before: 7757475099.70
# #after: 7757475099.70

# COMMAND ----------

ihr_psr_combined=spark.sql('''
Select * from ihr i
left join psr_modified p
on p.invoice_number = i.salesInvoiceNo
and p.time_period_end_date = i.ShipDate
and p.retailer_code=i.retailercode
-- and upper(i.channel)=upper(p.channel_name)
                           ''')
ihr_psr_combined.createOrReplaceTempView("ihr_psr_combined")

# COMMAND ----------

ihr_psr_aggregated=ihr_psr_combined\
    .withColumn("Category",F.trim(col("Category")))\
        .withColumn("Brand",F.trim(col("Brand")))\
            .withColumn("BrandForm",F.trim(col("BrandForm")))\
                .withColumn("SubbfName",F.trim(col("SubbfName")))\
                    .withColumn("channel_name",F.trim(col("channel_name")))\
    .groupBy("salesInvoiceNo","ShipDate","retailercode","InitiativeCode","channel","branchcode","invoice_number","return_of_invoice_number","RptMonthYear","time_period_end_date","ProductCode","Branch_code","retailer_code","channel_name","SubbfCode","SubbfName","Category","Brand","BrandForm","transaction_type")\
    .agg(F.first(F.col("amountdisbursed")).alias("amountdisbursed"),
         F.sum(F.col("item_unit_qty")).alias("item_unit_qty"),
         F.sum(F.col("gross_transact_amt")).alias("gross_transact_amt"),
         F.sum(F.col("tax_amt")).alias("tax_amt"),
         F.sum(F.col("net_transact_amt")).alias("net_transact_amt")) 
ihr_psr_aggregated.createOrReplaceTempView("ihr_psr_aggregated")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from ihr_psr_aggregated where InitiativeCode = 'LTR2601N00003617' --and invoice_number = 'CGVIR-25-I134637'

# COMMAND ----------

Master_retailer=Master_sdf
Master_retailer.createOrReplaceTempView("Master_retailer")

# COMMAND ----------

### check for schmes with two promotion group levels and each at differet product granularity
group_counts=Master_retailer.groupBy("Scheme_code").agg(F.countDistinct("Promotion_Group_Name").alias("promotion_group_count"),
                                                        F.countDistinct("Level_Type").alias("level_count"))\
                            .withColumn("promotion_group_level_count",F.when((col("promotion_group_count")>1) & (col("level_count")>1),lit(1)).otherwise(lit(0)))
group_counts.createOrReplaceTempView("group_counts")
master_sales_values=Master_retailer.join(group_counts,on="Scheme_code",how="left")\
    .withColumnRenamed("RetailerApplyCount","Retailer_Apply_Count")
master_sales_values.createOrReplaceTempView("master_sales_values")

# COMMAND ----------

# %sql
# select * from master_sales_values where Scheme_code = 'LTR2601N00003617' 
# --LTR2601N00003597CGBHA-25-I095580
# --and invoice_number = 'CGBHA-25-I095579'

# COMMAND ----------

intermediate = master_sales_values.alias("m").join(
    ihr_psr_aggregated.alias("i"),
    on=( (master_sales_values["scheme_code"] == ihr_psr_aggregated["initiativeCode"]) & (F.when(F.upper(master_sales_values["level_type"]) == "SUB-BRANDFORM", F.trim(F.upper(ihr_psr_aggregated["SubbfName"])) == F.trim(F.upper(master_sales_values["level_desc"])))
       .when(F.upper(master_sales_values["level_type"]) == "BRANDFORM", F.trim(F.upper(ihr_psr_aggregated["BrandForm"])) == F.trim(F.upper(master_sales_values["level_desc"])))
       .when(F.upper(master_sales_values["level_type"]) == "BRAND", F.trim(F.upper(ihr_psr_aggregated["Brand"])) ==  F.trim(F.upper(master_sales_values["level_desc"])))
       .when(F.upper(master_sales_values["level_type"]) == "CATEGORY",  F.trim(F.upper(ihr_psr_aggregated["Category"])) ==  F.trim(F.upper(master_sales_values["level_desc"])))
       .when(F.upper(master_sales_values["level_type"]) == "SKU",  F.trim(F.upper(ihr_psr_aggregated["ProductCode"])) ==  F.trim(F.upper(master_sales_values["level_desc"]))))),
    how="inner"
).groupBy("scheme_code","scheme_type","valid_from","valid_to","Scheme_Buy_Logic","Promotion_Group_Name","promotion_group_count","promotion_group_level_count","Level_Type","Promotion_Group_Logic","Promotion_Slab_Description","Promotion_Slab_Buy_Min","Promotion_Slab_Buy_Max","Promotion_Get_Type",
"Promotion_Slab_Get_Discount","channel","branchcode","retailer_code","Category","Brand","BrandForm","i.SubbfName",
"ShipDate","RptMonthYear","invoice_number","return_of_invoice_number","transaction_type") \
    .agg(F.first(F.col("amountdisbursed")).alias("amountdisbursed"),
         F.sum(F.col("item_unit_qty")).alias("item_unit_qty"),
         F.sum(F.col("gross_transact_amt")).alias("gross_transact_amt"),
         F.sum(F.col("tax_amt")).alias("tax_amt"),
         F.sum(F.col("net_transact_amt")).alias("net_transact_amt")) .select("scheme_code","scheme_type","valid_from","valid_to","Scheme_Buy_Logic","Promotion_Group_Name","promotion_group_count","promotion_group_level_count","Level_Type","Promotion_Group_Logic","Promotion_Slab_Description","Promotion_Slab_Buy_Min","Promotion_Slab_Buy_Max","Promotion_Get_Type","Promotion_Slab_Get_Discount","channel","branchcode","retailer_code","Category","Brand","BrandForm","i.SubbfName",
"ShipDate","RptMonthYear","invoice_number","return_of_invoice_number","item_unit_qty","gross_transact_amt","tax_amt","net_transact_amt","amountdisbursed","transaction_type")\
.filter(col("invoice_number").isNotNull())

intermediate.createOrReplaceTempView("intermediate")

intermediate_modified1=spark.sql(f'''
           select distinct return_of_invoice_number,ShipDate,invoice_number,scheme_code,subbfname,gross_transact_amt,item_unit_qty,net_transact_amt,amountdisbursed, transaction_type from intermediate ''')
intermediate_modified1.createOrReplaceTempView("intermediate_modified1")


# COMMAND ----------

# intermediate_modified=spark.sql('''
# with invoice_sd_ret_agg as
# (
#   select  scheme_code,invoice_number,Promotion_Group_Name,sum(net_transact_amt) as net_transact_amt_sum,sum(item_unit_qty) as item_unit_qty_sum from ( select SubbfName, scheme_code,invoice_number,Promotion_Group_Name,net_transact_amt,item_unit_qty,row_number() over(partition by scheme_code,invoice_number,Promotion_Group_Name,SubbfName order by Promotion_Slab_Description) as rnk from intermediate ) a where rnk=1 group by scheme_code,invoice_number,Promotion_Group_Name

# ),
# discount_calc_prep as
# (
# select t1.*,case when upper(scheme_type)="QUANTITY" then abs(item_unit_qty_sum) else abs(net_transact_amt_sum) end as condition from intermediate t1 join  invoice_sd_ret_agg t2 on t1.invoice_number=t2.invoice_number and t1.scheme_code=t2.scheme_code and t1.Promotion_Group_Name=t2.Promotion_Group_Name 
# ),
# dedup as (
# -- select * from discount_calc_prep where condition>=Promotion_Slab_Buy_Min and condition<=Promotion_Slab_Buy_Max

# select *except(Promotion_Slab_Description,Promotion_Slab_Buy_Max,Promotion_Slab_Buy_Min,Promotion_Slab_Get_Discount,Promotion_Group_Name),Promotion_Slab_Description,Promotion_Slab_Buy_Min,Promotion_Slab_Buy_Max,Promotion_Slab_Get_Discount,Promotion_Group_Name from discount_calc_prep where condition>=Promotion_Slab_Buy_Min and condition<=Promotion_Slab_Buy_Max  and gross_transact_amt>0
# union 
# select distinct *except(Promotion_Slab_Description,Promotion_Slab_Buy_Max, Promotion_Slab_Buy_Min,Promotion_Slab_Get_Discount,Promotion_Group_Name),  null as Promotion_Slab_Description , null as Promotion_Slab_Buy_Min,null as Promotion_Slab_Buy_Max,null as Promotion_Slab_Get_Discount, null as Promotion_Group_Name from discount_calc_prep where gross_transact_amt<0
# ),
# dedup_disc_calc as 
# (
# select * ,sum(gross_transact_amt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as gross_transact_amt_sum_1, sum(item_unit_qty) over(partition By scheme_code,invoice_number,Promotion_Group_Name) as item_unit_qty_sum_1 , sum(net_transact_amt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as net_transact_amt_sum_1, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty*Promotion_Slab_Get_Discount else (gross_transact_amt*Promotion_Slab_Get_Discount)/100 end as amtdisbursed_calculated_at_sbf_lvl_1, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty_sum_1*Promotion_Slab_Get_Discount else (gross_transact_amt_sum_1*Promotion_Slab_Get_Discount)/100 end as invcXscheme_lvl_Amount_disbursed_Calculated_1 from dedup
# )
# select * from dedup_disc_calc
#           ''')
# intermediate_modified.createOrReplaceTempView("intermediate_modified")

# COMMAND ----------

# intermediate_modified1=spark.sql('''
# with exclude_samesbf_from_mutliple_groups as 
# (
# select *, row_number() over(partition by invoice_number,scheme_code,SubbfName order by invcXscheme_lvl_Amount_disbursed_Calculated_1 desc) as rnk from intermediate_modified 
# ),
# dedup1 as (
# select * except(rnk) from exclude_samesbf_from_mutliple_groups where rnk=1
# )
# select *except(gross_transact_amt_sum_1,item_unit_qty_sum_1,net_transact_amt_sum_1,amtdisbursed_calculated_at_sbf_lvl_1,invcXscheme_lvl_Amount_disbursed_Calculated_1) ,sum(gross_transact_amt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as gross_transact_amt_agg, sum(item_unit_qty) over(partition By scheme_code,invoice_number,Promotion_Group_Name) as item_unit_qty_agg , sum(net_transact_amt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as net_transact_amt_agg, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty*Promotion_Slab_Get_Discount else (gross_transact_amt*Promotion_Slab_Get_Discount)/100 end as amtdisbursed_calculated_at_sbf_lvl, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty_agg*Promotion_Slab_Get_Discount else (gross_transact_amt_agg*Promotion_Slab_Get_Discount)/100 end as invcXscheme_lvl_Amount_disbursed_Calculated from dedup1


# ''')
# intermediate_modified1.createOrReplaceTempView("intermediate_modified1")

# COMMAND ----------

psr_new_of_return=spark.sql(f'''Select DocNumber as invoice_number,rptMonthYear as RptMonthYear,Date as time_period_end_date,
Pcode as ProductCode,PrimaryBranchCode as Branch_code,RtrCode as retailer_code,
upper(ChnlName) as channel_name,SubbfCode,SubbfName,
Qty as item_unit_qty,GrossValue as gross_transact_amt,
null as tax_amt,
Retailing as net_transact_amt
 from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails
 --cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails 
 where DocNumber in (select ApplyToDocNum from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails where DocNumber in (select salesinvoiceno from ihr where amountdisbursed<0 ))
''')
psr_new_of_return.createOrReplaceTempView("psr_new_of_return")
psr_modified_of_return=spark.sql('''
Select s1.*,
CategoryName as Category,BrandName as Brand,BrandformName as BrandForm from psr_new_of_return s1
left join cdl_india_data_prod.india_sdm_refined.productmaster p1
on s1.ProductCode=p1.ProductCode ''')
psr_modified_of_return.createOrReplaceTempView("psr_modified_of_return")

ihr_of_return=spark.sql(f'''
Select salesInvoiceNo,ShipDate,retailercode,InitiativeCode,channel,CAST(amountdisbursed AS double) AS amountdisbursed,
branchcode from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where SalesInvoiceNo in (select distinct invoice_number from psr_modified_of_return ) and ShipDate >= "{three_mnths_before}" and ShipDate <= "{end_date_value}" 
''')
ihr_of_return.createOrReplaceTempView("ihr_of_return")

ihr_psr_combined_of_return=spark.sql('''
Select * from ihr_of_return i1
left join psr_modified_of_return p2
on p2.invoice_number = i1.salesInvoiceNo
and p2.time_period_end_date = i1.ShipDate
and p2.retailer_code=i1.retailercode
-- and upper(i.channel)=upper(p.channel_name)
                           ''')
ihr_psr_combined_of_return.createOrReplaceTempView("ihr_psr_combined_of_return")


# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select min(shipdate),max(shipdate) from ihr_psr_combined_of_return

# COMMAND ----------

ihr_psr_aggregated_of_return=ihr_psr_combined_of_return\
    .withColumn("Category",F.trim(col("Category")))\
        .withColumn("Brand",F.trim(col("Brand")))\
            .withColumn("BrandForm",F.trim(col("BrandForm")))\
                .withColumn("SubbfName",F.trim(col("SubbfName")))\
                    .withColumn("channel_name",F.trim(col("channel_name")))\
    .groupBy("salesInvoiceNo","ShipDate","retailercode","InitiativeCode","channel","branchcode","invoice_number","RptMonthYear","time_period_end_date","ProductCode","Branch_code","retailer_code","channel_name","SubbfCode","SubbfName","Category","Brand","BrandForm")\
    .agg(F.first(F.col("amountdisbursed")).alias("amountdisbursed"),
         F.sum(F.col("item_unit_qty")).alias("item_unit_qty"),
         F.sum(F.col("gross_transact_amt")).alias("gross_transact_amt"),
         F.sum(F.col("tax_amt")).alias("tax_amt"),
         F.sum(F.col("net_transact_amt")).alias("net_transact_amt")) 
ihr_psr_aggregated_of_return.createOrReplaceTempView("ihr_psr_aggregated_of_return")


intermediate_of_return = master_sales_values.alias("m5").join(
    ihr_psr_aggregated_of_return.alias("i5"),
    on=( (master_sales_values["scheme_code"] == ihr_psr_aggregated_of_return["initiativeCode"]) & (F.when(F.upper(master_sales_values["level_type"]) == "SUB-BRANDFORM", F.trim(F.upper(ihr_psr_aggregated_of_return["SubbfName"])) == F.trim(F.upper(master_sales_values["level_desc"])))
       .when(F.upper(master_sales_values["level_type"]) == "BRANDFORM", F.trim(F.upper(ihr_psr_aggregated_of_return["BrandForm"])) == F.trim(F.upper(master_sales_values["level_desc"])))
       .when(F.upper(master_sales_values["level_type"]) == "BRAND", F.trim(F.upper(ihr_psr_aggregated_of_return["Brand"])) ==  F.trim(F.upper(master_sales_values["level_desc"])))
       .when(F.upper(master_sales_values["level_type"]) == "CATEGORY",  F.trim(F.upper(ihr_psr_aggregated_of_return["Category"])) ==  F.trim(F.upper(master_sales_values["level_desc"])))
       .when(F.upper(master_sales_values["level_type"]) == "SKU",  F.trim(F.upper(ihr_psr_aggregated_of_return["ProductCode"])) ==  F.trim(F.upper(master_sales_values["level_desc"]))))),
    how="inner"
).groupBy("scheme_code","scheme_type","valid_from","valid_to","Scheme_Buy_Logic","Promotion_Group_Name","promotion_group_count","promotion_group_level_count","Level_Type","Promotion_Group_Logic","Promotion_Slab_Description","Promotion_Slab_Buy_Min","Promotion_Slab_Buy_Max","Promotion_Get_Type",
"Promotion_Slab_Get_Discount","channel","branchcode","retailer_code","Category","Brand","BrandForm","i5.SubbfName",
"ShipDate","RptMonthYear","invoice_number") \
    .agg(F.first(F.col("amountdisbursed")).alias("amountdisbursed"),
         F.sum(F.col("item_unit_qty")).alias("item_unit_qty"),
         F.sum(F.col("gross_transact_amt")).alias("gross_transact_amt"),
         F.sum(F.col("tax_amt")).alias("tax_amt"),
         F.sum(F.col("net_transact_amt")).alias("net_transact_amt")) .select("scheme_code","scheme_type","valid_from","valid_to","Scheme_Buy_Logic","Promotion_Group_Name","promotion_group_count","promotion_group_level_count","Level_Type","Promotion_Group_Logic","Promotion_Slab_Description","Promotion_Slab_Buy_Min","Promotion_Slab_Buy_Max","Promotion_Get_Type","Promotion_Slab_Get_Discount","channel","branchcode","retailer_code","Category","Brand","BrandForm","i5.SubbfName",
"ShipDate","RptMonthYear","invoice_number","item_unit_qty","gross_transact_amt","tax_amt","net_transact_amt","amountdisbursed")\
.filter(col("invoice_number").isNotNull())

intermediate_of_return.createOrReplaceTempView("intermediate_of_return")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from intermediate_of_return where Scheme_code = 'LTR2602N00004522' and RptMonthYear = '202602' and ShipDate between '2026-02-01' and '2026-02-14'
# MAGIC --'LTR2602N00000121_MRI'
# MAGIC -- 'LTR2601N00003617' and invoice_number = 'CGVIR-25-I134637'
# MAGIC -- LTR2601N00003617CGVIR-25-I134637

# COMMAND ----------

# display(intermediate_of_return)

# COMMAND ----------

# MAGIC %skip
# MAGIC display(psr_modified_of_return.count())
# MAGIC #before: 1198328
# MAGIC #after: 1198328
# MAGIC print
# MAGIC display(psr_modified_of_return.agg(sum('gross_transact_amt')))
# MAGIC #before: 994762717.17
# MAGIC #after: 994762717.17

# COMMAND ----------

# DBTITLE 1,adjstd_intermediate_of_return
adjstd_intermediate_of_return=spark.sql(f'''
          --return values are negative hence adding
select t1.*except(amountdisbursed),t2.invoice_number as return_invoice_number,

-- REMOVED: coalesce(t2.gross_transact_amt,0) as gross_transact_amt_return,
-- CHANGED: Damaged Return rows now contribute 0 gross amount; all other return types unchanged
case when t2.transaction_type = 'DAMAGED RETURN' then 0 else coalesce(t2.gross_transact_amt,0) end as gross_transact_amt_return,

-- REMOVED: coalesce(t2.item_unit_qty,0) as item_unit_qty_return,
-- CHANGED: Damaged Return rows now contribute 0 quantity; all other return types unchanged
case when t2.transaction_type = 'DAMAGED RETURN' then 0 else coalesce(t2.item_unit_qty,0) end as item_unit_qty_return,

-- REMOVED: coalesce(t2.net_transact_amt,0) as net_transact_amt_return,
-- CHANGED: Damaged Return rows now contribute 0 net amount; all other return types unchanged
case when t2.transaction_type = 'DAMAGED RETURN' then 0 else coalesce(t2.net_transact_amt,0) end as net_transact_amt_return,

t1.amountdisbursed as amountdisbursed_orginal,

-- UNCHANGED: IHR return amount is always fully retained, Damaged or not
coalesce(t2.amountdisbursed,0) as amountdisbursed_retrun,

-- REMOVED: t1.gross_transact_amt+coalesce(t2.gross_transact_amt,0) as gross_transact_amt_updt,
-- CHANGED: _updt now built from the neutralized value above, so a Damaged Return leaves the original gross amount untouched
t1.gross_transact_amt + (case when t2.transaction_type = 'DAMAGED RETURN' then 0 else coalesce(t2.gross_transact_amt,0) end) as gross_transact_amt_updt,

-- REMOVED: t1.item_unit_qty+coalesce(t2.item_unit_qty,0) as item_unit_qty_updt,
-- CHANGED: same neutralization applied for quantity
t1.item_unit_qty + (case when t2.transaction_type = 'DAMAGED RETURN' then 0 else coalesce(t2.item_unit_qty,0) end) as item_unit_qty_updt,

-- REMOVED: t1.net_transact_amt+coalesce(t2.net_transact_amt,0) as net_transact_amt_updt,
-- CHANGED: same neutralization applied for net amount
t1.net_transact_amt + (case when t2.transaction_type = 'DAMAGED RETURN' then 0 else coalesce(t2.net_transact_amt,0) end) as net_transact_amt_updt,

t2.shipdate as srn_date,

-- ADDED: brand-new column, did not exist before -- carries the return's transaction type forward for audit / future filtering purposes
t2.transaction_type as return_transaction_type

from intermediate_of_return t1 left join intermediate_modified1 t2 on t1.invoice_number=t2.return_of_invoice_number and t1.scheme_code=t2.scheme_code and trim(upper(t1.subbfname))=upper(trim(t2.subbfname))
          ''')
adjstd_intermediate_of_return.createOrReplaceTempView("adjstd_intermediate_of_return")

# COMMAND ----------

# %sql
# select * from adjstd_intermediate_of_return where Scheme_code = 'LTR2602N00000121_MRI'

# COMMAND ----------

# DBTITLE 1,Adding this to accommodate same sbf multiple returns
adjstd_intermediate_of_return_1 = (
    adjstd_intermediate_of_return
    .groupBy('scheme_code','scheme_type','valid_from','valid_to','Scheme_Buy_Logic','Promotion_Group_Name','promotion_group_count','promotion_group_level_count','Level_Type','Promotion_Group_Logic','Promotion_Slab_Description','Promotion_Slab_Buy_Min','Promotion_Slab_Buy_Max','Promotion_Get_Type','Promotion_Slab_Get_Discount','channel','branchcode','retailer_code','Category','Brand','BrandForm','SubbfName','ShipDate','RptMonthYear','invoice_number')
    .agg(F.first(F.col("item_unit_qty")).alias("item_unit_qty"),
         F.first(F.col("gross_transact_amt")).alias("gross_transact_amt"),
         F.first(F.col("tax_amt")).alias("tax_amt"),
         F.first(F.col("net_transact_amt")).alias("net_transact_amt"),
         F.first(F.col("amountdisbursed_orginal")).alias("amountdisbursed_orginal"),

         F.max(F.col("srn_date")).alias("srn_date"),

         F.sum(F.col("item_unit_qty_return")).alias("item_unit_qty_return"),
         F.sum(F.col("gross_transact_amt_return")).alias("gross_transact_amt_return"),
         F.sum(F.col("net_transact_amt_return")).alias("net_transact_amt_return"),
         F.sum(F.col("amountdisbursed_retrun")).alias("amountdisbursed_retrun"),
         array_distinct(collect_list(col('return_invoice_number'))).alias('return_invoice_number')
         
        )
  
    .withColumn('gross_transact_amt_updt',coalesce((col('gross_transact_amt')+col('gross_transact_amt_return')),lit(0)))
    .withColumn('item_unit_qty_updt',coalesce(col('item_unit_qty')+col('item_unit_qty_return'),lit(0)))
    .withColumn('net_transact_amt_updt',coalesce(col('net_transact_amt')+col('net_transact_amt_return'),lit(0)))
)
adjstd_intermediate_of_return_1.createOrReplaceTempView("adjstd_intermediate_of_return_1")

# COMMAND ----------

# %sql
# -- select * from intermediate_of_return where Scheme_code = 'LTR2601N00003617' and invoice_number = 'CGVIR-25-I134637'
# display(adjstd_intermediate_of_return.filter((col('invoice_number')=='CGVIR-25-I134637')&(col('scheme_code')=='LTR2601N00003617')))
# display(adjstd_intermediate_of_return.filter((col('invoice_number')=='CGBHA-25-I095579')&(col('scheme_code')=='LTR2601N00003606')))
# LTR2601N00003606CGBHA-25-I095579
# display(adjstd_intermediate_of_return.filter((col('invoice_number')=='CGBHA-25-I095579')&(col('scheme_code')=='LTR2601N00003606')))
# display(adjstd_intermediate_of_return.filter((col('invoice_number')=='CGBHA-25-I095580')&(col('scheme_code')=='LTR2601N00003597')))
# where Scheme_code = 'LTR2601N00003597' and invoice_number = 'CGBHA-25-I095580'

# COMMAND ----------

# MAGIC %skip
# MAGIC # Checked example for damaged return on ionvoice level aggr.
# MAGIC # display(adjstd_intermediate_of_return.filter((col('invoice_number')=='CGVIR-25-I132684')&(col('scheme_code')=='LSS2601N1638_BC2')))

# COMMAND ----------

# NEW: Save dedup_of_return as separate df with LEFT JOIN.
# Slab match-> Promotion_Group_Name t4 se aayega, match nahi toh NULL.
# Used in cell 23 — NULL Promotion_Group_Name rows won't be counted in group count.

dedup_of_return_df=spark.sql('''
with invoice_sd_ret_agg_of_return as
(
  select  scheme_code,invoice_number,Promotion_Group_Name,sum(net_transact_amt_updt) as net_transact_amt_sum,sum(item_unit_qty_updt) as item_unit_qty_sum from ( select SubbfName, scheme_code,invoice_number,Promotion_Group_Name,net_transact_amt_updt,item_unit_qty_updt,row_number() over(partition by scheme_code,invoice_number,Promotion_Group_Name,SubbfName order by Promotion_Slab_Description) as rnk from adjstd_intermediate_of_return_1 ) a where rnk=1 group by scheme_code,invoice_number,Promotion_Group_Name
),
discount_calc_prep_of_return as
(
select t1.*,case when upper(scheme_type)="QUANTITY" then abs(item_unit_qty_sum) else abs(net_transact_amt_sum) end as condition from adjstd_intermediate_of_return_1 t1 join  invoice_sd_ret_agg_of_return t2 on t1.invoice_number=t2.invoice_number and t1.scheme_code=t2.scheme_code and t1.Promotion_Group_Name=t2.Promotion_Group_Name 
),
master as (
select distinct scheme_code,promotion_group_name,promotion_slab_description,Promotion_Slab_Buy_Min,Promotion_Slab_Buy_Max,Promotion_Slab_Get_Discount,Promotion_Get_Type from master_Sdf
),
dedup_of_return as (
select t3.*except(Promotion_Slab_Buy_Min,Promotion_Slab_Buy_Max,promotion_group_name,promotion_slab_description,Promotion_Get_Type,Promotion_Slab_Get_Discount),t4.*except(scheme_code) from discount_calc_prep_of_return t3 left join master t4 on t3.condition>=t4.Promotion_Slab_Buy_Min and t3.condition<=t4.Promotion_Slab_Buy_Max
and t3.scheme_code=t4.scheme_code and lower(trim(t3.promotion_group_name))=lower(trim(t4.promotion_group_name)) and lower(trim(t3.promotion_slab_description))=lower(trim(t4.promotion_slab_description))
)
select * from dedup_of_return
''')

dedup_of_return_df.createOrReplaceTempView("dedup_of_return")

# COMMAND ----------

# DBTITLE 1,Adding promotion group counts before excluding same sbf from multiple groups
# current_group_counts=adjstd_intermediate_of_return.groupBy("Scheme_code","invoice_number","ShipDate").agg(F.countDistinct("Promotion_Group_Name").alias("current_group_count"))
# count_check=current_group_counts.join(group_counts,on="Scheme_code",how="left")\
#     .withColumn("group_count_check",F.when(col("current_group_count")<col("promotion_group_count"),0).otherwise(1))
# count_check.createOrReplaceTempView("count_check")

## Add filter for only taking sbfs with 1 or more quantities left after return
## UPDATED: Source changed to dedup_of_return_df.
## Promotion_Group_Name is NULL for non-slab-matching rows — so they won't be counted.
## Condition item_unit_qty_updt!=0 retained as is.

current_group_counts=(dedup_of_return_df
                      .groupBy("Scheme_code","invoice_number","ShipDate")
                      .agg(
                          F.countDistinct(
                              F.when(F.col('item_unit_qty_updt')!=0,F.col('Promotion_Group_Name'))
                              ).alias("current_group_count")
                          )
)

count_check=current_group_counts.join(group_counts,on="Scheme_code",how="left")\
    .withColumn("group_count_check",F.when(col("current_group_count")<col("promotion_group_count"),0).otherwise(1))
count_check.createOrReplaceTempView("count_check")

current_sbf_counts = (
   adjstd_intermediate_of_return_1
   .groupBy(
       "Scheme_code",
       "invoice_number",
       "ShipDate",
       "Promotion_Group_Name"
   )
   .agg(
       F.countDistinct(
           F.when(
               F.col("item_unit_qty_updt") != 0,
               F.col("SubbfName")
           )
       ).alias("current_sbf_count")
   )
)
variance_check = (
   current_sbf_counts.alias("v")
   .join(
       master_sales_values
       .select(
           "Scheme_code",
           "Promotion_Group_Name",
           "Variance"
       )
       .distinct()
       .alias("m"),
       on=["Scheme_code","Promotion_Group_Name"],
       how="left"
   )
   .withColumn(
       "variance_check",
       F.when(
           col("Variance").isNull(),
           1
       )
       .when(
           col("current_sbf_count") >= col("Variance"),
           1
       )
       .otherwise(0)
   )
)
variance_check.createOrReplaceTempView("variance_check")

# COMMAND ----------

variance_check_final = (
   variance_check
   .groupBy(
       "scheme_code",
       "invoice_number",
       "ShipDate"
   )
   .agg(
       F.min("variance_check").alias("variance_check")
   )
)
variance_check_final.createOrReplaceTempView("variance_check_final")

# COMMAND ----------

# variance_check_final

# COMMAND ----------

# DBTITLE 1,testing
# display(count_check.filter((col('invoice_number')=='CGBHH-25-I048952')&(col('scheme_code')=='LTR2601N00003581')))

# COMMAND ----------

##### Here we have updated discount calculated for returns
# commenting the code as it has been used in above cell
#intermediate_modified_of_return=spark.sql('''
#with invoice_sd_ret_agg_of_return as
#(
# select  scheme_code,invoice_number,Promotion_Group_Name,sum(net_transact_amt_updt) as net_transact_amt_sum,sum(item_unit_qty_updt) as item_unit_qty_sum from ( select SubbfName, scheme_code,invoice_number,Promotion_Group_Name,net_transact_amt_updt,item_unit_qty_updt,row_number() over(partition by scheme_code,invoice_number,Promotion_Group_Name,SubbfName order by Promotion_Slab_Description) as rnk from adjstd_intermediate_of_return_1 ) a where rnk=1 group by scheme_code,invoice_number,Promotion_Group_Name
#),
#discount_calc_prep_of_return as
#(
#select t1.*,case when upper(scheme_type)="QUANTITY" then abs(item_unit_qty_sum) else abs(net_transact_amt_sum) end as condition from adjstd_intermediate_of_return_1 t1 join  invoice_sd_ret_agg_of_return t2 on t1.invoice_number=t2.invoice_number and t1.scheme_code=t2.scheme_code and t1.Promotion_Group_Name=t2.Promotion_Group_Name 
#),
#master(
#select distinct scheme_code,promotion_group_name,promotion_slab_description,Promotion_Slab_Buy_Min,Promotion_Slab_Buy_Max,Promotion_Slab_Get_Discount,Promotion_Get_Type from master_Sdf
#),
#dedup_of_return as (
#select t3.*except(Promotion_Slab_Buy_Min,Promotion_Slab_Buy_Max,promotion_group_name,promotion_slab_description,Promotion_Get_Type,Promotion_Slab_Get_Discount),t4.*except(scheme_code)  from discount_calc_prep_of_return t3 left join  master t4 on t3.condition>=t4.Promotion_Slab_Buy_Min and t3.condition<=t4.Promotion_Slab_Buy_Max 
#and t3.scheme_code=t4.scheme_code and lower(trim(t3.promotion_group_name))=lower(trim(t4.promotion_group_name)) and lower(trim(t3.promotion_slab_description))=lower(trim(t4.promotion_slab_description))
#),

intermediate_modified_of_return=spark.sql('''
with dedup_disc_calc_of_return as 
(
select * ,sum(gross_transact_amt_updt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as gross_transact_amt_sum_1, sum(item_unit_qty_updt) over(partition By scheme_code,invoice_number,Promotion_Group_Name) as item_unit_qty_sum_1 , sum(net_transact_amt_updt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as net_transact_amt_sum_1, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty*Promotion_Slab_Get_Discount else (gross_transact_amt*Promotion_Slab_Get_Discount)/100 end as amtdisbursed_calculated_at_sbf_lvl_1, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty_sum_1*Promotion_Slab_Get_Discount else (gross_transact_amt_sum_1*Promotion_Slab_Get_Discount)/100 end as invcXscheme_lvl_Amount_disbursed_Calculated_1 from dedup_of_return
)
,result8 as (
select * from dedup_disc_calc_of_return
),
 exclude_samesbf_from_mutliple_groups_of_return as 
(
select *, row_number() over(partition by invoice_number,scheme_code,SubbfName order by case when upper(Promotion_Group_Name)=upper("Group_Level1") then 1 when upper(Promotion_Group_Name)=upper("Group_Level2") then 2 when upper(Promotion_Group_Name)=upper("Group_Level3") then 3 when upper(Promotion_Group_Name)=upper("Group_Level4") then 4
 else 5 end ,invcXscheme_lvl_Amount_disbursed_Calculated_1 desc) as rnk from result8 
),
dedup1 as (
select * except(rnk) from exclude_samesbf_from_mutliple_groups_of_return where rnk=1
)
select *except(gross_transact_amt_sum_1,item_unit_qty_sum_1,net_transact_amt_sum_1,amtdisbursed_calculated_at_sbf_lvl_1,invcXscheme_lvl_Amount_disbursed_Calculated_1) ,sum(gross_transact_amt_updt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as gross_transact_amt_agg,sum(gross_transact_amt_return) over(partition by scheme_code,invoice_number) as gross_transact_amt_return_agg,sum(gross_transact_amt) over(partition by scheme_code,invoice_number) as gross_transact_amt_org_agg, sum(item_unit_qty_updt) over(partition By scheme_code,invoice_number,Promotion_Group_Name) as item_unit_qty_agg , sum(net_transact_amt_updt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as net_transact_amt_agg, coalesce(case when lower(Promotion_Get_Type)="discount every" then item_unit_qty_updt*Promotion_Slab_Get_Discount else (gross_transact_amt_updt*Promotion_Slab_Get_Discount)/100 end,0) as amtdisbursed_calculated_at_sbf_lvl, coalesce(case when lower(Promotion_Get_Type)="discount every" then item_unit_qty_agg*Promotion_Slab_Get_Discount else (gross_transact_amt_agg*Promotion_Slab_Get_Discount)/100 end,0) as invcXscheme_lvl_Amount_disbursed_Calculated,(gross_transact_amt_return_agg/gross_transact_amt_org_agg)*amountdisbursed_orginal as amountdisbursed_calc_retrun
from dedup1

          ''')
intermediate_modified_of_return.createOrReplaceTempView("intermediate_modified_of_return")

# COMMAND ----------

# display(
#     intermediate_modified_of_return.filter(
#         (col('scheme_code')=='LSS2601N1638_BC2') & 
#         (col('invoice_number')=='CGVIR-25-I132684')
#     ).select('Promotion_Group_Name','amountdisbursed_calc_retrun').distinct()
# )

# COMMAND ----------

# display(intermediate_modified_of_return.filter((col('invoice_number')=='ABBMR-25-I026433')))

# COMMAND ----------

# display(intermediate_modified_of_return.filter((col('invoice_number')=='CGBHH-25-I048952')&(col('scheme_code')=='LTR2601N00003581')))

# COMMAND ----------

# DBTITLE 1,Removing from here as already handled above
# current_group_counts=intermediate_modified_of_return.groupBy("Scheme_code","invoice_number","ShipDate").agg(F.countDistinct("Promotion_Group_Name").alias("current_group_count"))
# count_check=current_group_counts.join(group_counts,on="Scheme_code",how="left")\
#     .withColumn("group_count_check",F.when(col("current_group_count")<col("promotion_group_count"),0).otherwise(1))
# count_check.createOrReplaceTempView("count_check")

# COMMAND ----------

# display(intermediate_modified_of_return.select('scheme_code').distinct().count())

# COMMAND ----------

# updated_return_disc_calc=spark.sql('''select invoice_number,array_distinct(collect_list(return_invoice_number)) as return_invoice_number,ShipDate,max(srn_date) as SRN_Date,RptMonthYear,scheme_code,max(amountdisbursed_orginal) as amountdisbursed_orginal, aggregate(collect_set(amountdisbursed_retrun),cast(0.0 as double) ,(acc,x)->acc+x ) as amountdisbursed_retrun1
# ,sum(invcXscheme_lvl_Amount_disbursed_Calculated) as invcXscheme_lvl_Amount_disbursed_Calculated_new, max(amountdisbursed_calc_retrun) as amountdisbursed_calc_return from intermediate_modified_of_return where return_invoice_number is not null
# group by invoice_number,ShipDate,RptMonthYear,scheme_code ''')
# updated_return_disc_calc.createOrReplaceTempView("updated_return_disc_calc")

# COMMAND ----------

# MAGIC %skip
# MAGIC # Updating the code because invcXscheme_lvl_Amount_disbursed_Calculated is calculated till a promotion group level, so we have to take max at that level to avoid duplication, and then we have to sum till an invoice level
# MAGIC
# MAGIC updated_return_disc_calc = (spark.sql(f"""                                      
# MAGIC     with promo_lvl as (
# MAGIC         select 
# MAGIC         invoice_number,ShipDate,RptMonthYear,scheme_code,promotion_group_name,
# MAGIC         max(invcXscheme_lvl_Amount_disbursed_Calculated) as invcXscheme_lvl_Amount_disbursed_promo_grp
# MAGIC         from intermediate_modified_of_return
# MAGIC         group by invoice_number,ShipDate,RptMonthYear,scheme_code,promotion_group_name
# MAGIC     ),
# MAGIC     final_promo_amt as (
# MAGIC         select 
# MAGIC         invoice_number,ShipDate,RptMonthYear,scheme_code,
# MAGIC         sum(invcXscheme_lvl_Amount_disbursed_promo_grp) as invcXscheme_lvl_Amount_disbursed_Calculated_new
# MAGIC         from promo_lvl
# MAGIC         group by invoice_number,ShipDate,RptMonthYear,scheme_code
# MAGIC     )
# MAGIC     select
# MAGIC     a.invoice_number,
# MAGIC     array_distinct(collect_list(return_invoice_number)) as return_invoice_number,
# MAGIC     a.shipdate, max(srn_date) as SRN_Date,a.RptMonthYear,a.scheme_code,max(amountdisbursed_orginal) as amountdisbursed_orginal, 
# MAGIC     aggregate(collect_set(amountdisbursed_retrun),cast(0.0 as double) ,(acc,x)->acc+x ) as amountdisbursed_retrun1,
# MAGIC     max(f.invcXscheme_lvl_Amount_disbursed_Calculated_new) as invcXscheme_lvl_Amount_disbursed_Calculated_new, 
# MAGIC     max(amountdisbursed_calc_retrun) as amountdisbursed_calc_return 
# MAGIC     from intermediate_modified_of_return a
# MAGIC     left join final_promo_amt f 
# MAGIC     on a.invoice_number=f.invoice_number
# MAGIC     and a.shipdate=f.ShipDate
# MAGIC     and a.rptmonthyear=f.RptMonthYear
# MAGIC     and a.scheme_code=f.scheme_code
# MAGIC     group by a.invoice_number,a.ShipDate,a.RptMonthYear,a.scheme_code
# MAGIC     HAVING MAX(return_invoice_number) is not null
# MAGIC     """))
# MAGIC
# MAGIC updated_return_disc_calc.createOrReplaceTempView("updated_return_disc_calc")

# COMMAND ----------

# DBTITLE 1,final version
# Updating the code because invcXscheme_lvl_Amount_disbursed_Calculated is calculated till a promotion group level, so we have to take max at that level to avoid duplication, and then we have to sum till an invoice level
# For each unique return invoice number, take MAX(amountdisbursed_retrun), then: SUM across all return invoices
# Updating the code because invcXscheme_lvl_Amount_disbursed_Calculated is calculated till a promotion group level, so we have to take max at that level to avoid duplication, and then we have to sum till an invoice level
# For each unique return invoice number, take MAX(amountdisbursed_retrun), then: SUM across all return invoices
updated_return_disc_calc = (spark.sql(f"""
  with promo_lvl as (
    select invoice_number,ShipDate,RptMonthYear,scheme_code,promotion_group_name,
      max(invcXscheme_lvl_Amount_disbursed_Calculated) as invcXscheme_lvl_Amount_disbursed_promo_grp
    from intermediate_modified_of_return
    group by invoice_number,ShipDate,RptMonthYear,scheme_code,promotion_group_name),

  return_invoice_dedup as (
    select *,
      row_number() over (partition by invoice_number,shipdate,rptmonthyear,
        scheme_code,return_invoice_number order by amountdisbursed_retrun desc) as rn_return
    from adjstd_intermediate_of_return),

  final_promo_amt as (
    select invoice_number,ShipDate,RptMonthYear,scheme_code,
      sum(invcXscheme_lvl_Amount_disbursed_promo_grp) as invcXscheme_lvl_Amount_disbursed_Calculated_new
    from promo_lvl
    group by invoice_number,ShipDate,RptMonthYear,scheme_code),

  return_info as (
    select invoice_number,ShipDate,RptMonthYear,scheme_code,
      array_distinct(collect_list(return_invoice_number)) as return_invoice_number,
      max(srn_date) as SRN_Date,
      sum(case when rn_return=1 then amountdisbursed_retrun else 0 end) as amountdisbursed_retrun1
    from return_invoice_dedup
    group by invoice_number,ShipDate,RptMonthYear,scheme_code)

  select a.invoice_number, max(g.return_invoice_number) as return_invoice_number,
    max(g.amountdisbursed_retrun1) as amountdisbursed_retrun1,
    max(g.SRN_Date) as SRN_Date,
    a.shipdate,a.RptMonthYear,a.scheme_code,
    max(amountdisbursed_orginal) as amountdisbursed_orginal,
    max(f.invcXscheme_lvl_Amount_disbursed_Calculated_new) as invcXscheme_lvl_Amount_disbursed_Calculated_new,
    max(amountdisbursed_calc_retrun) as amountdisbursed_calc_return
  from intermediate_modified_of_return a
  left join final_promo_amt f on a.invoice_number=f.invoice_number
    and a.shipdate=f.ShipDate and a.rptmonthyear=f.RptMonthYear and a.scheme_code=f.scheme_code
  left join return_info g on a.invoice_number=g.invoice_number
    and a.shipdate=g.ShipDate and a.rptmonthyear=g.RptMonthYear and a.scheme_code=g.scheme_code
  group by a.invoice_number,a.ShipDate,a.RptMonthYear,a.scheme_code
  -- HAVING MAX(g.return_invoice_number) is not null
    HAVING EXISTS(MAX(g.return_invoice_number), x -> x is not null)
  """))

updated_return_disc_calc.createOrReplaceTempView('updated_return_disc_calc')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from updated_return_disc_calc where Scheme_code = 'LTR2601N00003617' and invoice_number = 'CGVIR-25-I134637'
# MAGIC --(adjstd_intermediate_of_return.filter((col('invoice_number')=='CGVIR-25-I132684')&(col('scheme_code')=='LSS2601N1638_BC2')))
# MAGIC -- display(promotion_slab_check_aggregated.filter((col('invoice_number')=='CGVIR-25-I134637')&(col('scheme_code')=='LTR2601N00003617'))) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from updated_return_disc_calc where Scheme_code = 'LSS2601N1638_BC2' and invoice_number = 'CGVIR-25-I132684'
# MAGIC --(adjstd_intermediate_of_return.filter((col('invoice_number')=='CGVIR-25-I132684')&(col('scheme_code')=='LSS2601N1638_BC2')))

# COMMAND ----------

# display(adjstd_intermediate_of_return.limit(2))

# COMMAND ----------

# MAGIC %skip
# MAGIC # Updating the code because invcXscheme_lvl_Amount_disbursed_Calculated is calculated till a promotion group level, so we have to take max at that level to avoid duplication, and then we have to sum till an invoice level
# MAGIC # For each unique return invoice number, take MAX(amountdisbursed_retrun), then: SUM across all return invoices
# MAGIC
# MAGIC updated_return_disc_calc = (spark.sql(f"""                                      
# MAGIC     with promo_lvl as (
# MAGIC         select 
# MAGIC         invoice_number,ShipDate,RptMonthYear,scheme_code,promotion_group_name,
# MAGIC         max(invcXscheme_lvl_Amount_disbursed_Calculated) as invcXscheme_lvl_Amount_disbursed_promo_grp
# MAGIC         from intermediate_modified_of_return
# MAGIC         group by invoice_number,ShipDate,RptMonthYear,scheme_code,promotion_group_name
# MAGIC     ),
# MAGIC     return_invoice_dedup as (
# MAGIC         select *,
# MAGIC         row_number() over (
# MAGIC             partition by
# MAGIC             invoice_number,
# MAGIC             shipdate,
# MAGIC             rptmonthyear,
# MAGIC             scheme_code,
# MAGIC             return_invoice_number
# MAGIC             order by
# MAGIC             amountdisbursed_retrun desc
# MAGIC         ) as rn_return
# MAGIC         from intermediate_modified_of_return
# MAGIC     ),
# MAGIC
# MAGIC     final_promo_amt as (
# MAGIC         select 
# MAGIC         invoice_number,ShipDate,RptMonthYear,scheme_code,
# MAGIC         sum(invcXscheme_lvl_Amount_disbursed_promo_grp) as invcXscheme_lvl_Amount_disbursed_Calculated_new
# MAGIC         from promo_lvl
# MAGIC         group by invoice_number,ShipDate,RptMonthYear,scheme_code
# MAGIC     )
# MAGIC     select
# MAGIC     a.invoice_number,
# MAGIC     array_distinct(collect_list(return_invoice_number)) as return_invoice_number,
# MAGIC     a.shipdate, max(srn_date) as SRN_Date,a.RptMonthYear,a.scheme_code,max(amountdisbursed_orginal) as amountdisbursed_orginal, 
# MAGIC     -- aggregate(collect_set(amountdisbursed_retrun),cast(0.0 as double) ,(acc,x)->acc+x ) as amountdisbursed_retrun1,
# MAGIC     sum(case when rn_return=1 then amountdisbursed_retrun else 0 end) as amountdisbursed_retrun1,
# MAGIC     max(f.invcXscheme_lvl_Amount_disbursed_Calculated_new) as invcXscheme_lvl_Amount_disbursed_Calculated_new, 
# MAGIC     max(amountdisbursed_calc_retrun) as amountdisbursed_calc_return 
# MAGIC     from return_invoice_dedup a
# MAGIC     left join final_promo_amt f 
# MAGIC     on a.invoice_number=f.invoice_number
# MAGIC     and a.shipdate=f.ShipDate
# MAGIC     and a.rptmonthyear=f.RptMonthYear
# MAGIC     and a.scheme_code=f.scheme_code
# MAGIC     group by a.invoice_number,a.ShipDate,a.RptMonthYear,a.scheme_code
# MAGIC     HAVING MAX(return_invoice_number) is not null
# MAGIC     """))
# MAGIC
# MAGIC updated_return_disc_calc.createOrReplaceTempView("updated_return_disc_calc")

# COMMAND ----------

# MAGIC %skip
# MAGIC display(updated_return_disc_calc.select('scheme_code').distinct().count())
# MAGIC #earlier-18267
# MAGIC print()
# MAGIC display(updated_return_disc_calc.count())
# MAGIC #earlier
# MAGIC #597850
# MAGIC print()
# MAGIC display(promotion_slab_check_aggregated.count())
# MAGIC #earlier
# MAGIC #

# COMMAND ----------

# display(updated_return_disc_calc.filter((col('invoice_number')=='CGBHH-25-I048952')&(col('scheme_code')=='LTR2601N00003581')))

# COMMAND ----------

# display(updated_return_disc_calc.filter((col('invoice_number')=='CGCHE-25-I105091')&(col('scheme_code')=='LTR2512N00000934')))

# COMMAND ----------

# display(variance_check_final)

# COMMAND ----------

# DBTITLE 1,promotion group count check
# discount_condition = spark.sql(f'''
# select
#    t8.* except(invcXscheme_lvl_Amount_disbursed_Calculated_new),
#    case
#        when t9.group_count_check = 0 then 0
#        when v.variance_check = 0 then 0
#        else invcXscheme_lvl_Amount_disbursed_Calculated_new
#    end as new_AmtDisbursed
# from updated_return_disc_calc t8
# left join count_check t9
#    on t8.scheme_code=t9.scheme_code
#    and t8.invoice_number=t9.invoice_number
#    and t8.shipdate=t9.shipdate
# left join variance_check_final v
#    on t8.scheme_code=v.scheme_code
#    and t8.invoice_number=v.invoice_number
#    and t8.shipdate=v.shipdate
# ''')
# discount_condition.createOrReplaceTempView("discount_condition")

# COMMAND ----------

# DBTITLE 1,promotion group count check - variance changes
discount_condition = spark.sql(f'''
select
   t8.* except(invcXscheme_lvl_Amount_disbursed_Calculated_new),
   case
       when t9.group_count_check = 0 then 0
       else invcXscheme_lvl_Amount_disbursed_Calculated_new
   end as new_AmtDisbursed
from updated_return_disc_calc t8
left join count_check t9
   on t8.scheme_code=t9.scheme_code
   and t8.invoice_number=t9.invoice_number
   and t8.shipdate=t9.shipdate
''')
discount_condition.createOrReplaceTempView("discount_condition")

# COMMAND ----------

# DBTITLE 1,test df
# # Create test DataFrame by joining discount calculation with Group Count and Variance checks.
# # If either Group Count Check or Variance Check fails, set new_AmtDisbursed = 0;
# # otherwise retain the calculated discount.
# discount_test = spark.sql(f'''
# select
#    t8.*, v.*,
#    case
#        when t9.group_count_check = 0 then 0
#        when v.variance_check = 0 then 0
#        else invcXscheme_lvl_Amount_disbursed_Calculated_new
#    end as new_AmtDisbursed
# from updated_return_disc_calc t8
# left join count_check t9
#    on t8.scheme_code=t9.scheme_code
#    and t8.invoice_number=t9.invoice_number
#    and t8.shipdate=t9.shipdate
# left join variance_check_final v
#    on t8.scheme_code=v.scheme_code
#    and t8.invoice_number=v.invoice_number
#    and t8.shipdate=v.shipdate
# ''')
# discount_test.createOrReplaceTempView("discount_test")

# COMMAND ----------

# DBTITLE 1,test df - variacne change
# Create test DataFrame by joining discount calculation with Group Count and Variance checks.
# If either Group Count Check or Variance Check fails, set new_AmtDisbursed = 0;
# otherwise retain the calculated discount.
discount_test = spark.sql(f'''
select
   t8.*,
   -- t8.*, v.*,
   case
       when t9.group_count_check = 0 then 0
       else invcXscheme_lvl_Amount_disbursed_Calculated_new
   end as new_AmtDisbursed
from updated_return_disc_calc t8
left join count_check t9
   on t8.scheme_code=t9.scheme_code
   and t8.invoice_number=t9.invoice_number
   and t8.shipdate=t9.shipdate
''')
discount_test.createOrReplaceTempView("discount_test")

# COMMAND ----------

# DBTITLE 1,Variance check
# Validation output for Variance Check.
# Shows invoices where Group Count Check passed but Variance Check failed,
# resulting in scheme ineligibility under the new business rule.

# display(
#     discount_test.filter(
#         (col("group_count_check") != 0) &
#         (col("variance_check")==0) &
#         (col("invcXscheme_lvl_Amount_disbursed_Calculated_new") != 0)
#     )
# )

# COMMAND ----------

# %sql
# SELECT *
# FROM sd_science.trade_plan_dtls_master
# WHERE Scheme_Code = 'LTR2601N00003635'; 

# COMMAND ----------

# display(intermediate_modified_of_return.filter((col('invoice_number')=='KAHRD-25-I042733')&(col('scheme_code')=='LTR2601N00003635')))

# COMMAND ----------

# display(discount_condition.filter((col('invoice_number')=='CGBHH-25-I048952')&(col('scheme_code')=='LTR2601N00003581')))

# COMMAND ----------

# DBTITLE 1,disallowance calculation
promotion_slab_check_aggregated=spark.sql(f'''
  select invoice_number,return_invoice_number,ShipDate,SRN_Date,
    scheme_code,amountdisbursed_orginal,
    amountdisbursed_retrun1 as amountdisbursed_retrun,
    amountdisbursed_calc_return,new_Amtdisbursed,new_Amtdisbursed_capped,
    "{formatted_date}" as RptMonthYear,
    amountdisbursed_orginal+amountdisbursed_retrun1-new_Amtdisbursed_capped as disallowance,
    case when amountdisbursed_orginal+amountdisbursed_retrun1=0
      then "Full Return" else "Partial Return"
    end as Remarks_SRN
  from (
    select t8.*,
      case when t9.slab_max_limit is null then new_Amtdisbursed
           when t9.slab_max_limit<new_Amtdisbursed then t9.slab_max_limit
           else new_Amtdisbursed
      end as new_Amtdisbursed_capped
    from discount_condition t8
    left join (select distinct scheme_code,slab_max_limit from Master_sdf) t9
    on t8.scheme_code=t9.scheme_code
  ) where return_invoice_number is not null
''')
promotion_slab_check_aggregated.createOrReplaceTempView("promotion_slab_check_aggregated")

# COMMAND ----------

# display(promotion_slab_check_aggregated.filter((col('invoice_number')=='CGVIR-25-I134637')&(col('scheme_code')=='LTR2601N00003617'))) 

# COMMAND ----------

# display(promotion_slab_check_aggregated.filter((col('invoice_number')=='CGVIR-25-I132684')&(col('scheme_code')=='LSS2601N1638_BC2')))

# Coming for IHR:
#amountdisbursed_orginal
#amountdisbursed_retrun
#Calculated:
#amountdisbursed_calc_return
#new_Amtdisbursed
#new_Amtdisbursed_capped -- After capping calculated final amunttobedisbursed according to our logics
# disallowance  -- amountdisbursed_orginal + amountdisbursed_retrun - new_Amtdisbursed_capped
# latest change for damage -- we made change in IHR return (amountdisbursed_retrun)
# We excluded damaged return from our disallwance calculation while adjusting for returns we are not considering damaged returns  

# COMMAND ----------

# %sql
# select * from  promotion_slab_check_aggregated where invoice_number="CGDOM-25-I096691" --LSS2511N0582_BC1

# COMMAND ----------

# DBTITLE 1,retailer master
# retailer_master=spark.sql(f'''
#                           with dedup(
#                           select distinct rtrcode,DistCode,ChnlName,SubChannelName,BranchCode, row_number() over(partition by rtrcode order by loadDate desc) as rn from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_retailermaster where RptMonthYear='{formatted_date}')
#                           select * from dedup where rn=1
#                           ''')
# retailer_master.createOrReplaceTempView("retailer_master")

# COMMAND ----------

formatted_date

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- in first fortnight
# MAGIC -- multibrand
# MAGIC -- cs not approved scheme then not in calcutaltion
# MAGIC -- _MRI

# COMMAND ----------

spark.sql(f'''
         delete from claims_mgmt.srn_calcs where rptmonthyear like "{formatted_date}"
        ''').show()

# COMMAND ----------

# DBTITLE 1,check for full return has no disallwoance and partial with less than 0 is 0
# promotion_slab_check_aggregated = (
#     promotion_slab_check_aggregated
#     .withColumn(
#         'disallowance',
#         when(col('Remarks_SRN')=='Full Return',0)
#         .otherwise(col('disallowance'))
#     )
# )

from pyspark.sql.functions import col, when

promotion_slab_check_aggregated = (
    promotion_slab_check_aggregated
    .withColumn(
        "disallowance",
        when(col("Remarks_SRN") == "Full Return", 0)
        .when(
            (col("Remarks_SRN") == "Partial Return") & (col("disallowance") < 0),
            0
        )
        .otherwise(col("disallowance"))
    )
)

# COMMAND ----------

promotion_slab_check_aggregated.write.mode("append").saveAsTable("claims_mgmt.srn_calcs")

# COMMAND ----------

# %sql
# select 
# -- *
# sum(disallowance) 
# from claims_mgmt.srn_calcs where RptMonthYear = '202601'
# -- and ShipDate >='2026-02-01' and ShipDate<= '2026-02-14'

# COMMAND ----------

df = spark.sql("""
        SELECT SUM(disallowance) AS total_disallowance
        FROM claims_mgmt.srn_calcs
        WHERE RptMonthYear = '202602'
          AND (scheme_code IN ('LSS2602N3033_BC2_L1','LSS2602N3033_BC2_L3', 'LSS2602N3034_BC2_L1','LSS2602N3034_BC2_L3', 'LTR2602N00000011','LTR2602N00000012','LTT2602N00001067')
               OR scheme_code LIKE '%MRI')
    """)
df.display()

# COMMAND ----------

if str(formatted_date) == '202602':

    spark.sql("""
            UPDATE claims_mgmt.srn_calcs
    SET disallowance = 0.0
    WHERE RptMonthYear = '202602'
    AND (
            scheme_code LIKE '%MRI'
            OR scheme_code IN (
                'LSS2602N3033_BC2_L1',
                'LSS2602N3033_BC2_L3',
                'LSS2602N3034_BC2_L1',
                'LSS2602N3034_BC2_L3',
                'LSS2602N4785_BC2_L1',
                'LSS2602N4785_BC2_L3',
                'LSS2602N3029_BC2_L1',
                'LSS2602N3029_BC2_L3',
                'LSS2602N3030_BC2_L1',
                'LSS2602N3030_BC2_L3',
                'LSS2602N3023_BC2_L1',
                'LSS2602N3023_BC2_L3',
                'LSS2602N3024_BC2_L1',
                'LSS2602N3024_BC2_L3',
                'LSS2602N4780_BC2_L1',
                'LSS2602N4780_BC2_L3',
                'LSS2602N3017_BC2_L1',
                'LSS2602N3017_BC2_L3',
                'LSS2602N3018_BC2_L1',
                'LSS2602N3018_BC2_L3',
                'LSS2602N3019_BC2_L1',
                'LSS2602N3019_BC2_L3',
                'LSS2602N3020_BC2_L1',
                'LSS2602N3020_BC2_L3',
                'LSS2602N3021_BC2_L1',
                'LSS2602N3021_BC2_L3',
                'LSS2602N3022_BC2_L1',
                'LSS2602N3022_BC2_L3',
                'LSS2602N4777_BC2_L1',
                'LSS2602N4777_BC2_L3',
                'LSS2602N4778_BC2_L1',
                'LSS2602N4778_BC2_L3',
                'LSS2602N4779_BC2_L1',
                'LSS2602N4779_BC2_L3',
                'LSS2602N3031_BC2_L1',
                'LSS2602N3031_BC2_L3',
                'LSS2602N3032_BC2_L1',
                'LSS2602N3032_BC2_L3',
                'LSS2602N4784_BC2_L1',
                'LSS2602N4784_BC2_L3',
                'LSS2602N3027_BC2_L1',
                'LSS2602N3027_BC2_L3',
                'LSS2602N3028_BC2_L1',
                'LSS2602N3028_BC2_L3',
                'LSS2602N4782_BC2_L1',
                'LSS2602N4782_BC2_L3',
                'LSS2602N4792_BC2',
                'LSS2602N4792_BC2_NC',
                'LSS2602N3025_BC2_L1',
                'LSS2602N3025_BC2_L3',
                'LSS2602N3026_BC2_L1',
                'LSS2602N3026_BC2_L3',
                'LSS2602N4781_BC2_L1',
                'LSS2602N4781_BC2_L3',
                'LSS2602N4791_BC2',
                'LTR2602N00000011',
                'LTR2602N00000012',
                'LTR2602N00000021',
                'LTR2602N00000022',
                'LTR2602N00004612',
                'LTR2602N00004993',
                'LTR2602N00004994',
                'LTR2601T00003118',
                'LTR2602N00004990',
                'LTT2602N00001067',
                'LTT2511N00003804'
            )
        )
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Made change below with partial return negative is bounded to 0

# COMMAND ----------

# display(
#   promotion_slab_check_aggregated.filter(
#       (col("scheme_code")=="LTR2601N00003635")&
#       (col("invoice_number")== "KAHRD-25-I042733")
#   )
# )

# COMMAND ----------


#dbutils.notebook.exit("Success")

# COMMAND ----------

# %sql
# drop table claims_mgmt.srn_calcs

# COMMAND ----------

# DBTITLE 1,excel file reading
# df = spark.read.format("com.crealytics.spark.excel") \
#     .option("header", "true") \
#     .load('dbfs:/mnt/datahub/Adhoc Inputs/ihr/ihr_spent_sbf/return_nov25.xlsx')
# df.createOrReplaceTempView("df")

# COMMAND ----------

# %sql
# select distinct t103.*,t104.date from (select t101.* from 
# (select distinct ApplyToDocNum,DocNumber from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails where DocNumber in (select salesinvoiceno from df))t101 left join df t102 on t101.docnumber=t102.salesinvoiceno)t103 left join (select distinct DocNumber,ApplyToDocNum, Date from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails) t104 on t104.docnumber=t103.ApplyToDocNum 