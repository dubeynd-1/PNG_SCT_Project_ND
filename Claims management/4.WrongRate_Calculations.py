# Databricks notebook source
# DBTITLE 1,import required libraries
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

# DBTITLE 1,Reading start date and end date
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

# DBTITLE 1,Product master
# MAGIC %skip
# MAGIC prod_master=spark.sql('Select productCode,CategoryName,BrandName,BrandformName,SubbfName from cdl_india_data_prod.india_sdm_refined.productmaster')
# MAGIC prod_master.createOrReplaceTempView("prod_master")

# COMMAND ----------

# %sql
# select * from sd_science.trade_plan_dtls_mapping_v1 t6 where  exists (select  1  from cs_approved_schemes t5 where cs_flag="CS Approved" and t5.initiativecode=t6.scheme_code)

# COMMAND ----------

# DBTITLE 1,Reading IHR and PSR data
psr_new=spark.sql(f'''Select InvCode as invoice_number,RptMonthYear,InvDate as time_period_end_date,
ProductCode,Primarybranchcode as Branch_code,Retailer_Code as retailer_code,
upper(Channel_Org) as channel_name,
Quantity_Org as item_unit_qty,Line_Gross_Value as gross_transact_amt,
Line_Tax_On_RLP_Val as tax_amt,
Line_Retailing_Val as net_transact_amt
 from cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails 
 where InvDate >= '{start_date_value}' and InvDate <= '{end_date_value}'--RptMonthYear='{formatted_date}' 
 ''')
psr_new.createOrReplaceTempView("psr_new")
psr_modified=spark.sql(f'''
Select s.*,t.SubbfCode,t.SubbfName,
p.CategoryName as Category,p.BrandName as Brand,p.BrandformName as BrandForm from psr_new s
left join cdl_india_data_prod.india_sdm_refined.productmaster p
on s.ProductCode=p.ProductCode
left join (select distinct DocNumber,Pcode,SubbfCode,SubbfName from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails where date >= '{start_date_value}' and date <= '{end_date_value}'
 ) t
on trim(s.ProductCode)=trim(t.Pcode) 
and s.invoice_number=t.DocNumber''')
psr_modified.createOrReplaceTempView("psr_modified")
ihr=spark.sql(f'''
Select salesInvoiceNo,ShipDate,DistCode,retailercode,InitiativeCode,channel,CAST(amountdisbursed AS double) AS amountdisbursed,
branchcode from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and  InitiativeEndDate >= '{start_date_value}' ''')
ihr.createOrReplaceTempView("ihr")

# COMMAND ----------

# DBTITLE 1,Reading Master Files
Master_sdf=spark.sql(f'''select * from sd_science.trade_plan_dtls_master where Scheme_Code in (select distinct InitiativeCode from ihr) and left(scheme_code,4)!="RKWI" ''')
Master_sdf=Master_sdf.withColumn("Level_Desc",explode(split(col("subfNameList"),"\\|")))
Master_sdf=Master_sdf.drop("RptMonthYear","subfNameList","scheme_code_sd")
Master_sdf=Master_sdf.dropDuplicates()
mstr_req_cols=list(spark.read.table("stg.masters_auditex").columns)[:-1]
Master_sdf=Master_sdf.select(*mstr_req_cols)
Master_sdf.createOrReplaceTempView("Master_sdf1")
Master_sdf=spark.sql(''' Select * except (Initiative_Promotion_Group_Name) from stg.masters_auditex where left(scheme_code,4)!="RKWI" and scheme_code in (select InitiativeCode from ihr ) and scheme_code not in (select scheme_code from Master_sdf1) union select * from Master_sdf1 ''')
Master_sdf.createOrReplaceTempView("Master_sdf")

# COMMAND ----------

# DBTITLE 1,IHR PSR combined
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
    .groupBy("salesInvoiceNo","ShipDate","retailercode","InitiativeCode","channel","branchcode","invoice_number","RptMonthYear","time_period_end_date","ProductCode","Branch_code","retailer_code","channel_name","SubbfCode","SubbfName","Category","Brand","BrandForm")\
    .agg(F.first(F.col("amountdisbursed")).alias("amountdisbursed"),
         F.sum(F.col("item_unit_qty")).alias("item_unit_qty"),
         F.sum(F.col("gross_transact_amt")).alias("gross_transact_amt"),
         F.sum(F.col("tax_amt")).alias("tax_amt"),
         F.sum(F.col("net_transact_amt")).alias("net_transact_amt")) 
ihr_psr_aggregated.createOrReplaceTempView("ihr_psr_aggregated")

# COMMAND ----------

# display(ihr_psr_aggregated.filter((col('invoice_number')=='NAJND-26-I005100')&(col('initiativecode')=='LTR202605TP342ABD')))

# COMMAND ----------

Master_retailer=Master_sdf
Master_retailer.createOrReplaceTempView("Master_retailer")

# COMMAND ----------

### check for schemes with two promotion group levels and each at differet product granularity
group_counts=Master_retailer.groupBy("Scheme_code").agg(F.countDistinct("Promotion_Group_Name").alias("promotion_group_count"),
                                                        F.countDistinct("Level_Type").alias("level_count"))\
                            .withColumn("promotion_group_level_count",F.when((col("promotion_group_count")>1) & (col("level_count")>1),lit(1)).otherwise(lit(0)))
group_counts.createOrReplaceTempView("group_counts")
master_sales_values=Master_retailer.join(group_counts,on="Scheme_code",how="left")\
    .withColumnRenamed("RetailerApplyCount","Retailer_Apply_Count")
master_sales_values.createOrReplaceTempView("master_sales_values")

# COMMAND ----------

# display(master_sales_values.filter(col('scheme_code')=='LTR202605TP342ABD'))

# COMMAND ----------

# DBTITLE 1,Filter out invalid transactions at scheme code X sbf level
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
"ShipDate","RptMonthYear","invoice_number") \
    .agg(F.first(F.col("amountdisbursed")).alias("amountdisbursed"),
         F.sum(F.col("item_unit_qty")).alias("item_unit_qty"),
         F.sum(F.col("gross_transact_amt")).alias("gross_transact_amt"),
         F.sum(F.col("tax_amt")).alias("tax_amt"),
         F.sum(F.col("net_transact_amt")).alias("net_transact_amt")) .select("scheme_code","scheme_type","valid_from","valid_to","Scheme_Buy_Logic","Promotion_Group_Name","promotion_group_count","promotion_group_level_count","Level_Type","Promotion_Group_Logic","Promotion_Slab_Description","Promotion_Slab_Buy_Min","Promotion_Slab_Buy_Max","Promotion_Get_Type",
"Promotion_Slab_Get_Discount","channel","branchcode","retailer_code","Category","Brand","BrandForm","i.SubbfName",
"ShipDate","RptMonthYear","invoice_number","item_unit_qty","gross_transact_amt","tax_amt","net_transact_amt","amountdisbursed")\
.filter(col("invoice_number").isNotNull())

intermediate.createOrReplaceTempView("intermediate")

# COMMAND ----------

# %sql
# select * from intermediate where scheme_code like '%MRI'

# COMMAND ----------

# display(intermediate.filter((col('invoice_number')=='NAJND-26-I005100')&(col('scheme_code')=='LTR202605TP342ABD')))

# COMMAND ----------

current_group_counts=intermediate.groupBy("Scheme_code","invoice_number","ShipDate").agg(F.countDistinct("Promotion_Group_Name").alias("current_group_count"))
count_check=current_group_counts.join(group_counts,on="Scheme_code",how="left")\
    .withColumn("group_count_check",F.when(col("current_group_count")<col("promotion_group_count"),0).otherwise(1))
count_check.createOrReplaceTempView("count_check")

# COMMAND ----------

# %sql
# select * from count_check where Scheme_code like '%MRI'

# COMMAND ----------

#since each prmotion group can have multiple slabs and returns multiple rows when joined with master hence taking only one row per promotion group in order to dedup (in cte invoice_sd_ret_agg)

intermediate_modified=spark.sql('''
                                
with invoice_sd_ret_agg as
(
  select  scheme_code,invoice_number,Promotion_Group_Name,sum(net_transact_amt) as net_transact_amt_sum,sum(item_unit_qty) as item_unit_qty_sum from ( select SubbfName, scheme_code,invoice_number,Promotion_Group_Name,net_transact_amt,item_unit_qty,row_number() over(partition by scheme_code,invoice_number,Promotion_Group_Name,SubbfName order by Promotion_Slab_Description) as rnk from intermediate ) a where rnk=1 group by scheme_code,invoice_number,Promotion_Group_Name

),
discount_calc_prep as
(
select t1.*,case when upper(scheme_type)="QUANTITY" then abs(item_unit_qty_sum) else abs(net_transact_amt_sum) end as condition from intermediate t1 join  invoice_sd_ret_agg t2 on t1.invoice_number=t2.invoice_number and t1.scheme_code=t2.scheme_code and t1.Promotion_Group_Name=t2.Promotion_Group_Name 
),
dedup as (
select * from discount_calc_prep where condition>=Promotion_Slab_Buy_Min and condition<=Promotion_Slab_Buy_Max
),
dedup_disc_calc as 
(
select * ,sum(gross_transact_amt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as gross_transact_amt_sum_1, sum(item_unit_qty) over(partition By scheme_code,invoice_number,Promotion_Group_Name) as item_unit_qty_sum_1 , sum(net_transact_amt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as net_transact_amt_sum_1, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty*Promotion_Slab_Get_Discount else (gross_transact_amt*Promotion_Slab_Get_Discount)/100 end as amtdisbursed_calculated_at_sbf_lvl_1, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty_sum_1*Promotion_Slab_Get_Discount else (gross_transact_amt_sum_1*Promotion_Slab_Get_Discount)/100 end as invcXscheme_lvl_Amount_disbursed_Calculated_1 from dedup
)
select * from dedup_disc_calc
          ''')
intermediate_modified.createOrReplaceTempView("intermediate_modified")

# COMMAND ----------

# display(intermediate_modified.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('scheme_code')=='LDM2512N7949')))

# COMMAND ----------

intermediate_modified1=spark.sql('''
with exclude_samesbf_from_mutliple_groups as 
(
select *, row_number() over(partition by invoice_number,scheme_code,SubbfName order by invcXscheme_lvl_Amount_disbursed_Calculated_1 desc) as rnk from intermediate_modified 
),
dedup1 as (
select * except(rnk) from exclude_samesbf_from_mutliple_groups where rnk=1
)
select *except(gross_transact_amt_sum_1,item_unit_qty_sum_1,net_transact_amt_sum_1,amtdisbursed_calculated_at_sbf_lvl_1,invcXscheme_lvl_Amount_disbursed_Calculated_1) ,sum(gross_transact_amt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as gross_transact_amt_agg, sum(item_unit_qty) over(partition By scheme_code,invoice_number,Promotion_Group_Name) as item_unit_qty_agg , sum(net_transact_amt) over(partition by scheme_code,invoice_number,Promotion_Group_Name ) as net_transact_amt_agg, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty*Promotion_Slab_Get_Discount else (gross_transact_amt*Promotion_Slab_Get_Discount)/100 end as amtdisbursed_calculated_at_sbf_lvl, case when lower(Promotion_Get_Type)="discount every" then item_unit_qty_agg*Promotion_Slab_Get_Discount else (gross_transact_amt_agg*Promotion_Slab_Get_Discount)/100 end as invcXscheme_lvl_Amount_disbursed_Calculated from dedup1


''')
intermediate_modified1.createOrReplaceTempView("intermediate_modified1")

# COMMAND ----------

# %sql
# select * from intermediate_modified1 where scheme_code like '%MRI'

# COMMAND ----------

# MAGIC %skip
# MAGIC display(intermediate_modified1.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('scheme_code')=='LDM2512N7949')))

# COMMAND ----------

# display(intermediate_modified1.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('scheme_code')=='LDM2512N7949')))

# COMMAND ----------

scheme_count=spark.sql(f'''
Select InitiativeCode,retailercode,count(distinct ShipDate) as scheme_count from 
(Select * from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where ShipDate <= '{end_date_value}' and ShipDate >='{month_start}' and amountdisbursed>0)
group by InitiativeCode,retailercode
  ''')
scheme_count.createOrReplaceTempView("scheme_count")


master_scheme_count=master_sales_values.dropDuplicates(["scheme_code"]).join(scheme_count,(scheme_count["InitiativeCode"]==master_sales_values["scheme_code"]),how="left")\
    .withColumn("Retailer_count_check",F.when(col("Retailer_Apply_Count")==0,lit(0)).otherwise(F.when(col("scheme_count")>col("Retailer_Apply_Count"),lit(1)).otherwise(0))).filter(col("Retailer_Apply_Count")>0).select("Scheme_Code","Retailer_Apply_Count","retailercode","Retailer_count_check")
master_scheme_count.createOrReplaceTempView("master_scheme_count")

# COMMAND ----------

# display(master_scheme_count.filter(col('scheme_code')=='LDM2512N7949'))

# COMMAND ----------

discount_condition = intermediate_modified1.join(master_scheme_count, (intermediate_modified1["Scheme_Code"]==master_scheme_count["Scheme_Code"]) & (intermediate_modified1["retailer_code"]==master_scheme_count["retailercode"]),how="left")\
    .select(intermediate_modified1["*"],
            master_scheme_count["Retailer_Apply_Count"])\
    .groupby("Scheme_code","ShipDate","invoice_number","scheme_type","Promotion_Group_Name","retailer_code").agg(
                                                            F.first(F.col("invcXscheme_lvl_Amount_disbursed_Calculated")).alias("invcXscheme_lvl_Amount_disbursed_Calculated1"),
                                                            F.first(F.col("amountdisbursed")).alias("amountdisbursed"),
                                                            F.first(F.col("Retailer_Apply_Count")).alias("Retailer_Apply_Count"))
                                                            
discount_condition.createOrReplaceTempView("discount_condition")

# COMMAND ----------

# %sql
# select * from discount_condition
# where 
# -- invoice_number='CGWAG-25-I010183'
# -- and
# scheme_code like '%MRI'
# -- ='LDM2512N7949'

# COMMAND ----------

# display(discount_condition.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('scheme_code')=='LDM2512N7949')))

# COMMAND ----------

# discount_condition = intermediate_modified1.join(master_scheme_count, (intermediate_modified1["Scheme_Code"]==master_scheme_count["Scheme_Code"]) & (intermediate_modified1["retailer_code"]==master_scheme_count["retailercode"]),how="left")\
#     .select(intermediate_modified1["*"],
#             master_scheme_count["Retailer_count_check"])\
#     .withColumn("invcXscheme_lvl_Amount_disbursed_Calculated",F.when(col("Retailer_count_check")==1,lit(0)).otherwise(col("invcXscheme_lvl_Amount_disbursed_Calculated")))\
#     .withColumn("amtdisbursed_calculated_at_sbf_lvl",F.when(col("Retailer_count_check")==1,lit(0)).otherwise(col("amtdisbursed_calculated_at_sbf_lvl"))) \
#     .groupby("Scheme_code","ShipDate","invoice_number","scheme_type","Promotion_Group_Name").agg(
#                                                             F.first(F.col("invcXscheme_lvl_Amount_disbursed_Calculated")).alias("invcXscheme_lvl_Amount_disbursed_Calculated1"),
#                                                             F.first(F.col("amountdisbursed")).alias("amountdisbursed"),
#                                                             F.first(F.col("Retailer_count_check")).alias("Retailer_count_check"))
                                                            
# discount_condition.createOrReplaceTempView("discount_condition")

# COMMAND ----------

promotion_slab_check_aggregated=discount_condition\
    .groupBy("scheme_code","invoice_number","ShipDate","retailer_Code")\
        .agg(F.sum("invcXscheme_lvl_Amount_disbursed_Calculated1").alias("Amnt_disbursed_calculated"),
            F.first("amountdisbursed").alias("amountdisbursed"),
                                      F.first(F.col("Retailer_Apply_Count")).alias("Retailer_Apply_Count"))\
          .withColumn("Amnt_disbursed_calculated", col("Amnt_disbursed_calculated"))\
                .withColumn("amountdisbursed", col("amountdisbursed"))
promotion_slab_check_aggregated.createOrReplaceTempView("promotion_slab_check_aggregated")

# COMMAND ----------

# %sql
# select sum(Amnt_disbursed_calculated) from promotion_slab_check_aggregated where scheme_code like '%MRI'

# COMMAND ----------

# display(promotion_slab_check_aggregated.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('scheme_code')=='LDM2512N7949')))

# COMMAND ----------

multi_group_level_check=promotion_slab_check_aggregated\
    .join(count_check,on=["Scheme_code","invoice_number","ShipDate"],how="left")\
        .withColumn("Amnt_disbursed_calculated1",F.when(col("group_count_check")==0,lit(0)).otherwise(col("Amnt_disbursed_calculated")))\
            .withColumn("retailer_apply_count_ihr",row_number().over(Window.partitionBy("Scheme_code","retailer_Code").orderBy("ShipDate") ))\
                .withColumn("Retailer_count_check",F.when(col("retailer_apply_count_ihr")>col("Retailer_Apply_Count"),lit(1)).otherwise(0))\
                .withColumn("Amnt_disbursed_calculated1",F.when(col("Retailer_count_check")==1,lit(0)).otherwise(col("Amnt_disbursed_calculated")))\
                    .withColumnRenamed("Scheme_code","InitiativeCode")
multi_group_level_check.createOrReplaceTempView("multi_group_level_check")

# COMMAND ----------

# %sql
# select sum(Amnt_disbursed_calculated) from multi_group_level_check where InitiativeCode like '%MRI' and ShipDate>'2026-01-14'

# COMMAND ----------

master_sales_values=master_sales_values.withColumnRenamed("RetailerApplyCount","retailer_apply_count")
master_sales_values.createOrReplaceTempView("master_sales_values")

# COMMAND ----------

master_sales_value_retailer_count=master_sales_values.groupBy("scheme_code").agg(F.first(col("retailer_apply_count")).alias("retailer_apply_count"),
F.first("Slab_Max_Limit").alias("Slab_Max_Limit")).withColumnRenamed("Scheme_code","InitiativeCode")
master_sales_value_retailer_count.createOrReplaceTempView("master_sales_value_retailer_count")

# COMMAND ----------

slab_max_level_check=multi_group_level_check.join(master_sales_value_retailer_count,on=["InitiativeCode"],how="left")
slab_max_level_check=slab_max_level_check \
        .withColumn("Comments",F.when(col("Slab_Max_Limit").isNotNull() & (col("Slab_Max_Limit")<col("Amnt_disbursed_calculated1")),lit("Slab max limit applied")).otherwise(lit("NA"))) \
                .withColumn("Amnt_disbursed_calculated2",F.when(F.isnan(col("Slab_Max_Limit")),col("Amnt_disbursed_calculated1")).otherwise(F.when(col("Slab_Max_Limit")<col("Amnt_disbursed_calculated1"),col("Slab_Max_Limit")).otherwise(col("Amnt_disbursed_calculated1")))) \
                .withColumn("diff",F.abs(col("Amnt_disbursed_calculated2")-col("amountdisbursed")))
                
            
slab_max_level_check.createOrReplaceTempView("slab_max_level_check")

# COMMAND ----------

# display(slab_max_level_check.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('InitiativeCode')=='LDM2512N7949')))

# COMMAND ----------

# display(slab_max_level_check.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('InitiativeCode')=='LDM2512N7949')))

# COMMAND ----------

# %sql
# select * from slab_max_level_check
# where invoice_number='CGWAG-25-I010183' and initiativecode='LDM2512N7949'

# COMMAND ----------

# %sql
# -- select * from slab_max_level_check
# where invoice_number='CGWAG-25-I010183' and initiativecode='LDM2512N7949'
# and diff>0.2

# COMMAND ----------

# %sql
# with cte as (select *, diff as calculated from slab_max_level_check
#                               )
# select InitiativeCode,invoice_number,ShipDate,Amnt_disbursed_calculated2 as Amnt_disbursed_calamountdisbursed, calculated,
# case when Retailer_count_check=1 then "Retailer Apply count issue" when group_count_check=0 then "groups not satisfied" else comments end as comments
# from cte where diff>0.2
# and invoice_number='CGWAG-25-I010183' and initiativecode='LDM2512N7949'

# COMMAND ----------

mri_schemes = [
    "LTR2601N00003096_MRI",
    "LTR2601N00003103_MRI",
    "LTR2601N00003114_MRI",
    "LTR2601N00003120_MRI",
    "LTR2601N00003133_MRI",
    "LTR2601N00003142_MRI",
    "LTR2601N00003145_MRI",
    "LTR2601N00003156_MRI",
    "LTR2601N00003157_MRI",
    "LTR2601N00003158_MRI",
    "LTR2601N00003161_MRI",
    "LTR2601N00003166_MRI",
    "LTR2601N00003179_MRI",
    "LTR2601N00003270_MRI",
    "LTR2601N00003274_MRI",
    "LTR2601N00003284_MRI",
    "LTR2601N00003303_MRI",
    "LTR2601N00003305_MRI",
    "LTR2601N00003311_MRI",
    "LTR2601N00003321_MRI",
    "LTR2601N00003512_MRI",
    "LTR2601N00003521_MRI",
    "LTR2601N00003523_MRI",
    "LTR2601N00003524_MRI",
    "LTR2601N00003536_MRI",
    "LTR2601N00003538_MRI",
    "LTR2601N00003541_MRI",
    "LTR2601N00003712_MRI",
    "LTR2601N00003713_MRI",
    "LTR2601N00003730_MRI",
    "LTR2601N00003731_MRI",
    "LTR2601N00003732_MRI"
]

mri_schemes_sql = ",".join(f"'{x}'" for x in mri_schemes)
# mri_schemes_sql

# COMMAND ----------

# DBTITLE 1,amount_disbursed_df-original
if str(end_date_value)[-2:] == "14":
    amount_disbursed_df=spark.sql(f'''
                                with cte as (
                                    select *, diff as calculated from slab_max_level_check
                                )
                                select InitiativeCode,invoice_number,ShipDate,Amnt_disbursed_calculated2 as Amnt_disbursed_calculated,amountdisbursed, calculated,
                                case when Retailer_count_check=1 then "Retailer Apply count issue" when group_count_check=0 then "All groups not satisfied" else comments end as comments
                                from cte where diff>0.2
                                ''')
    amount_disbursed_df.createOrReplaceTempView("amount_disbursed_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC #### amount_disbursed_df changed for MRI full monthly

# COMMAND ----------

if str(end_date_value)[-2:] != "14":
    amount_disbursed_df = spark.sql(f"""
        WITH cte AS (
            SELECT *,
                diff AS calculated
            FROM slab_max_level_check
        )
        SELECT
            InitiativeCode,
            invoice_number,
            ShipDate,
            CASE
                WHEN InitiativeCode IN ({mri_schemes_sql})
                    THEN CAST(0.00 AS DOUBLE)
                ELSE Amnt_disbursed_calculated2
            END AS Amnt_disbursed_calculated,
            -- CASE
            --     WHEN InitiativeCode IN ({mri_schemes_sql})
            --         THEN CAST(0.00 AS DOUBLE)
            --     ELSE amountdisbursed
            -- END AS amountdisbursed,
            amountdisbursed,
            calculated,
            CASE
                WHEN InitiativeCode IN ({mri_schemes_sql})
                    THEN comments
                WHEN Retailer_count_check = 1
                    THEN 'Retailer Apply count issue'
                WHEN group_count_check = 0
                    THEN 'All groups not satisfied'
                ELSE comments
            END AS comments
        FROM cte
        WHERE
            (
                InitiativeCode IN ({mri_schemes_sql})
                AND day(ShipDate) > 14
            )
            OR
            (
                InitiativeCode NOT IN ({mri_schemes_sql})
                AND diff > 0.2
            )
    """)

    amount_disbursed_df.createOrReplaceTempView("amount_disbursed_vw")

# COMMAND ----------

# display(amount_disbursed_df.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('InitiativeCode')=='LDM2512N7949')))

# COMMAND ----------

# %sql
# select * from claims_mgmt.worng_rate_calcs limit 2

# COMMAND ----------

# DBTITLE 1,Removing wrong rate for Free goods
# Removing wrong rate for free goods (LFG schemes) because that is calculated separately
amount_disbursed_vw_upd = (
    amount_disbursed_df
    .withColumn('Amnt_disbursed_calculated',
                when(upper(left(col('initiativecode'),lit(3)))=="LFG",col('amountdisbursed'))
                .otherwise(col('Amnt_disbursed_calculated')))
    .withColumn('calculated',
                when(upper(left(col('initiativecode'),lit(3)))=="LFG",lit(0))
                .otherwise(col('calculated')))
)
amount_disbursed_vw_upd.createOrReplaceTempView('amount_disbursed_vw_upd')

# COMMAND ----------

# display(amount_disbursed_vw_upd.filter((col('invoice_number')=='CGWAG-25-I010183')&(col('InitiativeCode')=='LDM2512N7949')))

# COMMAND ----------

# %sql
# select * from claims_mgmt.worng_rate_calcs
# where invoice_number='CGWAG-25-I010183' and InitiativeCode='LDM2512N7949'

# COMMAND ----------

# %sql
# select * from claims_mgmt.worng_rate_calcs
# where RptMonthYear='202511'
# -- and upper(left(initiativecode,3))="LFG"
# and Amnt_disbursed_calculated>amountdisbursed

# COMMAND ----------

# %sql
# select * from claims_mgmt.worng_rate_calcs
# where RptMonthYear='202601'
# -- and upper(left(initiativecode,3))="LFG"
# and Amnt_disbursed_calculated>amountdisbursed

# COMMAND ----------

df=spark.sql(f'''
          select *, "{formatted_date}" as RptMonthYear from amount_disbursed_vw_upd 
          ''')

# COMMAND ----------

spark.sql(f''' delete from claims_mgmt.worng_rate_calcs where rptmonthyear="{formatted_date}" ''').display()

# COMMAND ----------

# df=spark.sql(f'''
#           select *, "{formatted_date}" as RptMonthYear from amount_disbursed_vw_upd --where InitiativeCode in (select InitiativeCode from cs_approved_schemes where Cs_flag="CS Approved") 
#           ''')
# spark.sql(f''' delete from claims_mgmt.worng_rate_calcs where rptmonthyear="{formatted_date}" ''').display()
df.write.mode("append").saveAsTable("claims_mgmt.worng_rate_calcs")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

display(spark.sql(f"""select * from claims_mgmt.worng_rate_calcs where RptMonthYear={formatted_date} """))

# COMMAND ----------

# %sql
# describe history claims_mgmt.worng_rate_calcs

# COMMAND ----------

# DBTITLE 1,Date check
# ihr_psr_aggregated_modified=spark.sql('''Select distinct salesInvoiceNo,shipDate,InitiativeCode,amountdisbursed from ihr_psr_aggregated where amountdisbursed>0''')\
#     .withColumnRenamed("InitiativeCode","Scheme_Code")
# ihr_psr_aggregated_modified.createOrReplaceTempView("ihr_psr_aggregated_modified")