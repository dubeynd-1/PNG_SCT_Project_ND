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

# spark.conf.set("spark.sql.shuffle.partitions", 500)

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

# MAGIC %sql
# MAGIC select * from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr 

# COMMAND ----------

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
branchcode from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and  InitiativeEndDate >= '{start_date_value}'  ''')
ihr.createOrReplaceTempView("ihr")

# COMMAND ----------

# DBTITLE 1,CONSIDERATION CHECK
spark.sql(f'''select initiativecode,`description_used_y/n` as desc_flg, what_is_used as desc,matrix_check_fortnight,scheme_code_consideration_remark_1 from stg.scheme_master_conf''').createOrReplaceTempView("smt")

### filter schemes valid for this fortnight/month
spark.sql(f'''
          select /*+ Broadcast(t2) */
 distinct t1.initiativecode,matrix_check_fortnight,scheme_code_consideration_remark_1 from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr t1 join smt t2 on t1.InitiativeCode like concat(t2.initiativecode,"%")  where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' and (t2.desc_flg="No" OR (t2.desc_flg='Yes' and locate(upper(t2.desc),(upper(t1.initiativename)))>0 ))
          ''').createOrReplaceTempView("vs_list")

# COMMAND ----------

ihr_psr_chnls=spark.sql(f'''
                    select i.salesinvoiceno, i.InitiativeCode, i.DistCode, p.RtrCode AS Retailer_Code, coalesce(upper(i.channel),upper(p.ChnlName)) as ihr_Channel, coalesce(upper(r.Transaction_Customer_Type),upper(p.SubChannelName)) as psr_SubChannel,i.BranchCode,i.ShipDate 
                    from ihr i 
                    left join cdl_india_data_prod.india_distributordata_refined.tblrefinedview_salesdetails p on i.salesinvoiceno=p.ApplyToDocNum
                    left join cdl_india_data_prod.india_distributordata_refined.tblbasetrn_salesdetails r on i.salesinvoiceno = r.InvCode  
                    left join stg.indirect_ship_day_fct_new_touchless_promo sd on sd.invoice_number = i.salesinvoiceno
                    where i.ShipDate between '{start_date_value}' and '{end_date_value}' 
                    --and InitiativeEndDate >= '{start_date_value}' 
                    -- where i.InvCode in (select salesinvoiceno from ihr) and i.ShipDate between '{start_date_value}' and '{end_date_value}'
                    ''')
ihr_psr_chnls.createOrReplaceTempView("ihr_psr_chnls")

# COMMAND ----------

# %sql
# select * from ihr_psr_chnls where salesinvoiceno = 'SENGL-25-I061464' and InitiativeCode = 'LTR2601N00003925'

# COMMAND ----------

mapping = spark.sql(f'''
select distinct
    scheme_code,Distributor_code,Branch_code,retailer_Code,upper(channel_name) as mapping_channel,upper(type_name) as mapping_sub_channel,
    case 
        when retailer_code is not null and branch_code is not null and Distributor_Code is not null and upper(trim(scheme_code)) like 'LTR____N%' and
        substr(upper(trim(scheme_code)),1,16) in (select distinct substr(upper(trim(initcode)),1,16) from stg.cs_combined)
        -- where rptmonthyear = '{formatted_date}') 
        then 'Extracted schemes from retailer in CS'
        
        when Retailer_Code is null and branch_code is not null
        then 'Branch Level Scheme'
        
        when retailer_code is not null and branch_code is not null and Distributor_Code is not null
        then 'Retailer Level Scheme'
        
        when Retailer_Code is null and branch_code is null
        then 'Distributor Level Scheme'
    end as scheme_type

from sd_Science.trade_plan_Dtls_mapping_v1
where scheme_code in (select initiativecode from ihr_psr_chnls)
''')

mapping.createOrReplaceTempView("mapping")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dime_chk AS
# MAGIC SELECT
# MAGIC     d.INITCode AS initcode,
# MAGIC     MAX(CASE WHEN upper(trim(d.Customer_Type))='ALL CUSTOMER TYPES' THEN 1 ELSE 0 END) AS all_customer_type,
# MAGIC     collect_set(CASE WHEN upper(trim(d.Customer_Type))='ALL CUSTOMER TYPES'
# MAGIC                      THEN upper(trim(channel_raw)) END) AS allowed_channels,
# MAGIC     collect_set(CASE WHEN upper(trim(d.Customer_Type))<>'ALL CUSTOMER TYPES'
# MAGIC                      THEN upper(trim(sub_channel_raw)) END) AS allowed_subchannels
# MAGIC FROM claims_mgmt.dime_channel_summary d
# MAGIC LATERAL VIEW OUTER explode(split(d.Plan_For, ',')) ch AS channel_raw
# MAGIC LATERAL VIEW OUTER explode(split(d.Customer_Type, ',')) sc AS sub_channel_raw
# MAGIC WHERE d.INITCode IN (
# MAGIC     SELECT scheme_code FROM mapping WHERE scheme_type='Retailer Level Scheme'
# MAGIC )
# MAGIC AND d.Customer_Type <> '' AND d.Plan_For <> ''
# MAGIC GROUP BY d.INITCode

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW Retailer_lvl_check AS
WITH cs_rule AS (
    SELECT cs.initcode,
        MAX(CASE WHEN upper(trim(cs.customer_type))='ALL CUSTOMER TYPES' THEN 1 ELSE 0 END) AS all_customer_type,
        collect_set(CASE WHEN upper(trim(cs.customer_type))='ALL CUSTOMER TYPES' THEN upper(trim(channel_raw)) END) AS allowed_channels,
        collect_set(CASE WHEN upper(trim(cs.customer_type))<>'ALL CUSTOMER TYPES' THEN upper(trim(sub_channel_raw)) END) AS allowed_subchannels
    FROM stg.cs_combined cs
    LATERAL VIEW OUTER explode(split(cs.plan_for,',')) ch AS channel_raw
    LATERAL VIEW OUTER explode(split(cs.customer_type,',')) sc AS sub_channel_raw
    WHERE cs.initcode IN (SELECT scheme_code FROM mapping WHERE scheme_type='Retailer Level Scheme')
      AND cs.rptmonthyear={formatted_date}
    GROUP BY cs.initcode
),
dime_rule AS (
    SELECT d.INITCode AS initcode,
        MAX(CASE WHEN upper(trim(d.Customer_Type))='ALL CUSTOMER TYPES' THEN 1 ELSE 0 END) AS all_customer_type,
        collect_set(CASE WHEN upper(trim(d.Customer_Type))='ALL CUSTOMER TYPES' THEN upper(trim(channel_raw)) END) AS allowed_channels,
        collect_set(CASE WHEN upper(trim(d.Customer_Type))<>'ALL CUSTOMER TYPES' THEN upper(trim(sub_channel_raw)) END) AS allowed_subchannels
    FROM claims_mgmt.dime_channel_summary d
    LATERAL VIEW OUTER explode(split(d.Plan_For,',')) ch AS channel_raw
    LATERAL VIEW OUTER explode(split(d.Customer_Type,',')) sc AS sub_channel_raw
    WHERE d.INITCode IN (SELECT scheme_code FROM mapping WHERE scheme_type='Retailer Level Scheme')
      AND d.Customer_Type<>'' AND d.Plan_For<>'' AND d.RptMonthyear={formatted_date}
    GROUP BY d.INITCode
)
SELECT i.salesinvoiceno,i.InitiativeCode,i.DistCode,i.Retailer_Code,i.ihr_Channel,i.psr_SubChannel,i.BranchCode,
    coalesce(cs.allowed_channels,d.allowed_channels) AS mapping_cs_channel,
    coalesce(cs.allowed_subchannels,d.allowed_subchannels) AS mapping_cs_sub_channel,
    'RETAILER_LVL_SCHEMES_CHECK' AS Scheme_Type,'RTLR_ALL_CHECK' AS Scheme_Check,
    CASE
        WHEN cs.initcode IS NOT NULL AND cs.all_customer_type=1 AND i.ihr_Channel IS NULL THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN cs.initcode IS NOT NULL AND (
            array_contains(cs.allowed_channels,upper(trim(i.ihr_Channel))) OR
            array_contains(cs.allowed_subchannels,upper(trim(i.psr_SubChannel)))
        ) THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN d.initcode IS NOT NULL AND d.all_customer_type=1 AND i.ihr_Channel IS NULL THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN d.initcode IS NOT NULL AND (
            array_contains(d.allowed_channels,upper(trim(i.ihr_Channel))) OR
            array_contains(d.allowed_subchannels,upper(trim(i.psr_SubChannel)))
        ) THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN cs.initcode IS NOT NULL OR d.initcode IS NOT NULL THEN 'CHANNEL/SUB-CHANNEL MISMATCH'
        ELSE 'NOT IN DIME CS/MAPPING'
    END AS REMARK
FROM ihr_psr_chnls i
LEFT JOIN cs_rule cs ON upper(trim(i.InitiativeCode))=upper(trim(cs.initcode))
LEFT JOIN dime_rule d ON upper(trim(i.InitiativeCode))=upper(trim(d.initcode))
WHERE i.InitiativeCode IN (SELECT scheme_code FROM mapping WHERE scheme_type='Retailer Level Scheme')
""")

# COMMAND ----------

# DBTITLE 1,Channel_lvl_check - new with DIME post not in CS
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW Channel_lvl_check AS
WITH cs_rule AS (
    SELECT cs.initcode,
        MAX(CASE WHEN upper(trim(cs.customer_type))='ALL CUSTOMER TYPES' THEN 1 ELSE 0 END) AS all_customer_type,
        collect_set(CASE WHEN upper(trim(cs.customer_type))='ALL CUSTOMER TYPES' THEN upper(trim(channel_raw)) END) AS allowed_channels,
        collect_set(CASE WHEN upper(trim(cs.customer_type))<>'ALL CUSTOMER TYPES' THEN upper(trim(sub_channel_raw)) END) AS allowed_subchannels
    FROM stg.cs_combined cs
    LATERAL VIEW OUTER explode(split(cs.plan_for,',')) ch AS channel_raw
    LATERAL VIEW OUTER explode(split(cs.customer_type,',')) sc AS sub_channel_raw
    WHERE cs.initcode NOT IN (SELECT scheme_code FROM mapping WHERE scheme_type='Retailer Level Scheme')
      AND cs.rptmonthyear={formatted_date}
    GROUP BY cs.initcode
),
dime_rule AS (
    SELECT d.INITCode AS initcode,
        MAX(CASE WHEN upper(trim(d.Customer_Type))='ALL CUSTOMER TYPES' THEN 1 ELSE 0 END) AS all_customer_type,
        collect_set(CASE WHEN upper(trim(d.Customer_Type))='ALL CUSTOMER TYPES' THEN upper(trim(channel_raw)) END) AS allowed_channels,
        collect_set(CASE WHEN upper(trim(d.Customer_Type))<>'ALL CUSTOMER TYPES' THEN upper(trim(sub_channel_raw)) END) AS allowed_subchannels
    FROM claims_mgmt.dime_channel_summary d
    LATERAL VIEW OUTER explode(split(d.Plan_For,',')) ch AS channel_raw
    LATERAL VIEW OUTER explode(split(d.Customer_Type,',')) sc AS sub_channel_raw
    WHERE d.INITCode NOT IN (SELECT scheme_code FROM mapping WHERE scheme_type='Retailer Level Scheme')
      AND d.Customer_Type<>'' AND d.Plan_For<>'' AND d.RptMonthyear={formatted_date}
    GROUP BY d.INITCode
)
SELECT i.salesinvoiceno,i.InitiativeCode,i.DistCode,i.Retailer_Code,i.ihr_Channel,i.psr_SubChannel,i.BranchCode,
    coalesce(cs.allowed_channels,d.allowed_channels) AS mapping_cs_channel,
    coalesce(cs.allowed_subchannels,d.allowed_subchannels) AS mapping_cs_sub_channel,
    'CHNL_LVL_SCHEMES_CHECK' AS Scheme_Type,'COMBINED_CHECK' AS Scheme_Check,
    CASE
        WHEN cs.initcode IS NOT NULL AND cs.all_customer_type=1 AND i.ihr_Channel IS NULL THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN cs.initcode IS NOT NULL AND (
            array_contains(cs.allowed_channels,upper(trim(i.ihr_Channel))) OR
            array_contains(cs.allowed_subchannels,upper(trim(i.psr_SubChannel)))
        ) THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN d.initcode IS NOT NULL AND d.all_customer_type=1 AND i.ihr_Channel IS NULL THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN d.initcode IS NOT NULL AND (
            array_contains(d.allowed_channels,upper(trim(i.ihr_Channel))) OR
            array_contains(d.allowed_subchannels,upper(trim(i.psr_SubChannel)))
        ) THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN cs.initcode IS NOT NULL OR d.initcode IS NOT NULL THEN 'CHANNEL/SUB-CHANNEL MISMATCH'
        ELSE 'NOT IN DIME CS/MAPPING'
    END AS REMARK
FROM ihr_psr_chnls i
LEFT JOIN cs_rule cs ON upper(trim(i.InitiativeCode))=upper(trim(cs.initcode))
LEFT JOIN dime_rule d ON upper(trim(i.InitiativeCode))=upper(trim(d.initcode))
WHERE i.InitiativeCode NOT IN (
    SELECT scheme_code FROM mapping WHERE scheme_type='Retailer Level Scheme'
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Combined_lvl_check AS
# MAGIC
# MAGIC SELECT
# MAGIC     salesinvoiceno,
# MAGIC     InitiativeCode,
# MAGIC     DistCode,
# MAGIC     Retailer_Code,
# MAGIC     ihr_Channel,
# MAGIC     psr_SubChannel,
# MAGIC     BranchCode,
# MAGIC     concat_ws(',', mapping_cs_channel) AS mapping_cs_channel,
# MAGIC     concat_ws(',', mapping_cs_sub_channel) AS mapping_cs_sub_channel,
# MAGIC     Scheme_Type,
# MAGIC     Scheme_Check,
# MAGIC     REMARK
# MAGIC FROM Retailer_lvl_check
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     salesinvoiceno,
# MAGIC     InitiativeCode,
# MAGIC     DistCode,
# MAGIC     Retailer_Code,
# MAGIC     ihr_Channel,
# MAGIC     psr_SubChannel,
# MAGIC     BranchCode,
# MAGIC     concat_ws(',', mapping_cs_channel) AS mapping_cs_channel,
# MAGIC     concat_ws(',', mapping_cs_sub_channel) AS mapping_cs_sub_channel,
# MAGIC     Scheme_Type,
# MAGIC     Scheme_Check,
# MAGIC     REMARK
# MAGIC FROM Channel_lvl_check;

# COMMAND ----------

# DBTITLE 1,Updated channel check with formmated _date
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW CHANNEL_CHECK AS

SELECT
    salesinvoiceno,
    InitiativeCode,
    DistCode,
    BranchCode,
    MAX(CASE 
        WHEN cs.initcode IS NOT NULL THEN 'CS-Approved'
        ELSE 'Not in CS'
    END) AS CS_Check,

    CASE
        WHEN array_contains(collect_set(REMARK), 'CHANNEL/SUB-CHANNEL MATCHING')
            THEN 'CHANNEL/SUB-CHANNEL MATCHING'
        WHEN array_contains(collect_set(REMARK), 'CHANNEL/SUB-CHANNEL MISMATCH')
            THEN 'CHANNEL/SUB-CHANNEL MISMATCH'
        ELSE 'NOT IN CS/MAPPING'
    END AS REMARK

FROM Combined_lvl_check c

LEFT JOIN (
    SELECT DISTINCT initcode
    FROM stg.cs_combined
    -- WHERE rptmonthyear = '{formatted_date}'
) cs
    ON c.InitiativeCode = cs.initcode

GROUP BY 
    salesinvoiceno, InitiativeCode, DistCode, BranchCode
""")

# COMMAND ----------

channel_check_df = spark.sql("SELECT * FROM CHANNEL_CHECK")

# COMMAND ----------

channel_check_df.repartition(500).write.mode("overwrite").saveAsTable("claims_mgmt.Channel_lvl_check")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT REMARK,count(*) FROM CHANNEL_CHECK GROUP BY REMARK

# COMMAND ----------

dbutils.notebook.exit('suc')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT REMARK,count(*) FROM CHANNEL_CHECK GROUP BY REMARK
# MAGIC CHANNEL/SUB-CHANNEL MATCHING	16938550
# MAGIC NOT IN CS/MAPPING	716792
# MAGIC CHANNEL/SUB-CHANNEL MISMATCH	14946

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct InitiativeCode,salesinvoiceno,REMARK from CHANNEL_CHECK where InitiativeCode = 'LTR2601N00003925'

# COMMAND ----------

dbutils.notebook.exit('sucess')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC -- claims_mgmt.consider_chnl_check
# MAGIC claims_mgmt.Channel_lvl_check
# MAGIC  where InitiativeCode = 
# MAGIC  'LSS2602N1458_BC10'
# MAGIC --  'LSS2602N0214'
# MAGIC  and REMARK in ('CHANNEL/SUB-CHANNEL MISMATCH')

# COMMAND ----------

# %sql
# UPDATE claims_mgmt.Channel_lvl_check
# SET REMARK = 'CHANNEL/SUB-CHANNEL MATCHING'
# WHERE REMARK = 'CHANNEL/SUB-CHANNEL MISMATCH'
#   AND InitiativeCode IN (
#       SELECT InitiativeCode
#       FROM temp
#   );

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_mgmt.Channel_lvl_check 
# MAGIC where InitiativeCode in (
# MAGIC     'LTT2512N00000545',
# MAGIC     'LTT2601N00003417',
# MAGIC     'LSS2510N6367_BC1',
# MAGIC     'LSS2510N6307_BC1',
# MAGIC     'LSS2508N5048_BC2',
# MAGIC     'LSS2508R5047_BC2',
# MAGIC     'LSS2508R5048_BC2',
# MAGIC     'LSS2508R5007_BC2',
# MAGIC     'LSS2510N6329_BC1',
# MAGIC     'LSS2510N6330_BC1',
# MAGIC     'LSS2510N6332_BC1',
# MAGIC     'LTT2601N00003416',
# MAGIC     'LSS2510N6321_BC1',
# MAGIC     'LSS2510N6322_BC1',
# MAGIC     'LSS2510N6334_BC1',
# MAGIC     'LSS2510N6337_BC1',
# MAGIC     'LSS2510N6240_BC1',
# MAGIC     'LSS2510N6290_BC1',
# MAGIC     'LSS2506N5560_TC1',
# MAGIC     'LSS2510N6237_BC1',
# MAGIC     'LSS2510N6287_BC1',
# MAGIC     'LSS2510N6288_BC1'
# MAGIC ) 
# MAGIC and REMARK not in ('CHANNEL/SUB-CHANNEL MATCHING')

# COMMAND ----------

# %sql
# select * from  claims_mgmt.Channel_lvl_check where InitiativeCode = ''

# COMMAND ----------

# MAGIC %sql
# MAGIC select REMARK,Count(*) from  claims_mgmt.Channel_lvl_check group by REMARK

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct InitiativeCode from  claims_mgmt.Channel_lvl_check where REMARK = 'CHANNEL/SUB-CHANNEL MISMATCH'

# COMMAND ----------

# # %sql
# # CREATE OR REPLACE TEMP VIEW temp AS
# # SELECT DISTINCT InitiativeCode
# # FROM claims_mgmt.Channel_lvl_check
# -- WHERE REMARK = 'CHANNEL/SUB-CHANNEL MISMATCH'
# --   AND InitiativeCode NOT IN (
# --       'LSS2602N1458_BC10',
# --       'LSS2602NC14022',
# --       'LSS2602WS1001'
# --   );

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

df = spark.sql("""
SELECT
    c.salesinvoiceno,
    c.InitiativeCode,
    c.REMARK,
    c.CS_Check,
    c.DistCode, c.BranchCode,
    v.matrix_check_fortnight        AS Consideration_check,
    v.scheme_code_consideration_remark_1 AS Consideration_Remark
FROM claims_mgmt.Channel_lvl_check c
-- FROM (SELECT * FROM CHANNEL_CHECK) c
LEFT JOIN vs_list v
    ON c.InitiativeCode = v.InitiativeCode
""")
df.write.mode("overwrite").saveAsTable("claims_mgmt.consider_chnl_check")

# COMMAND ----------

# MAGIC %sql
# MAGIC select initcode from stg.cs_combined where initcode in ('LSS2602N4355_BC2_L3','LSS2602N3922_BC9')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_mgmt.consider_chnl_check where CS_Check = 'Not in CS'

# COMMAND ----------

# MAGIC %sql
# MAGIC select CS_Check,Count(*) from claims_mgmt.consider_chnl_check group by CS_Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg.cs_combined where initcode = 'LTR2602N00000810'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sd_science.trade_plan_dtls_master where scheme_code ='LTR2602N00000810_MRI'
# MAGIC -- union
# MAGIC -- select scheme_code from stg.masters_auditex

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_mgmt.consider_chnl_check where InitiativeCode like '%MRI'

# COMMAND ----------

# df1 = spark.sql("""
# SELECT 
# ch.salesinvoiceno,
# ch.InitiativeCode,
# ch.DistCode, ch.BranchCode,
# ch.REMARK,
# ch.Consideration_check,
# ch.Consideration_Remark,
# ch.CS_Check AS Channel_Summary_Present_check,
# wr.calculated AS Wrong_Rate,
# 'wrong rate calculation' as Remark_wrong_rate,
# srn.amountdisbursed_orginal,
# srn.amountdisbursed_retrun,
# srn.disallowance,
# srn.Remarks_SRN
# FROM claims_mgmt.consider_chnl_check ch
# LEFT JOIN claims_mgmt.worng_rate_calcs wr
# ON ch.InitiativeCode = wr.InitiativeCode
# AND ch.salesinvoiceno = wr.invoice_number
# LEFT JOIN claims_mgmt.srn_calcs srn
# ON ch.InitiativeCode = srn.scheme_code
# AND ch.salesinvoiceno = srn.invoice_number
# """)
# df1.write.mode("overwrite").saveAsTable("claims_mgmt.base_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_mgmt.base_table where InitiativeCode like '%MRI'

# COMMAND ----------

# df_loc = spark.sql("""SELECT Distinct DistributorCode,BranchCode,SiteName FROM stg.locationhierarchy """)
# df_loc.write.mode("overwrite").saveAsTable("stg.dist_branch_site_mapping")

# COMMAND ----------

# df_final = spark.sql(""" SELECT Distinct p.*, m.SiteName FROM claims_mgmt.base_table p LEFT JOIN stg.dist_branch_site_mapping m ON p.DistCode = m.DistributorCode and p.BranchCode = m.BranchCode 
# """)
# df_final.write.mode("overwrite").saveAsTable("claims_mgmt.base_table_with_site")