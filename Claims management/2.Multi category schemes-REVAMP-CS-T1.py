# Databricks notebook source
import io
import math
import requests
import calendar
from dateutil.rrule import rrule, MONTHLY
import shutil
import os
import re

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
folder=date_obj.strftime("%b-%y")
file_name=date_obj.strftime("%b-%Y")

print(formatted_date)
print(month_start)
print(ihr_date_format)
print(start_date_value,end_date_value,folder,file_name)

# COMMAND ----------

# DBTITLE 1,scheme master config
# spark.sql(f'''select initiativecode,`description_used_y/n` as desc_flg, what_is_used as desc from stg.scheme_master_conf where type in ('Brand-Channel','Targeted Plan') or initiativecode like 'LSS%' ''').createOrReplaceTempView("smt")
# display(spark.sql('select * from smt'))
spark.sql(f'''select initiativecode,`description_used_y/n` as desc_flg, what_is_used as desc from stg.scheme_master_conf where initiativecode like 'LSS%' or initiativecode like 'LTR%' or initiativecode like 'LTT%' ''').createOrReplaceTempView("smt")
display(spark.sql('select * from smt'))


# COMMAND ----------

ihr=spark.sql(f'''
Select salesInvoiceNo,ShipDate,DistCode,retailercode,InitiativeCode,InitiativeName,channel,CAST(amountdisbursed AS double) AS amountdisbursed,
branchcode from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where ShipDate >= '{start_date_value}' and ShipDate <= '{end_date_value}' ''')
ihr.createOrReplaceTempView("ihr")
# display(
#     ihr.limit(10)
# )

# COMMAND ----------

# %sql
# select * from ihr where InitiativeCode like '%MRI'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select sum(amountdisbursed) from ihr where InitiativeCode = 'LTR2602N00000851'
# MAGIC -- Select sum(amountdisbursed) from cdl_india_data_prod.india_distributordata_refined.tblrefinedview_ihr where ShipDate >= '2026-02-01' and ShipDate <= '2026-02-28' and InitiativeCode = 'LTR2602N00000851'

# COMMAND ----------

Master_sdf=spark.sql(f'''select * from sd_science.trade_plan_dtls_master where Scheme_Code in (select distinct InitiativeCode from ihr) and left(scheme_code,4)!="RKWI" ''')
Master_sdf=Master_sdf.withColumn("Level_Desc",explode(split(col("subfNameList"),"\\|")))
Master_sdf=Master_sdf.select('scheme_code','short_description','Level_Desc','level_type','Promotion_Group_Name')
Master_sdf=Master_sdf.dropDuplicates()

Master_sdf.createOrReplaceTempView("Master_sdf1")
Master_sdf=(spark.sql(''' Select scheme_code,short_description,Level_Desc, level_type,Promotion_Group_Name from stg.masters_auditex where left(scheme_code,4)!="RKWI" and scheme_code in (select InitiativeCode from ihr) and scheme_code not in (select scheme_code from Master_sdf1) union select * from Master_sdf1 ''').dropDuplicates())
Master_sdf.createOrReplaceTempView("Master_sdf")
Master_sdf_final = spark.sql(f"""
                         select t1.*,t2.initiativename
                         from Master_sdf t1
                         left join ihr t2
                         on 
                         t1.scheme_code = t2.initiativecode
                         """)
Master_sdf_final.createOrReplaceTempView("Master_sdf_final")

# COMMAND ----------

# %sql
# select distinct fdf.*,mdf.short_description from claims_mgmt.final_df fdf
# left join Master_sdf_final mdf on fdf.scheme_code = mdf.scheme_code
# where scheme_category = 'Multi Brand Scheme' 

# COMMAND ----------

# %sql
# SELECT *
# FROM Master_sdf_final
# WHERE scheme_code  = 'LTR2507N0418'
# -- WHERE scheme_code  = 'LSS2602MRC2244_BC13'
# -- IN (
# --     SELECT DISTINCT scheme_code
# --     FROM claims_mgmt.final_df
# --     WHERE scheme_category = 'Multi Brand Scheme'
# -- );

# COMMAND ----------

# if group_level1 is there then don't check category schemes, 
# if other group levles then check for all
# check LSS schemes

# COMMAND ----------

# %sql
# select scheme_code,level_type,count(distinct Level_Desc) from  Master_sdf_final where level_type = 'Category' group by scheme_code,level_type

# COMMAND ----------

# DBTITLE 1,scheme_int-V1
# MAGIC %skip
# MAGIC scheme_base = spark.sql(f"""
# MAGIC                          select m1.*
# MAGIC                          from Master_sdf_final m1
# MAGIC                          inner join smt m2
# MAGIC                          on 
# MAGIC                          m1.scheme_code like concat(m2.initiativecode,"%")
# MAGIC                          and (m2.desc_flg="No" OR (m2.desc_flg='Yes' and locate(upper(m2.desc),(upper(m1.initiativename)))>0 ))
# MAGIC                          """)
# MAGIC
# MAGIC scheme_int = scheme_base.filter(
# MAGIC     (
# MAGIC         (upper(trim(col("scheme_code"))).startswith("LTR") |
# MAGIC          upper(trim(col("scheme_code"))).startswith("LTT") |
# MAGIC          (upper(trim(col("scheme_code"))).startswith("LSS") &
# MAGIC           (upper(trim(col("Promotion_Group_Name")))=="GROUP_LEVEL1")))
# MAGIC         & (upper(trim(col("level_type")))!="CATEGORY")
# MAGIC     )
# MAGIC     |
# MAGIC     (
# MAGIC         upper(trim(col("scheme_code"))).startswith("LSS") &
# MAGIC         (upper(trim(col("Promotion_Group_Name")))!="GROUP_LEVEL1")
# MAGIC     )
# MAGIC ).dropDuplicates()
# MAGIC
# MAGIC # scheme_int.createOrReplaceTable("claims_mgmt.scheme_int")
# MAGIC scheme_int.write.mode("overwrite").saveAsTable("claims_mgmt.scheme_int")
# MAGIC display(scheme_int.count())
# MAGIC # display(scheme_int)

# COMMAND ----------

scheme_base = spark.sql(f"""
                         select m1.*
                         from Master_sdf_final m1
                         inner join smt m2
                         on 
                         m1.scheme_code like concat(m2.initiativecode,"%")
                         and (m2.desc_flg="No" OR (m2.desc_flg='Yes' and locate(upper(m2.desc),(upper(m1.initiativename)))>0 ))
                         """)

scheme_int = (
    scheme_base
    .filter(
        upper(trim(col("scheme_code"))).startswith("LTR") |
        upper(trim(col("scheme_code"))).startswith("LTT") |
        upper(trim(col("scheme_code"))).startswith("LSS")
    )
    .filter(
        ~(
            (upper(trim(col("Promotion_Group_Name"))) == "GROUP_LEVEL1") &
            (upper(trim(col("level_type"))) == "CATEGORY")
        )
    )
    .dropDuplicates()
)
scheme_int.write.mode("overwrite").saveAsTable("claims_mgmt.scheme_int")
display(scheme_int.count())

# COMMAND ----------

# %sql
# select * from claims_mgmt.scheme_int where scheme_code RLIKE '[A-Za-z]$';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from claims_mgmt.scheme_int 
# MAGIC -- where 
# MAGIC -- scheme_code = 
# MAGIC -- 'LTR2602TPRE810234'
# MAGIC -- 'LSS2602N3033_BC2_L3'
# MAGIC -- 'LSS2602N4792_BC2'
# MAGIC -- scheme_code = 'LSS2602N3033_BC2_L1'
# MAGIC -- scheme_code = 'LSS2602N4781_BC2_L3'

# COMMAND ----------

# DBTITLE 1,Original Categroy code
# scheme_base = spark.sql(f"""
#                          select m1.*
#                          from Master_sdf_final m1
#                          inner join smt m2
#                          on 
#                          m1.scheme_code like concat(m2.initiativecode,"%")
#                          and (m2.desc_flg="No" OR (m2.desc_flg='Yes' and locate(upper(m2.desc),(upper(m1.initiativename)))>0 ))
#                          """)

# # Filtering for rows with level type as category because we don't want to use these for laundry or multi brand classification                       
# scheme_int = (
#     scheme_base
#     .filter(upper(trim(col('level_type')))!='CATEGORY')
#     ).dropDuplicates()
# display(scheme_int)

# COMMAND ----------

# DBTITLE 1,Multi Category Schemes

# Normalize function
# Standardizing text before matching to avoid issues due to:
# - uppercase/lowercase differences, special characters, extra spaces
# Example- Oral-b -> oral b

def normalize(col_name):
    """
    Lowercase + remove special chars + normalize spaces
    """
    return trim(regexp_replace(lower(col(col_name)), r'[^a-z0-9]+',' '))

# COMMAND ----------

# %sql
# SELECT count(*)
# FROM cdl_india_data_prod.india_sdm_refined.productmaster

# COMMAND ----------

# %sql
# SELECT *
# FROM cdl_india_data_prod.india_sdm_refined.productmaster
# -- WHERE BrandName NOT IN ('UNKNOWN', 'UNKNOWN ER', 'UNKNOWN SUBD')

# COMMAND ----------

######Creating the brand mapping table - one time activity, got the file from Priyanka

# COMMAND ----------

# DBTITLE 1,FOR UPDATING claims_mgmt.brand_business_mapping
# MAGIC %skip
# MAGIC data = [
# MAGIC ('DUMMY BRAND','NA'),
# MAGIC ('TIDE','Tide'),
# MAGIC ('ARIEL','Ariel'),
# MAGIC ('ARIEL LIQUID','Ariel'),
# MAGIC ('PAMPERS BABY WIPES','Pampers'),
# MAGIC ('PAMPERS','Pampers'),
# MAGIC ('GUARD 3IN1','Gillette'),
# MAGIC ('GUARD','Gillette'),
# MAGIC ('GILLETTE PRESTO INTL','Gillette'),
# MAGIC ('GILLETTE SENSOR EXCEL','Gillette'),
# MAGIC ('GILLETTE VECTOR PLUS','Gillette'),
# MAGIC ('WS DEVICE','Gillette'),
# MAGIC ('7 O CLOCK','Gillette'),
# MAGIC ('7 O CLOCK PII','Gillette'),
# MAGIC ('BLUE II','Gillette'),
# MAGIC ('365 DE','Gillette'),
# MAGIC ('WILKINSON SWORD','Gillette'),
# MAGIC ('GILLETTE LABS','Gillette'),
# MAGIC ('WILMAN','Gillette'),
# MAGIC ('FUSION','Gillette'),
# MAGIC ('SKINGUARD','Gillette'),
# MAGIC ('BLUE 3','Gillette'),
# MAGIC ('GILLETTE SPORT','Gillette'),
# MAGIC ('WINNER','Gillette'),
# MAGIC ('GILLETTE MACH3','Gillette'),
# MAGIC ('GILLETTE G II PLUS','Gillette'),
# MAGIC ('VECTOR3','Gillette'),
# MAGIC ('MALE BODY','Gillette'),
# MAGIC ('MACH-3 TURBO','Gillette'),
# MAGIC ('OLAY','Olay'),
# MAGIC ('MAXEPA','NA'),
# MAGIC ('CLOBETAMIL','NA'),
# MAGIC ('NEUROBION','NA'),
# MAGIC ('V BABYBALSAM','Vicks'),
# MAGIC ('EMFLAM PLUS','NA'),
# MAGIC ('MINOR BRANDS','NA'),
# MAGIC ('POLYBION','NA'),
# MAGIC ('NASIVION','NA'),
# MAGIC ('V VAPORUB','Vicks'),
# MAGIC ('ECOBION','NA'),
# MAGIC ('COSOME','NA'),
# MAGIC ('ELECTROBION','NA'),
# MAGIC ('SEVEN SEAS','NA'),
# MAGIC ('EMQUIN','NA'),
# MAGIC ('DVION','NA'),
# MAGIC ('EVION','NA'),
# MAGIC ('V THROAT DROPS','Vicks'),
# MAGIC ('ESOFENCE','NA'),
# MAGIC ('BETAMIL','NA'),
# MAGIC ('CANDISTAT','NA'),
# MAGIC ('OFLOXACIN','NA'),
# MAGIC ('LIVOGEN','NA'),
# MAGIC ('ARGIGEST','NA'),
# MAGIC ('AMBIPUR','Ambipur'),
# MAGIC ('UNKNOWN','NA'),
# MAGIC ('BRAUN','Braun'),
# MAGIC ('PRESTO','Gillette'),
# MAGIC ('MACH3','Gillette'),
# MAGIC ('LONDON BRIDGE','NA'),
# MAGIC ('PERMA SHARP','NA'),
# MAGIC ('WILKINSON','Gillette'),
# MAGIC ('7 OCLOCK','Gillette'),
# MAGIC ('GILLETTE SERIES','Gillette'),
# MAGIC ('VENUS','Venus'),
# MAGIC ('AMBI PUR','Ambipur'),
# MAGIC ('UNKNOWN SUBD','NA'),
# MAGIC ('PANTENE OIL REPLACEMENT','Pantene'),
# MAGIC ('HERBAL ESSENCES','H&S'),
# MAGIC ('H&S','H&S'),
# MAGIC ('PANTENE OIL','Pantene'),
# MAGIC ('HEAD & SHOULDERS','H&S'),
# MAGIC ('PANTENE','Pantene'),
# MAGIC ('WHISPER CHOICE','Whisper'),
# MAGIC ('WHISPER MAXI','Whisper'),
# MAGIC ('LINERS','Whisper'),
# MAGIC ('WHISPER ULTRA','Whisper'),
# MAGIC ('FLEXFOAM','Whisper'),
# MAGIC ('WHISPER','Whisper'),
# MAGIC ('ORAL B','Oral B'),
# MAGIC ('ORAL-B','Oral B'),
# MAGIC ('BABYRUB','Vicks'),
# MAGIC ('ACTION 500','Vicks'),
# MAGIC ('VICKS COUGH SYRUP','Vicks'),
# MAGIC ('VAPORUB XTRA STRONG','Vicks'),
# MAGIC ('VICKS','Vicks'),
# MAGIC ('VCD','Vicks'),
# MAGIC ('VICKS 3IN1','Vicks'),
# MAGIC ('INHALER','Vicks'),
# MAGIC ('VAPORUB','Vicks'),
# MAGIC ('FUSION PC','Gillette'),
# MAGIC ('GILLETTE','Gillette'),
# MAGIC ('GILLETTE SATIN CARE','Gillette'),
# MAGIC ('KING C GILLETTE','Gillette'),
# MAGIC ('OLD SPICE','Old Spice'),
# MAGIC ('SATIN CARE','Gillette'),
# MAGIC ('ALWAYS','NA'),
# MAGIC ('UNKNOWN ER','NA')
# MAGIC ]
# MAGIC
# MAGIC df_new=spark.createDataFrame(data,[
# MAGIC   'BrandName','business_brand'
# MAGIC ])
# MAGIC df_new.write.mode('overwrite').saveAsTable('claims_mgmt.brand_business_mapping')
# MAGIC
# MAGIC display(df_new)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Product master df
# product_master=spark.sql('select distinct BrandName from stg.prod_dim_ext_vw')
product_master = spark.sql("""
SELECT *
FROM stg.product_master_all_brand
WHERE BrandName NOT IN ('UNKNOWN', 'UNKNOWN ER', 'UNKNOWN SUBD')
""")

# Taking brand names from brand mapping table which contains the business names of brands
brand_mapping_df=spark.sql('select distinct BrandName, business_brand from claims_mgmt.brand_business_mapping')
brand_mapping_df.createOrReplaceTempView("brand_mapping_df")

# COMMAND ----------

# Scheme data prep
# scheme_df = (
#     claims_mgmt.scheme_int
#     .withColumn('Level_Desc',normalize('Level_Desc'))
# )

# scheme_df.createOrReplaceTempView("scheme_df")
scheme_df = (
    spark.table("claims_mgmt.scheme_int")
    .withColumn("Level_Desc", normalize("Level_Desc"))
)

scheme_df.createOrReplaceTempView("scheme_df")

# COMMAND ----------

brand_df = (
    brand_mapping_df
    .select('BrandName','business_brand')
    .distinct()
    .withColumn('BrandName',normalize('BrandName'))
    .withColumn('business_brand',normalize('business_brand'))
    .filter(col('BrandName').isNotNull())
    .filter(col('BrandName')!= "")
    .dropDuplicates()
)
brand_df.createOrReplaceTempView('brand_df')
# display(brand_df)

# COMMAND ----------

# Brand master prep
product_master_df_1 = (
    product_master
    .select('BrandName','BrandformName','ProductCode','SubbfName','CategoryName')
    .distinct()
    .withColumn('BrandName',normalize('BrandName'))
    .withColumn('BrandformName',normalize('BrandformName'))
    .withColumn('ProductCode',normalize('ProductCode'))
    .withColumn('SubbfName',normalize('SubbfName'))
    .withColumn('CategoryName',normalize('CategoryName'))
    .distinct()
    .filter(col('BrandName').isNotNull())
    .filter(col('BrandName')!= "")
    .dropDuplicates()
)
product_master_df_1.createOrReplaceTempView('product_master_df_1')

# COMMAND ----------

product_master_df = (
    spark.sql(f"""
              select p.*,b.business_brand from product_master_df_1 p
              left join brand_df b 
              on p.brandname=b.brandname
              """)
)
# display(product_master_df)
product_master_df.createOrReplaceTempView('product_master_df')

# COMMAND ----------

# %sql
# select * from scheme_df where scheme_code in ('LTR2601N00003959')

# COMMAND ----------

# Brand matching
# Broadcast is used because brand df is a small dataset
matched_w_business_brands = (
    scheme_df.alias('m')
    .join(
        broadcast(product_master_df).alias('p'),
        on=( (F.when(F.upper(scheme_df["level_type"]) == "SUB-BRANDFORM", F.trim(F.upper(product_master_df["SubbfName"])) == F.trim(F.upper(scheme_df["level_desc"])))
       .when(F.upper(scheme_df["level_type"]) == "BRANDFORM", F.trim(F.upper(product_master_df["BrandformName"])) == F.trim(F.upper(scheme_df["level_desc"])))
       .when(F.upper(scheme_df["level_type"]) == "BRAND", F.trim(F.upper(product_master_df["BrandName"])) ==  F.trim(F.upper(scheme_df["level_desc"])))
       .when(F.upper(scheme_df["level_type"]) == "CATEGORY",  F.trim(F.upper(product_master_df["CategoryName"])) ==  F.trim(F.upper(scheme_df["level_desc"])))
       .when(F.upper(scheme_df["level_type"]) == "SKU",  F.trim(F.upper(product_master_df["ProductCode"])) ==  F.trim(F.upper(scheme_df["level_desc"])))))
        , how="inner"
    )
    .select(
        'scheme_code',
        'Level_Desc',
        'BrandName',
        'business_brand'
    )
    .distinct()
)

matched_w_business_brands.write.mode("overwrite").saveAsTable("claims_mgmt.matched_w_business_brands")
# matched_w_business_brands.write.mode("overwrite").saveAsTable("matched_w_business_brands_test")

# COMMAND ----------

# %sql
# select * from claims_mgmt.matched_w_business_brands where scheme_code = 'LSS2602N4903_BC3'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct scheme_code,Level_Desc,BrandName,business_brand from claims_mgmt.matched_w_business_brands 
# MAGIC -- where scheme_code like '%MRI'
# MAGIC -- scheme_code = 'LSS2602N4781_BC2_L3'
# MAGIC -- # -- scheme_code = 'LTR2602N00000849'
# MAGIC -- # --'LTR2507N0381'
# MAGIC -- # --where business_brand in ('na') 
# MAGIC -- # --scheme_code= 'LTR2602N00000669' 
# MAGIC -- # scheme_code = 'LSS2512GB1035_BC10'

# COMMAND ----------

# %sql
# -- # fd = spark.table("matched_w_business_brands")
# Select * from matched_w_business_brands_test where scheme_code in ('LTR2601N00003959')

# COMMAND ----------

# %sql
# select * from claims_mgmt.matched_w_business_brands where scheme_code ='LSS2602N3033_BC2_L1'

# COMMAND ----------

spark.table("claims_mgmt.matched_w_business_brands").count()

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC # Remove overlapping smaller brands
# MAGIC # example: level desc='pampers baby wipes' may match both-
# MAGIC # pampers, pampers baby wipes, but logicaly this is not a multi brand scenario so we keep only the longest/most specific match - pampers baby wipes
# MAGIC # Logic:
# MAGIC # Remove brand A if:
# MAGIC # - another brand B exists
# MAGIC # - B contains A
# MAGIC # - B is longer than A
# MAGIC # This avoids false multi-brand classifications
# MAGIC
# MAGIC a=matched_df.alias('a')
# MAGIC b=matched_df.alias('b')
# MAGIC
# MAGIC overlap_df=(
# MAGIC     a.join(
# MAGIC         b,
# MAGIC         (
# MAGIC             (
# MAGIC                 col('a.scheme_code')==col('b.scheme_code')
# MAGIC             ) &
# MAGIC             (
# MAGIC                 col('a.level_desc')==col('b.level_desc')
# MAGIC             ) &
# MAGIC             (
# MAGIC                 col('a.brandname')!=col('b.brandname')
# MAGIC             ) &
# MAGIC             (
# MAGIC                 col('b.brandname').contains(col('a.brandname'))
# MAGIC             ) &
# MAGIC             (
# MAGIC                 length('b.brandname') > length('a.brandname')
# MAGIC             )
# MAGIC         ),
# MAGIC         "left"
# MAGIC     )
# MAGIC     # Retaining only rows where no larger overlapping brand exists
# MAGIC     .filter(col('b.brandname').isNull())
# MAGIC     .select(
# MAGIC         col('a.scheme_code').alias('scheme_code'),
# MAGIC         col('a.brandname').alias('brandname'),
# MAGIC         col('a.business_brand').alias('business_brand')
# MAGIC     )
# MAGIC     .distinct()
# MAGIC )
# MAGIC
# MAGIC display(overlap_df.limit(5))

# COMMAND ----------

# collect distinct brands per scheme
scheme_brand_df = (
    spark.table("claims_mgmt.matched_w_business_brands")
    # Removing rows with "na" as business brand because that shouldn't be used to classify multi brand schemes
     .groupBy('scheme_code')
    .agg(
        collect_set('business_brand').alias('brand_set')
    )
)
scheme_brand_df.write.mode("overwrite").saveAsTable("claims_mgmt.scheme_brand_df")

# COMMAND ----------

saved_scheme_brand_df = spark.table("claims_mgmt.scheme_brand_df")
print("=" * 100)
print("TOTAL ROWS")
display(saved_scheme_brand_df.agg(F.count("*").alias("total_rows")))

print("=" * 100)
print("ROWS WITH EMPTY brand_set")
display(saved_scheme_brand_df.filter(size(col("brand_set")) == 0))

# COMMAND ----------

from pyspark.sql.functions import *

result_df = (
    spark.table("claims_mgmt.scheme_brand_df")
    .withColumn(
        "scheme_category",
        when(
            array_contains(col("brand_set"), "tide") &
            array_contains(col("brand_set"), "ariel"),
            "Laundry Scheme"
        )
        .when(
            (size(col("brand_set")) > 1) &
            ~(
                (
                    array_contains(col("brand_set"), "gillette") &
                    array_contains(col("brand_set"), "venus") &
                    (size(array_except(
                        col("brand_set"),
                        array(lit("gillette"), lit("venus"), lit("na"))
                    )) == 0)
                )
                |
                (
                    array_contains(col("brand_set"), "gillette") &
                    array_contains(col("brand_set"), "old spice") &
                    (size(array_except(
                        col("brand_set"),
                        array(lit("gillette"), lit("old spice"), lit("na"))
                    )) == 0)
                )
            ),
            "Multi Brand Scheme"
        )
    )
)

all_schemes_df = (
    spark.table("claims_mgmt.scheme_int")
    .select("scheme_code")
    .dropDuplicates()
)

final_df = (
    all_schemes_df
    .join(
        result_df.select(
            "scheme_code",
            "scheme_category",
            "brand_set"
        ),
        "scheme_code",
        "left"
    )
)

final_df.write.mode("overwrite").saveAsTable("claims_mgmt.final_df")

# COMMAND ----------

from pyspark.sql.functions import *
final_df_to_save = (
    spark.table("claims_mgmt.final_df")
    .withColumnRenamed("scheme_code", "INITCode")
    .filter(col("scheme_category").isNotNull())
    .withColumn("RptMonthyear", lit(formatted_date))
    .drop("brand_set")
)

# COMMAND ----------

# %sql
# select distinct scheme_code,scheme_category from claims_mgmt.final_df where scheme_category is not null

# COMMAND ----------

# DBTITLE 1,To be removed when multi brand logic is finalized
# Removing multi brand categorization for now, this can be commented out or removed once the logic is aligned with matrix to include multi brand categorization in the newer runs
from pyspark.sql.functions import col
# final_df_to_save = (final_df_to_save.filter(col("scheme_category").isin("Laundry Scheme")))
final_df_to_save = (final_df_to_save.filter(col("scheme_category").isin("Laundry Scheme", "Multi Brand Scheme")))
final_df_to_save.write.mode("overwrite").saveAsTable("claims_mgmt.final_df_to_save")

# COMMAND ----------

spark.sql(f'''delete from claims_mgmt.laundry_multi_brand_schemes where rptmonthyear="{formatted_date}"''')
final_df_to_save = spark.table("claims_mgmt.final_df_to_save")
final_df_to_save.write.mode("append").saveAsTable("claims_mgmt.laundry_multi_brand_schemes")

# COMMAND ----------

# MAGIC %sql
# MAGIC     select scheme_category,RptMonthyear,count(*) from claims_mgmt.laundry_multi_brand_schemes
# MAGIC     group by scheme_category,RptMonthyear

# COMMAND ----------

# %sql
#     select * from claims_mgmt.laundry_multi_brand_schemes where RptMonthyear = '202603' 

# COMMAND ----------

# %sql
# select * from claims_mgmt.laundry_multi_brand_schemes where RptMonthyear = '202602' and INITCode = 'LSS2602N4903_BC3'

# COMMAND ----------

# %sql
# select RptMonthyear,Count(*) from claims_mgmt.laundry_multi_brand_schemes group by RptMonthyear
# --where INITCode in ('LTR2602N00000011','LTR2602N00000012','LTR2602N00000021','LTR2602N00000022','LTR2602N00004612','LTR2602N00004993','LTR2602N00004994')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

print("total schemes: ")
display(final_df.count())
print()
print("total Laundry schemes: ")
display(final_df.filter(col('scheme_category')=='Laundry Scheme').count())
print()
print("total Multi Brand schemes: ")
display(final_df.filter(col('scheme_category')=='Multi Brand Scheme').count())

# COMMAND ----------

display(final_df.filter(col('scheme_category')=='Laundry Scheme'))

# COMMAND ----------

display(final_df.filter(col('scheme_category')=='Multi Brand Scheme'))