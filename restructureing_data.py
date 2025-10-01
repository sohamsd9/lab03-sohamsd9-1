import os 
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path




spark = SparkSession.builder.appName("JobPostingsAnalysis").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine","true").option("escape", "\"").csv("lightcast_job_postings.csv")

OUTPUT_DIR = "solution"
Path(OUTPUT_DIR).mkdir(exist_ok=True)
company_df = df.select(
    col("COMPANY").alias("COMPANY_ID"),  
    col("COMPANY_NAME").alias("COMPANY_NAME"),
    col("COMPANY_RAW").alias("COMPANY_RAW"),
    col("COMPANY_IS_STAFFING").alias("COMPANY_IS_STAFFING")
).distinct()
company_df.toPandas().to_csv(f"{OUTPUT_DIR}/company.csv", index=False)


job_postings_df = df.select(
    col("ID").alias("JOB_ID"),
    col("TITLE_RAW").alias("TITLE_RAW"),
    col("TITLE_CLEAN").alias("TITLE_CLEAN"),
    col("POSTED").alias("POSTED"),
    col("EXPIRED").alias("EXPIRED"),
    col("SALARY_FROM").alias("SALARY_FROM"),
    col("SALARY_TO").alias("SALARY_TO"),
    col("MIN_YEARS_EXPERIENCE").alias("MIN_YEARS_EXPERIENCE"),
    col("MAX_YEARS_EXPERIENCE").alias("MAX_YEARS_EXPERIENCE"),
    col("SKILLS").alias("SKILLS"),
    col("SPECIALIZED_SKILLS").alias("SPECIALIZED_SKILLS"),
    col("SOFTWARE_SKILLS").alias("SOFTWARE_SKILLS"),
    col("EMPLOYMENT_TYPE_NAME").alias("EMPLOYMENT_TYPE"),
    col("COMPANY").alias("COMPANY_ID") 
).distinct()
job_postings_df.toPandas().to_csv(f"{OUTPUT_DIR}/job_postings.csv", index=False)

job_location_df = df.select(
    col("ID").alias("JOB_ID"),   
    col("CITY_NAME").alias("CITY"),
    col("STATE_NAME").alias("STATE"),
    col("COUNTY_NAME").alias("COUNTY"),
    col("LOCATION").alias("LOCATION")
).distinct()
job_location_df.toPandas().to_csv(f"{OUTPUT_DIR}/job_location.csv", index=False)


soc_details_df = df.select(
    col("ID").alias("JOB_ID"),  
    col("SOC_2021_2").alias("SOC_2"),
    col("SOC_2021_2_NAME").alias("SOC_2_NAME"),
    col("SOC_2021_3").alias("SOC_3"),
    col("SOC_2021_3_NAME").alias("SOC_3_NAME"),
    col("SOC_2021_4").alias("SOC_4"),
    col("SOC_2021_4_NAME").alias("SOC_4_NAME"),
    col("SOC_2021_5").alias("SOC_5"),
    col("SOC_2021_5_NAME").alias("SOC_5_NAME")
).distinct()
soc_details_df.toPandas().to_csv(f"{OUTPUT_DIR}/soc_details.csv", index=False)


lot_details_df = df.select(
    col("ID").alias("JOB_ID"),  
    col("LOT_CAREER_AREA").alias("LOT_CAREER_AREA"),
    col("LOT_CAREER_AREA_NAME").alias("LOT_CAREER_AREA_NAME"),
    col("LOT_OCCUPATION").alias("LOT_OCCUPATION"),
    col("LOT_OCCUPATION_NAME").alias("LOT_OCCUPATION_NAME"),
    col("LOT_SPECIALIZED_OCCUPATION").alias("LOT_SPECIALIZED_OCCUPATION"),
    col("LOT_SPECIALIZED_OCCUPATION_NAME").alias("LOT_SPECIALIZED_OCCUPATION_NAME")
).distinct()
lot_details_df.toPandas().to_csv(f"{OUTPUT_DIR}/lot_details.csv", index=False)


naics_details_df = df.select(
    col("ID").alias("JOB_ID"), 
    col("NAICS_2022_2").alias("NAICS2"),
    col("NAICS_2022_2_NAME").alias("NAICS2_NAME"),
    col("NAICS_2022_3").alias("NAICS3"),
    col("NAICS_2022_3_NAME").alias("NAICS3_NAME"),
    col("NAICS_2022_4").alias("NAICS4"),
    col("NAICS_2022_4_NAME").alias("NAICS4_NAME"),
    col("NAICS_2022_5").alias("NAICS5"),
    col("NAICS_2022_5_NAME").alias("NAICS5_NAME"),
    col("NAICS_2022_6").alias("NAICS6"),
    col("NAICS_2022_6_NAME").alias("NAICS6_NAME")
).distinct()
naics_details_df.toPandas().to_csv(f"{OUTPUT_DIR}/naics_details.csv", index=False)

spark.stop()
