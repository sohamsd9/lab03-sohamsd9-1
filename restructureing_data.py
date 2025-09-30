import pandas as pd
from pathlib import Path

# Input & output paths
DATA_PATH = Path("lightcast_job_postings.csv")
OUTPUT_DIR = Path("_output1")
OUTPUT_DIR.mkdir(exist_ok=True)

# Load dataset
print("Loading dataset...")
df = pd.read_csv(DATA_PATH)

# 1. Job_Postings
job_postings = df[[
    "ID", "TITLE_RAW", "TITLE_CLEAN", "POSTED", "EXPIRED",
    "SALARY_FROM", "SALARY_TO",
    "MIN_YEARS_EXPERIENCE", "MAX_YEARS_EXPERIENCE",
    "SKILLS", "SPECIALIZED_SKILLS", "SOFTWARE_SKILLS",
    "EMPLOYMENT_TYPE_NAME", "COMPANY"
]].drop_duplicates()
job_postings.to_csv(OUTPUT_DIR / "job_postings.csv", index=False)

# 2. Company
company = df[[
    "COMPANY", "COMPANY_NAME", "COMPANY_RAW", "COMPANY_IS_STAFFING"
]].drop_duplicates()
company.rename(columns={"COMPANY": "COMPANY_ID"}, inplace=True)
company.to_csv(OUTPUT_DIR / "company.csv", index=False)

# 3. Job_Location
job_location = df[[
    "ID", "CITY_NAME", "STATE_NAME", "COUNTY_NAME", "LOCATION"
]].drop_duplicates()
job_location.to_csv(OUTPUT_DIR / "job_location.csv", index=False)

# 4. SOC_Details
soc_details = df[[
    "ID",
    "SOC_2021_2", "SOC_2021_2_NAME",
    "SOC_2021_3", "SOC_2021_3_NAME",
    "SOC_2021_4", "SOC_2021_4_NAME",
    "SOC_2021_5", "SOC_2021_5_NAME"
]].drop_duplicates()
soc_details.to_csv(OUTPUT_DIR / "soc_details.csv", index=False)

# 5. LOT_Details
lot_details = df[[
    "ID",
    "LOT_CAREER_AREA", "LOT_CAREER_AREA_NAME",
    "LOT_OCCUPATION", "LOT_OCCUPATION_NAME",
    "LOT_SPECIALIZED_OCCUPATION", "LOT_SPECIALIZED_OCCUPATION_NAME"
]].drop_duplicates()
lot_details.to_csv(OUTPUT_DIR / "lot_details.csv", index=False)

# 6. NAICS_Details
naics_details = df[[
    "ID",
    "NAICS_2022_2", "NAICS_2022_2_NAME",
    "NAICS_2022_3", "NAICS_2022_3_NAME",
    "NAICS_2022_4", "NAICS_2022_4_NAME",
    "NAICS_2022_5", "NAICS_2022_5_NAME",
    "NAICS_2022_6", "NAICS_2022_6_NAME"
]].drop_duplicates()
naics_details.to_csv(OUTPUT_DIR / "naics_details.csv", index=False)

print("✅ Restructuring complete! Files saved in _output/")
