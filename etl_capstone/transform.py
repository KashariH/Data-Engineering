import pandas as pd
import json

# -------- Customer Data --------

customer_df = pd.read_json(r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_customer.json")
# Transform
customer_df['FIRST_NAME'] = customer_df['FIRST_NAME'].str.title()
customer_df['MIDDLE_NAME'] = customer_df['MIDDLE_NAME'].str.lower()
customer_df['LAST_NAME'] = customer_df['LAST_NAME'].str.title()

customer_df.to_json(
    r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_customer_cleaned.json",
    orient='records', indent=4
)
print("Cleaned Customer JSON saved.")

# -------- Credit Data + TIMEID --------

credit_df = pd.read_json(r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_credit.json")
# Transform
credit_df['DAY'] = credit_df['DAY'].astype(str).str.zfill(2)
credit_df['MONTH'] = credit_df['MONTH'].astype(str).str.zfill(2)
credit_df['YEAR'] = credit_df['YEAR'].astype(str)
credit_df['TIMEID'] = credit_df['YEAR'] + credit_df['MONTH'] + credit_df['DAY']
credit_df = credit_df.drop(columns=['DAY', 'MONTH', 'YEAR'])

credit_df.to_json(
    r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_credit_cleaned.json",
    orient='records', indent=4
)
print("Cleaned Credit JSON with TIMEID saved.")

# -------- Branch Data --------

branch_df = pd.read_json(r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_branch.json")
# Transform
branch_df['BRANCH_ZIP'] = branch_df['BRANCH_ZIP'].fillna("99999")
def format_phone(phone):
    digits = ''.join(filter(str.isdigit, str(phone)))
    return f"({digits[:3]}){digits[3:6]}-{digits[6:]}" if len(digits) == 10 else phone
branch_df['BRANCH_PHONE'] = branch_df['BRANCH_PHONE'].apply(format_phone)
# Save back to JSON
branch_df.to_json(
    r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_branch_cleaned.json",
    orient='records', indent=4
)
print("Cleaned Branch JSON saved.")









