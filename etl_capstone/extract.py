from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd

# Extract data from JSON files using Spark & Pandas

customer_df=pd.read_json(r"C:\Users\cdw_sapp_customer.json") 
print(customer_df.head(20))

credit_df = pd.read_json(r"C:\Users\cdw_sapp_credit.json")
print(credit_df.head(20))

branch_df = pd.read_json(r"C:\Users\cdw_sapp_branch.json")
print(branch_df.head(20)) 


