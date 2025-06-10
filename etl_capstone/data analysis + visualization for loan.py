import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# Connect to the database
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",        
        database="creditcard_capstone"
    )

    # Query to get loan application status by self-employment
    query = """
    SELECT Self_Employed, Application_Status
    FROM CDW_SAPP_LOAN_APPLICATION
    """

    # Load data into pandas DataFrame
    credit_db = pd.read_sql(query, conn)

finally:
    conn.close()

# Normalize casing to avoid mismatch
credit_db['Self_Employed'] = credit_db['Self_Employed'].str.strip().str.lower()
credit_db['Application_Status'] = credit_db['Application_Status'].str.strip().str.capitalize()

# Filter for self-employed applicants
self_employed = credit_db[credit_db['Self_Employed'] == 'yes']

# Count application statuses
status_counts = self_employed['Application_Status'].value_counts()

# Plot
plt.figure(figsize=(6, 6))
colors = ['#4CAF50', '#F44336']  # Green and red
plt.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%', colors=colors)
plt.title('Loan Application Status for Self-Employed Applicants')
plt.savefig('self_employed_approval_percentage.png')  # Save as .png
plt.show()