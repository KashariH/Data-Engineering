import pandas as pd
import mysql.connector
from mysql.connector import Error, IntegrityError
# Paths
customer_json = r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_customer.json"
credit_csv = r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_credit.csv"
branch_csv = r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_branch.csv"
try:
    # Connect to MySQL
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='password',
        database='creditcard_capstone'
    )
    if conn.is_connected():
        print(":white_check_mark: Connected to MySQL")
        cursor = conn.cursor()
        ### Load CUSTOMER Data ###

        customer_df = pd.read_json(customer_json)
        for _, row in customer_df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO CDW_SAPP_CUSTOMER (
                        SSN, FIRST_NAME, MIDDLE_NAME, LAST_NAME,
                        CREDIT_CARD_NO, APT_NO, STREET_NAME, CUST_CITY,
                        CUST_STATE, CUST_COUNTRY, CUST_ZIP,
                        CUST_PHONE, CUST_EMAIL
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['SSN'], row['FIRST_NAME'], row['MIDDLE_NAME'], row['LAST_NAME'],
                    row['CREDIT_CARD_NO'], row['APT_NO'], row['STREET_NAME'], row['CUST_CITY'],
                    row['CUST_STATE'], row['CUST_COUNTRY'], row['CUST_ZIP'],
                    row['CUST_PHONE'], row['CUST_EMAIL']
                ))
            except IntegrityError as e:
                print(f":warning: Skipping duplicate or error for SSN {row['SSN']}: {e}")
        conn.commit()
        print(":white_check_mark: Customer data loaded.")

        ### Load CREDIT Data ###
        credit_df = pd.read_csv(credit_csv)

        # Rename to match MySQL schema
        credit_df.rename(columns={"CREDIT_CARD_NO": "CUST_CC_NO"}, inplace=True)
        for _, row in credit_df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO CDW_SAPP_CREDIT_CARD (
                        TRANSACTION_ID, CUST_CC_NO, TIMEID, TRANSACTION_TYPE,
                        TRANSACTION_VALUE, BRANCH_CODE
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row['TRANSACTION_ID'], row['CUST_CC_NO'], row['TIMEID'],
                    row['TRANSACTION_TYPE'], row['TRANSACTION_VALUE'], row['BRANCH_CODE']
                ))
            except IntegrityError as e:
                print(f":warning: Skipping credit row: {e}")
        conn.commit()
        print(":white_check_mark: Credit card data loaded.")
        
        ### Load BRANCH Data ###
        branch_df = pd.read_csv(branch_csv)
        for _, row in branch_df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO CDW_SAPP_BRANCH (
                        BRANCH_CODE, BRANCH_NAME, BRANCH_STREET, BRANCH_CITY,
                        BRANCH_STATE, BRANCH_ZIP, BRANCH_PHONE, LAST_UPDATED
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['BRANCH_CODE'], row['BRANCH_NAME'], row['BRANCH_STREET'], row['BRANCH_CITY'],
                    row['BRANCH_STATE'], row['BRANCH_ZIP'], row['BRANCH_PHONE'], row['LAST_UPDATED']
                ))
            except IntegrityError as e:
                print(f":warning: Skipping branch row: {e}")
        conn.commit()
        print(":white_check_mark: Branch data loaded.")
except Error as e:
    print(":x: MySQL error:", e)
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print(":lock: MySQL connection closed.")






