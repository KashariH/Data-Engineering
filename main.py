from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types as T
from pyspark.sql import functions as F

# Step 1: Initialize Spark Session

spark = SparkSession.builder \
    .appName("CDW_SAPP_ETL") \
    .config("spark.jars", "C:/Users/kashari.hutchins/OneDrive - Accenture/Desktop/Data Engineering Local Repo/mysql-connector-java-8.0.23.ja") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Step 2: MySQL Connection 

mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
mysql_properties = {
    "user": "root",
    "password": "password", 
    "driver": "com.mysql.cj.jdbc.Driver"
}
# -------------------------------
# EXTRACT SECTION
# -------------------------------
# Read customer data (JSON to Pandas, then Spark)

customer_pd = pd.read_json(r"C:\Users\cdw_sapp_customer.json")
customer_df = spark.createDataFrame(customer_pd)

# Read credit card data

credit_pd = pd.read_json(r"C:\Users\cdw_sapp_credit.json")
credit_pd['TIMEID'] = pd.to_datetime(credit_pd[['YEAR', 'MONTH', 'DAY']])
credit_pd.drop(['DAY', 'MONTH', 'YEAR'], axis=1, inplace=True)
credit_df = spark.createDataFrame(credit_pd)

# Read branch data

branch_pd = pd.read_json(r"C:\Users\cdw_sapp_branch.json")
branch_pd['BRANCH_PHONE'] = branch_pd['BRANCH_PHONE'].astype(str).str.zfill(10)
branch_pd['BRANCH_PHONE'] = branch_pd['BRANCH_PHONE'].apply(lambda x: f"({x[0:3]}){x[3:6]}-{x[6:]}")
branch_df = spark.createDataFrame(branch_pd)
# -------------------------------
# LOAD SECTION
# -------------------------------
def load_to_mysql(df, table_name):
    print(f"\nLoading into {table_name}...")
    df.write \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", table_name) \
        .option("user", mysql_properties["user"]) \
        .option("password", mysql_properties["password"]) \
        .option("driver", mysql_properties["driver"]) \
        .mode("overwrite") \
        .save()
    print(f"Successfully loaded:")
# -------------------------------
# RUN THE ETL PROCESS
# -------------------------------
if __name__ == "__main__":
    print(":mag: Previewing data before load...")
    print("\nCustomer:")



    customer_df.show(5, truncate=False)
    print("\nCredit Card:")

    credit_df.show(5, truncate=False)
    print("\nBranch:")

    branch_df.show(5, truncate=False)
    # Load all tables
    load_to_mysql(branch_df, "CDW_SAPP_BRANCH")
    load_to_mysql(credit_df, "CDW_SAPP_CREDIT_CARD")
    load_to_mysql(customer_df, "CDW_SAPP_CUSTOMER")
    print("\n ETL Process Completed Successfully.")
    spark.stop()


from pyspark.sql import SparkSession
# Create or retrieve the Spark session
spark = SparkSession.builder \
    .appName("Read JSON Files") \
    .getOrCreate()
# Load JSON files
branch_df = spark.read.json(
    r"C:\Users\cdw_sapp_branch.json",
    multiLine=True
)
credit_df = spark.read.json(
    r"C:\Users\cdw_sapp_credit.json",
    multiLine=True
)
customer_df = spark.read.json(
    r"C:\Users\cdw_sapp_customer.json",
    multiLine=True
)
print("\nBranch Data Schema:")
branch_df.printSchema()
print("\nBranch Data Sample:")
branch_df.show(5, truncate=False)
print("\nCredit Card Data Schema:")
credit_df.printSchema()
print("\nCredit Card Data Sample:")
credit_df.show(5, truncate=False)
print("\nCustomer Data Schema:")
customer_df.printSchema()
print("\nCustomer Data Sample:")
customer_df.show(5, truncate=False)
spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import col, sum
def init_spark(app_name, jar_path):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jar_path) \
        .getOrCreate()
def load_json_with_schema(spark, file_path, schema=None):
    if schema:
        return spark.read.json(file_path, schema=schema)
    else:
        return spark.read.json(file_path, multiLine=True)
def write_to_mysql(df, table_name, mysql_url, props):
    print(f"\n:inbox_tray: Loading into table: {table_name}")
    df.write.jdbc(url=mysql_url, table=table_name, mode="append", properties=props)
def check_nulls(df):
    print("\n:test_tube: Null Check:")
    df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
def check_duplicates(df, key):
    print(f"\n:mag: Duplicate Check on {key}:")
    df.groupBy(key).count().filter("count > 1").show()
def main():

    # Initialize Spark
    spark = init_spark("CreditCardCapstone", r"C:\Spark\jars\mysql-connector-java-8.0.33.jar")
    # Define MySQL connection
    mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
    mysql_props = {
        "user": "root",
        "password": "password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Define schema for branch
    branch_schema = StructType([
        StructField("BRANCH_CODE", LongType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("BRANCH_PHONE", StringType(), True),
        StructField("BRANCH_STATE", StringType(), True),
        StructField("BRANCH_STREET", StringType(), True),
        StructField("BRANCH_ZIP", LongType(), True),
        StructField("LAST_UPDATED", StringType(), True),
        StructField("BRANCH_CITY", StringType(), True)
    ])
    # Load JSON files
    branch_df = load_json_with_schema(spark, r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_branch.json", schema=branch_schema)
    credit_df = load_json_with_schema(spark, r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_credit.json")
    customer_df = load_json_with_schema(spark, r"C:\Users\kashari.hutchins\OneDrive - Accenture\Desktop\Data Engineering Local Repo\cdw_sapp_customer.json")

    # Show schemas
    print("\n:office: Branch Data Schema:")
    branch_df.printSchema()
    print("\n:credit_card: Credit Card Data Schema:")
    credit_df.printSchema()
    print("\n:bust_in_silhouette: Customer Data Schema:")
    customer_df.printSchema()

    # Show sample rows
    print("\n:office: Branch Sample:")
    branch_df.show(5, truncate=False)
    print("\n:credit_card: Credit Card Sample:")
    credit_df.show(5, truncate=False)
    print("\n:bust_in_silhouette: Customer Sample:")
    customer_df.show(5, truncate=False)

    # Check for nulls and duplicates
    check_nulls(branch_df)
    check_duplicates(branch_df, "BRANCH_CODE")

    # Load to MySQL
    write_to_mysql(branch_df, "cdw_sapp_branch", mysql_url, mysql_props)
    write_to_mysql(credit_df, "cdw_sapp_credit_card", mysql_url, mysql_props)
    write_to_mysql(customer_df, "cdw_sapp_customer", mysql_url, mysql_props)
    print("\n:white_check_mark: All data loaded successfully!")

    # Stop Spark session
    spark.stop()
if __name__ == "__main__":
    main()


# TRANSACTIONS CLI ZIP, MONTH & YEAR
import mysql.connector
from mysql.connector import Error
def get_transactions_by_zip_month_year():
    print("\n:mag: Search Transactions by ZIP, Month & Year")
    print("=" * 50)
    # Step 1: Get ZIP
    while True:
        zip_code = input("Enter a 5-digit ZIP code: ").strip()
        if zip_code.isdigit() and len(zip_code) == 5:
            break
        print(":x: Invalid ZIP code. Please enter exactly 5 digits.")
   
    # Step 2: Get Month
    while True:
        month = input("Enter month (1-12): ").strip()
        if month.isdigit() and 1 <= int(month) <= 12:
            month = int(month)
            break
        print(":x: Invalid month. Please enter a number from 1 to 12.")
   
    # Step 3: Get Year
    while True:
        year = input("Enter year (e.g. 2018): ").strip()
        if year.isdigit() and len(year) == 4:
            year = int(year)
            break
        print(":x: Invalid year. Please enter a 4-digit year.")
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='creditcard_capstone',
            user='root',
            password='password'
        )
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            query = """
                SELECT
                    c.CREDIT_CARD_NO,
                    c.TRANSACTION_TYPE,
                    c.TRANSACTION_VALUE,
                    c.YEAR,
                    c.MONTH,
                    c.DAY
                FROM CDW_SAPP_CREDIT_CARD c
                JOIN CDW_SAPP_CUSTOMER cu ON c.CREDIT_CARD_NO = cu.CREDIT_CARD_NO
                WHERE cu.CUST_ZIP = %s
                  AND c.MONTH = %s
                  AND c.YEAR = %s
                ORDER BY c.DAY DESC;
            """
            cursor.execute(query, (zip_code, month, year))
            results = cursor.fetchall()
            print("\n:bar_chart: Results:")
            print(f"Transactions for ZIP: {zip_code} | Date: {month:02}/{year}")
            print("-" * 70)
            if results:
                for row in results:
                    print(f"{row['YEAR']}-{row['MONTH']:02}-{row['DAY']:02} | "
                          f":credit_card: Card: {row['CREDIT_CARD_NO']} | "
                          f":moneybag: Amount: ${row['TRANSACTION_VALUE']} | "
                          f":open_file_folder: Type: {row['TRANSACTION_TYPE']}")
            else:
                print(":warning:  No transactions found for that ZIP, month, and year.")
    except Error as e:
        print(":exclamation: Database error:", e)
    except Exception as e:
        print(":exclamation: Unexpected error:", e)
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("\n:white_check_mark: Connection closed.")
# Run the function
get_transactions_by_zip_month_year()


# CLI MODIFY CUSTOMER INFO
import mysql.connector
import re
import os
ALLOWED_CUSTOMER_FIELDS = [
    'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CREDIT_CARD_NO',
    'STREET_NAME', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY',
    'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL'
]
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
def pause():
    input("\nPress Enter to return to the main menu...")
def connect_to_db():
    return mysql.connector.connect(
        host='localhost',
        port=3306,
        user='root',
        password='password',
        database='creditcard_capstone'
    )
def get_validated_input(prompt, pattern, error_message):
    while True:
        value = input(prompt).strip()
        if re.fullmatch(pattern, value):
            return value
        else:
            print(error_message)
def view_customer_info_by_ssn(conn, ssn):
    cursor = conn.cursor()
    query = "SELECT * FROM cdw_sapp_customer WHERE SSN = %s"
    cursor.execute(query, (ssn,))
    row = cursor.fetchone()
    if row:
        columns = [desc[0] for desc in cursor.description]
        print("\n--- Customer Information ---")
        for col, val in zip(columns, row):
            print(f"{col}: {val}")
        print()
    else:
        print("No customer found with that SSN.\n")
    cursor.close()
def view_customer_info(conn):
    ssn = get_validated_input("Enter Customer SSN (9 digits): ", r"\d{9}", "SSN must be 9 digits")
    view_customer_info_by_ssn(conn, ssn)
def modify_customer_details(conn):
    clear_screen()
    ssn = get_validated_input("Enter SSN of the customer to update (9 digits): ", r"\d{9}", "SSN must be 9 digits")
    print("\nFields you can update:")
    for field in ALLOWED_CUSTOMER_FIELDS:
        print(f"  - {field}")
    field = input("\nEnter the field to update exactly as shown above: ").strip().upper()
    if field not in ALLOWED_CUSTOMER_FIELDS:
        print("\nInvalid field name. Please select a valid field from the list.")
        pause()
        return
    value = input(f"Enter new value for {field}: ").strip()
    try:
        cursor = conn.cursor()
        query = f"UPDATE cdw_sapp_customer SET {field} = %s WHERE SSN = %s"
        cursor.execute(query, (value, ssn))
        conn.commit()
        if cursor.rowcount > 0:
            print("\nCustomer details updated successfully.")
        else:
            print("\nNo customer found with the provided SSN.")
        cursor.close()
    except mysql.connector.Error as e:
        print(f"\nDatabase error: {e}")
    finally:
        pause()
def generate_monthly_bill(conn):
    card_number = input("Enter credit card number: ").strip()
    while True:
        try:
            month = int(input("Enter billing month (1-12): ").strip())
            if 1 <= month <= 12:
                break
            print("Month must be between 1 and 12.")
        except ValueError:
            print("Invalid input. Enter a number.")
    year = int(input("Enter billing year (e.g., 2024): ").strip())
    cursor = conn.cursor()
    query = """
    SELECT t.TRANSACTION_ID, t.DAY, t.MONTH, t.YEAR, t.CUST_SSN, t.CREDIT_CARD_NO,
           t.CUST_ZIP, t.TRANSACTION_TYPE, t.TRANSACTION_VALUE, t.BRANCH_CODE
    FROM credit_card t
    WHERE t.CREDIT_CARD_NO = %s AND t.MONTH = %s AND t.YEAR = %s
    ORDER BY t.YEAR DESC, t.MONTH DESC, t.DAY DESC
    """
    try:
        cursor.execute(query, (card_number, month, year))
        transactions = cursor.fetchall()
        total = sum(row[8] for row in transactions)
        print(f"\n--- Monthly Bill for Card {card_number} ({month:02d}/{year}) ---")
        print(f"Total Due: ${total:.2f}")
        print("\nTransactions:")
        for row in transactions:
            print(f"Date: {row[3]:04d}-{row[2]:02d}-{row[1]:02d}, Amount: ${row[8]:.2f}, Type: {row[7]}")
        print()
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        cursor.close()
def get_transactions_by_date_range(conn):
    ssn = get_validated_input("Enter Customer SSN (9 digits): ", r"\d{9}", "SSN must be 9 digits")
    start_date = input("Enter start date (YYYY-MM-DD): ").strip()
    end_date = input("Enter end date (YYYY-MM-DD): ").strip()
    cursor = conn.cursor()
    query = """
    SELECT YEAR, MONTH, DAY, TRANSACTION_VALUE, TRANSACTION_TYPE
    FROM credit_card
    WHERE CUST_SSN = %s AND STR_TO_DATE(CONCAT(YEAR, '-', MONTH, '-', DAY), '%%Y-%%m-%%d')
          BETWEEN %s AND %s
    ORDER BY YEAR DESC, MONTH DESC, DAY DESC
    """
    try:
        cursor.execute(query, (ssn, start_date, end_date))
        results = cursor.fetchall()
        if not results:
            print("\nNo transactions found for the given date range.\n")
        else:
            print("\n--- Transactions Between Dates ---")
            for row in results:
                print(f"Date: {row[0]:04d}-{row[1]:02d}-{row[2]:02d}, Amount: ${row[3]:.2f}, Type: {row[4]}")
            print()
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        cursor.close()
def get_transactions(conn):
    print(":wrench: This function is not yet implemented.")
    pause()
def get_customer_months_years(conn):
    print(":wrench: This function is not yet implemented.")
    pause()
def main():
    try:
        conn = connect_to_db()
    except mysql.connector.Error as err:
        print(f"Failed to connect to database: {err}")
        return
    while True:
        clear_screen()
        print("\n--- Main Menu ---")
        print("1. View Transaction Dates by Zip/Month/Year")
        print("2. View Customer's Transaction Months/Years by SSN")
        print("3. View Existing Customer Account")
        print("4. Modify Customer Account Info")
        print("5. Generate Monthly Bill")
        print("6. View Transactions Between Two Dates")
        print("7. Exit")
        choice = input("Choose an option: ").strip()
        if choice == "1":
            get_transactions(conn)
        elif choice == "2":
            get_customer_months_years(conn)
        elif choice == "3":
            view_customer_info(conn)
        elif choice == "4":
            modify_customer_details(conn)
        elif choice == "5":
            generate_monthly_bill(conn)
        elif choice == "6":
            get_transactions_by_date_range(conn)
        elif choice == "7":
            print("Goodbye!")
            conn.close()
            break
        else:
            print("Invalid option. Try again.")
            pause()
if __name__ == "__main__":
    main()


# Visualize the transactions type counts using matplotlib

import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
def fetch_and_plot_transaction_types():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="creditcard_capstone"
        )
        # Query to count transaction types
        query = """
        SELECT TRANSACTION_TYPE, COUNT(*) AS count
        FROM CDW_SAPP_CREDIT_CARD
        GROUP BY TRANSACTION_TYPE
        ORDER BY count DESC;
        """
        # Read into pandas DataFrame
        credit_db = pd.read_sql(query, conn)
        conn.close()
        # Plotting
        plt.figure(figsize=(10, 6))
        plt.bar(credit_db['TRANSACTION_TYPE'], credit_db['count'], color='green')
        plt.title('Transaction Type Count')
        plt.xlabel('Transaction Type')
        plt.ylabel('Number of Transactions')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
if __name__ == "__main__":
    fetch_and_plot_transaction_types() 

# Visualize top 10 states with the most customers using matplotlib 
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
def plot_top_states_by_customers():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="creditcard_capstone"
        )
        # SQL query to get top 10 states by customer count
        query = """
        SELECT CUST_STATE, COUNT(*) AS count
        FROM CDW_SAPP_CUSTOMER
        GROUP BY CUST_STATE
        ORDER BY count DESC
        LIMIT 10;
        """
        # Load the query result into a DataFrame
        credit_db = pd.read_sql(query, conn)
        conn.close()
        # Plotting
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.bar(credit_db['CUST_STATE'], credit_db['count'], color='pink')
        plt.xticks(rotation=0)
        plt.xlabel('State')
        plt.ylabel('Number of Customers')
        plt.title('Top 10 States by Customers')
        plt.tight_layout()
        plt.show()
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
if __name__ == "__main__":
    plot_top_states_by_customers() 


#Visualize top 10 customers by spending using matplotlib
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
def plot_top_customers_by_spending():
    try:
        # Open MySQL connection
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="creditcard_capstone"
        )
        # SQL query to get top 10 customers by total spending
        query = """
        SELECT CONCAT(FIRST_NAME, ' ', LAST_NAME) AS CUSTOMER_NAME, SUM(TRANSACTION_VALUE) AS TOTAL_SPENT
        FROM CDW_SAPP_CREDIT_CARD AS cc
        JOIN CDW_SAPP_CUSTOMER AS cu ON cc.CUST_SSN = cu.SSN
        GROUP BY CUSTOMER_NAME
        ORDER BY TOTAL_SPENT DESC
        LIMIT 10;
        """
        # Load data into DataFrame
        df = pd.read_sql(query, conn)
        print(df.head())  

        df.plot(kind='bar', x='CUSTOMER_NAME', y='TOTAL_SPENT',
                title='Top 10 Customers by Spending', color='brown', legend=False)
        plt.ylabel('Total Spent ($)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig("top_10_customers_by_spending.png")  # Saves the plot as an image
        plt.show()
        conn.close()
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
if __name__ == "__main__":
    plot_top_customers_by_spending()

# Loan API 
import requests
from pyspark.sql import SparkSession
def fetch_and_load_loan_data():
    # Step 1: Start Spark session
    spark = SparkSession.builder \
        .appName("Loan Application ETL") \
        .config("spark.jars", "/path/to/mysql-connector-java-8.0.33.jar") \
        .getOrCreate()
    # Step 2: Fetch loan data from API
    response = requests.get("https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json")

    # Step 3: Check API status
    print("Status Code:", response.status_code)
    if response.status_code == 200:
        data = response.json() 

        # Step 4: Convert to Spark DataFrame
        df = spark.read.json(spark.sparkContext.parallelize(data))
        # Optional: Preview schema or top rows
        df.printSchema()
        df.show(5)

        # Step 5: Write to MySQL
        mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
        mysql_properties = {
            "user": "root",
            "password": "password",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        df.write \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", "CDW_SAPP_loan_application") \
            .option("user", mysql_properties["user"]) \
            .option("password", mysql_properties["password"]) \
            .option("driver", mysql_properties["driver"]) \
            .mode("overwrite") \
            .save()
        print(":white_check_mark: Data successfully loaded into MySQL!")
    else:
        print(":x: Failed to fetch data from the API.")
if __name__ == "__main__":
    fetch_and_load_loan_data()


# Data Visualizations from the LOAN API
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
def visualize_loan_status_self_employed():
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="creditcard_capstone"
        )
        # Query loan application status by self-employment
        query = """
        SELECT Self_Employed, Application_Status
        FROM CDW_SAPP_LOAN_APPLICATION
        """
        # Load data into pandas DataFrame
        credit_db = pd.read_sql(query, conn)
    except mysql.connector.Error as e:
        print(f"Database connection failed: {e}")
        return
    finally:
        if conn.is_connected():
            conn.close()
    # Clean and normalize the data
    credit_db['Self_Employed'] = credit_db['Self_Employed'].str.strip().str.lower()
    credit_db['Application_Status'] = credit_db['Application_Status'].str.strip().str.capitalize()
    # Filter for self-employed applicants only
    self_employed = credit_db[credit_db['Self_Employed'] == 'yes']
    # Count the number of each Application_Status
    status_counts = self_employed['Application_Status'].value_counts()
    # Plot pie chart
    plt.figure(figsize=(6, 6))
    colors = ['#4CAF50', '#F44336']  # Green and red for Approved and Rejected
    plt.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%', colors=colors)
    plt.title('Loan Application Status for Self-Employed Applicants')
    plt.tight_layout()
    plt.savefig('self_employed_approval_percentage.png')  # Save plot as .png
    plt.show()
if __name__ == "__main__":
    visualize_loan_status_self_employed() 

# CREDIT CARD VISUALIZATIONS 

# TOTAL CREDIT CARD SPENDING BY MONTH 
import pandas as pd  # For data manipulation and analysis
import matplotlib.pyplot as plt  # For plotting
import seaborn as sns  # For enhanced visualization
import calendar  # To convert month numbers to month names
# Set seaborn plot style
sns.set(style="whitegrid")
def plot_monthly_spending():
    
    credit_data = pd.DataFrame({
        "Month": [2, 3, 7, 4, 10, 5, 5, 8, 3, 9, 8, 12, 4, 4, 5, 7, 9],
        "Amount": [
            78.9, 14.24, 56.7, 59.73, 3.59, 6.89, 43.39, 95.39, 93.26, 100.38,
            98.75, 42.71, 40.24, 17.81, 29.0, 70.63, 27.04
        ]
    })
    # Group by month and sum the amounts
    monthly_spending = credit_data.groupby("Month")["Amount"].sum().reset_index()
    # Convert numeric month to month names
    monthly_spending["Month"] = monthly_spending["Month"].apply(lambda x: calendar.month_name[x])
    # Ensure calendar order
    monthly_spending["Month"] = pd.Categorical(
        monthly_spending["Month"],
        categories=list(calendar.month_name)[1:],  # Exclude empty string at index 0
        ordered=True
    )
    monthly_spending = monthly_spending.sort_values("Month")
  
    # Create line chart
    plt.figure(figsize=(10, 6))
    plt.plot(monthly_spending["Month"], monthly_spending["Amount"], marker='o')
    plt.title("Total Credit Card Spending by Month")
    plt.xlabel("Month")
    plt.ylabel("Total Amount Spent")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("monthly_credit_spending.png")  
    plt.show()
if __name__ == "__main__":
    plot_monthly_spending()


# SPENDING BY CATEGORY BAR CHART
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# Set seaborn style
sns.set(style="whitegrid")
def plot_category_spending():


    credit_data = pd.DataFrame({
        "Month": [2, 3, 7, 4, 10, 5, 5, 8, 3, 9, 8, 12, 4, 4, 5, 7, 9],
        "Amount": [
            78.9, 14.24, 56.7, 59.73, 3.59, 6.89, 43.39, 95.39, 93.26, 100.38,
            98.75, 42.71, 40.24, 17.81, 29.0, 70.63, 27.04
        ]
    })
    # Add category column
    credit_data["Category"] = [
        "Education", "Entertainment", "Grocery", "Entertainment", "Gas", "Education", "Entertainment",
        "Gas", "Entertainment", "Bills", "Gas", "Gas", "Grocery", "Bills", "Bills", "Test", "Test"
    ]
    # Group and sort by category spending
    category_spending = credit_data.groupby("Category")["Amount"].sum().sort_values(ascending=False).reset_index()
    # Plot
    plt.figure(figsize=(10, 6))
    sns.barplot(data=category_spending, x="Amount", y="Category", palette="viridis")
    plt.title("Total Spending by Category")
    plt.xlabel("Total Amount Spent")
    plt.ylabel("Category")
    plt.tight_layout()
    plt.savefig("category_spending.png")  
    plt.show()
if __name__ == "__main__":
    plot_category_spending()

# HEATMAP VISUALIZATION 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
def plot_monthly_category_heatmap():
   
    credit_data = pd.DataFrame({
        "Month": [2, 3, 7, 4, 10, 5, 5, 8, 3, 9, 8, 12, 4, 4, 5, 7, 9],
        "Amount": [
            78.9, 14.24, 56.7, 59.73, 3.59, 6.89, 43.39, 95.39, 93.26, 100.38,
            98.75, 42.71, 40.24, 17.81, 29.0, 70.63, 27.04
        ],
        "Category": [
            "Education", "Entertainment", "Grocery", "Entertainment", "Gas", "Education", "Entertainment",
            "Gas", "Entertainment", "Bills", "Gas", "Gas", "Grocery", "Bills", "Bills", "Test", "Test"
        ]
    })
    # Pivot table to prepare heatmap data
    monthly_category = credit_data.groupby(["Month", "Category"])["Amount"].sum().unstack(fill_value=0)
    # Plot heatmap
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.patch.set_facecolor('red')
    ax.set_facecolor('red')
    sns.heatmap(monthly_category, annot=True, fmt=".1f", cmap="YlGnBu", linewidths=0.5, ax=ax)
    # Customize plot aesthetics
    plt.title("Monthly Spending by Category (Heatmap)", color='white')
    plt.xlabel("Category", color='white')
    plt.ylabel("Month", color='white')
    ax.tick_params(colors='white')
    plt.tight_layout()
    plt.savefig("monthly_spending_heatmap.png")  # Optional: Save the heatmap as an image
    plt.show()
if __name__ == "__main__":
    plot_monthly_category_heatmap()


# LOAN APPROVAL BY INCOME
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

income_loan = pd.DataFrame({
    'Income_Bracket': ['Low', 'Medium', 'High'],
    'Approval_Rate': [0.4, 0.6, 0.85]
})
# Set style
sns.set(style="whitegrid")
# Create the plot
plt.figure(figsize=(8, 6))
bar = sns.barplot(
    x="Income_Bracket",
    y="Approval_Rate",
    data=income_loan,
    palette="coolwarm"
)
# Add labels
for index, value in enumerate(income_loan["Approval_Rate"]):
    bar.text(index, value + 0.02, f"{value:.2f}", ha='center', color='black')
# Customize the plot
plt.title("Loan Approval Rate by Income Bracket")
plt.xlabel("Income Bracket")
plt.ylabel("Approval Rate")
plt.ylim(0, 1.1)
plt.tight_layout()
# Display it
plt.show()

# Loan Approval by Gender & Employment 
import pandas as pd
import matplotlib.pyplot as plt
# Sample loan application data
loan_data = pd.DataFrame({
    "Loan_Status": ['N', 'Y', 'Y', 'Y', 'N', 'N', 'N', 'Y', 'Y', 'N', 'N', 'Y', 'Y', 'Y', 'N', 'N', 'Y', 'Y', 'N', 'Y'],
    "Self_Employed": ['Yes', 'No', 'No', 'No', 'No', 'Yes', 'No', 'No', 'No', 'Yes', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No'],
    "Gender": ['Male', 'Male', 'Male', 'Male', 'Male', 'Male', 'Male', 'Male', 'Male', 'Male', 'Male', 'Male', 'Male', 'Female', 'Male', 'Female', 'Male', 'Female', 'Male', 'Male']
})
# Clean and prepare data
loan_data["Loan_Status"] = loan_data["Loan_Status"].str.upper().str.strip() # Converts Loan_Status to uppercase and removes any leading/trailing whitespace.
loan_data["Self_Employed"] = loan_data["Self_Employed"].str.title().str.strip() # Converts Self_Employed to title case and removes any leading/trailing whitespace.
loan_data["Gender"] = loan_data["Gender"].str.title().str.strip()
loan_data["Employment_Type"] = loan_data["Self_Employed"].map({'No': 'Employed', 'Yes': 'Self-Employed'}) # Maps 'No' to 'Employed' and 'Yes' to 'Self-Employed' for better readability.
loan_data["Group"] = loan_data["Gender"] + " - " + loan_data["Employment_Type"]
# Calculate approval/rejection percentages
grouped = loan_data.groupby(["Group", "Loan_Status"]).size().unstack(fill_value=0) # Groups the data by 'Group' and 'Loan_Status', counting occurrences, and fills missing values with 0.
grouped_percent = grouped.div(grouped.sum(axis=1), axis=0) * 100 # Converts counts to percentages by dividing each count by the total count for that group and multiplying by 100.
# Plot stacked bar chart
ax = grouped_percent[['N', 'Y']].plot(
    kind='bar',
    stacked=True,
    color=['orange', 'blue'],
    figsize=(10, 6)
)
plt.title("Loan Approval Rate (%) by Gender and Employment Type") # Adds a title to the stacked bar chart.
plt.xlabel("Gender & Employment Type")
plt.ylabel("Approval Percentage")
plt.xticks(rotation=45)
plt.legend(title="Loan Status", labels=["Rejected", "Approved"])
plt.grid(axis='y') # Adds horizontal grid lines for better readability of the y-axis values.
# Add percentage labels on each stacked segment
for i, (index, row) in enumerate(grouped_percent.iterrows()):
    y_offset = 0
    for status in ['N', 'Y']:
        value = row[status]
        if value > 0:
            ax.text(i, y_offset + value / 2, f"{value:.1f}%", ha='center', va='center', fontsize=9, color='black') # Adds text labels to each segment of the stacked bar chart showing the percentage value with one decimal place.
            y_offset += value
plt.tight_layout()
plt.show()


# Loan Rejection by Property Area
import pandas as pd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Input data
loan_status = ['N', 'Y', 'Y', 'Y', 'N', 'N', 'N', 'Y', 'Y', 'N', 'N', 'Y', 'Y', 'Y', 'N', 'N', 'Y', 'Y']
property_area = [
    'Semiurban', 'Urban', 'Urban', 'Urban', 'Semiurban', 'Urban', 'Urban', 'Rural', 'Urban',
    'Semiurban', 'Rural', 'Semiurban', 'Rural', 'Semiurban', 'Semiurban', 'Urban', 'Rural', 'Urban'
]
# Create DataFrame
df = pd.DataFrame({
    "Loan_Status": loan_status,
    "Property_Area": property_area[:len(loan_status)]
})
# Filter rejected loans
rejections = df[df["Loan_Status"] == "N"]
# Count and percentage
rejection_counts = rejections["Property_Area"].value_counts().reindex(['Urban', 'Semiurban', 'Rural'])
rejection_percentages = (rejection_counts / rejection_counts.sum()) * 100
# Combine into single DataFrame
plot_df = pd.DataFrame({
    'Property_Area': rejection_counts.index,
    'Rejections': rejection_counts.values,
    'Percentage': rejection_percentages.values
})
# Sort for better visual flow
plot_df = plot_df.sort_values('Rejections')
# Plot setup
sns.set_style("whitegrid")
palette = sns.color_palette("Set2")
fig, ax = plt.subplots(figsize=(9, 6))
# Horizontal barplot
bars = sns.barplot(
    data=plot_df,
    y='Property_Area',
    x='Rejections',
    palette=palette,
    ax=ax
)
# Annotate inside bars
for i, (count, pct) in enumerate(zip(plot_df['Rejections'], plot_df['Percentage'])):
    ax.text(count - 0.1, i, f"{count} ({pct:.1f}%)",
            color='black', fontsize=11, fontweight='bold',
            va='center', ha='right' if count > 1 else 'left')
    
# Titles and labels
ax.set_title("Loan Rejections by Property Area", fontsize=16, fontweight='bold', pad=20)
ax.set_xlabel("Number of Rejections", fontsize=12)
ax.set_ylabel("")

sns.despine(left=True, bottom=True)
plt.tight_layout()
plt.show()






































 







































