import mysql.connector
from mysql.connector import Error
import pandas as pd
def connect_db():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone'
        )
        if conn.is_connected():
            print("Connected to MySQL")
            return conn
    except Error as e:
        print(f"MySQL Error: {e}")
    return None
def view_transactions_by_zip(conn):
    zip_code = input("Enter ZIP code: ").strip()
    month = input("Enter Month (01-12): ").zfill(2)
    year = input("Enter Year (e.g., 2018): ").strip()
    query = """
    SELECT c.FIRST_NAME, c.LAST_NAME, cc.TRANSACTION_ID, cc.TRANSACTION_TYPE, cc.TRANSACTION_VALUE, cc.TIMEID
    FROM CDW_SAPP_CREDIT_CARD cc
    JOIN CDW_SAPP_CUSTOMER c ON cc.CREDIT_CARD_NO = c.CREDIT_CARD_NO
    WHERE c.CUST_ZIP = %s AND cc.TIMEID LIKE %s
    ORDER BY cc.TIMEID DESC;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (zip_code, f"{year}{month}%"))
        results = cursor.fetchall()
        print("Transactions by ZIP/MONTH/YEAR:")
        for row in results:
            print(row)
    except Error as e:
        print(f":x: MySQL Error: {e}")
def view_customer_transactions_by_ssn(conn):
    ssn = input("Enter Customer SSN: ").strip()
    month = input("Enter Month (01-12): ").zfill(2)
    year = input("Enter Year (e.g., 2018): ").strip()
    query = """
    SELECT cc.TRANSACTION_ID, cc.TRANSACTION_TYPE, cc.TRANSACTION_VALUE, cc.TIMEID
    FROM CDW_SAPP_CREDIT_CARD cc
    JOIN CDW_SAPP_CUSTOMER c ON cc.CREDIT_CARD_NO = c.CREDIT_CARD_NO
    WHERE c.SSN = %s AND cc.TIMEID LIKE %s
    ORDER BY cc.TIMEID DESC;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (ssn, f"{year}{month}%"))
        results = cursor.fetchall()
        print("Customer Transactions by SSN and Date:")
        for row in results:
            print(row)
    except Error as e:
        print(f"MySQL Error: {e}")
def modify_customer_info(conn):
    ssn = input("Enter the SSN of the customer to update: ").strip()
    print("Enter new values (leave blank to skip):")
    updates = {}
    fields = [
        'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME',
        'CREDIT_CARD_NO', 'APT_NO', 'STREET_NAME',
        'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY',
        'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL'
    ]
    for field in fields:
        value = input(f"{field}: ").strip()
        if value:
            updates[field] = value
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        query = f"UPDATE CDW_SAPP_CUSTOMER SET {set_clause} WHERE SSN = %s"
        try:
            cursor = conn.cursor()
            cursor.execute(query, list(updates.values()) + [ssn])
            conn.commit()
            print(":white_check_mark: Customer record updated.")
        except Error as e:
            print(f":x: MySQL Error: {e}")
    else:
        print("No updates provided.")
def generate_monthly_bill(conn):
    ssn = input("Enter Customer SSN: ").strip()
    month = input("Enter Month (01-12): ").zfill(2)
    year = input("Enter Year (e.g., 2018): ").strip()
    query = """
    SELECT SUM(cc.TRANSACTION_VALUE) AS Monthly_Bill
    FROM CDW_SAPP_CREDIT_CARD cc
    JOIN CDW_SAPP_CUSTOMER c ON cc.CREDIT_CARD_NO = c.CREDIT_CARD_NO
    WHERE c.SSN = %s AND cc.TIMEID LIKE %s;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (ssn, f"{year}{month}%"))
        total = cursor.fetchone()
        print(f"\n:credit_card: Monthly Bill for {month}/{year}: ${total[0]:.2f}" if total and total[0] else "No transactions found.")
    except Error as e:
        print(f":x: MySQL Error: {e}")
def view_transactions_between_dates(conn):
    start_date = input("Enter Start Date (YYYYMMDD): ")
    end_date = input("Enter End Date (YYYYMMDD): ")
    query = """
    SELECT * FROM CDW_SAPP_CREDIT_CARD
    WHERE TIMEID BETWEEN %s AND %s
    ORDER BY TIMEID;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (start_date, end_date))
        results = cursor.fetchall()
        print("\n:date: Transactions between Dates:")
        for row in results:
            print(row)
    except Error as e:
        print(f":x: MySQL Error: {e}")
def main():
    conn = connect_db()
    if not conn:
        return
    while True:
        print("\n:pushpin: MAIN MENU")
        print("1. View transactions by ZIP/MONTH/YEAR")
        print("2. View customer transactions by SSN and MONTH/YEAR")
        print("3. Modify customer account information")
        print("4. Generate customer monthly bill")
        print("5. View transactions between two dates")
        print("6. Exit")
        choice = input("Enter your choice (1–6): ").strip()
        if choice == '1':
            view_transactions_by_zip(conn)
        elif choice == '2':
            view_customer_transactions_by_ssn(conn)
        elif choice == '3':
            modify_customer_info(conn)
        elif choice == '4':
            generate_monthly_bill(conn)
        elif choice == '5':
            view_transactions_between_dates(conn)
        elif choice == '6':
            print("Exiting the program successfully.")
            break
        else:
            print(":Invalid choice. Please select 1–6.")
    conn.close()
if __name__ == "__main__":
    main()











