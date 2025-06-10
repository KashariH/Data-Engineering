import mysql.connector
from mysql.connector import Error
def fetch_transactions(zip_code, month, year):
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone'
        )
        if conn.is_connected():
            cursor = conn.cursor(dictionary=True)
            month = str(month).zfill(2)
            zip_code = str(zip_code).zfill(5)
            timeid_prefix = f"{year}{month}"
            query = """
                SELECT
                    c.FIRST_NAME, c.LAST_NAME, c.CUST_ZIP, t.TRANSACTION_ID,
                    t.TRANSACTION_TYPE, t.TRANSACTION_VALUE, t.TIMEID
                FROM CDW_SAPP_CREDIT_CARD t
                JOIN CDW_SAPP_CUSTOMER c ON t.CUST_CC_NO = c.CREDIT_CARD_NO
                WHERE c.CUST_ZIP = %s AND t.TIMEID LIKE %s
                ORDER BY t.TRANSACTION_VALUE DESC
            """
            cursor.execute(query, (zip_code, timeid_prefix + "%"))
            results = cursor.fetchall()
            if results:
                print(f"\n:mag: Transactions for ZIP {zip_code} in {month}/{year}:")
                for row in results:
                    print(f" - {row['FIRST_NAME']} {row['LAST_NAME']}: ${row['TRANSACTION_VALUE']} ({row['TRANSACTION_TYPE']}) | TIMEID: {row['TIMEID']}")
            else:
                print(":x: No transactions found for given ZIP and date.")
    except Error as e:
        print(f":x: MySQL Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
# :large_green_circle: Main CLI Interface
if __name__ == "__main__":
    print(":credit_card: Transaction Lookup CLI")
    try:
        zip_input = input("Enter ZIP code (5 digits): ")
        month_input = input("Enter Month (1–12): ")
        year_input = input("Enter Year (only 2018 is allowed): ")
        if not (zip_input.isdigit() and len(zip_input) == 5):
            print(":x: Invalid ZIP. Must be exactly 5 digits.")
        elif not (month_input.isdigit() and 1 <= int(month_input) <= 12):
            print(":x: Invalid Month. Enter a number from 1 to 12.")
        elif year_input != "2018":
            print(":x: Only transactions from the year 2018 are supported.")
        else:
            fetch_transactions(zip_input, month_input, year_input)
    except KeyboardInterrupt:
        print("\n:octagonal_sign: Exiting CLI.")










