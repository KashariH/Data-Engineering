import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="creditcard_capstone"
)
query3 = """
SELECT CONCAT(FIRST_NAME, ' ', LAST_NAME) AS CUSTOMER_NAME, SUM(TRANSACTION_VALUE) AS TOTAL_SPENT
FROM CDW_SAPP_CREDIT_CARD AS cc
JOIN CDW_SAPP_CUSTOMER AS cu ON cc.CUST_CC_NO = cu.CREDIT_CARD_NO
GROUP BY CUSTOMER_NAME
ORDER BY TOTAL_SPENT DESC
LIMIT 10;
"""
df3 = pd.read_sql(query3, conn)
print(df3.head())
df3.plot(kind='bar', x='CUSTOMER_NAME', y='TOTAL_SPENT', title='Top 10 Customers by Spending', color='brown', legend=False)
plt.ylabel('Total Spent ($)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig("top_10_customers_by_spending.png")
plt.show()
conn.close()

















