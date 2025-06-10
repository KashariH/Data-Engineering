import mysql.connector # Lets you connect to and query a MySQL database
import pandas as pd # Used for data manipulation and to easily load SQL query results into a DataFrame
import matplotlib.pyplot as plt # Used to create the bar chart.

# Connect to database
conn = mysql.connector.connect( # Establishes a connection to MySQL database running on local machine.
    host="localhost",
    user="root",
    password="password",
    database="creditcard_capstone"
)

query = """
SELECT TRANSACTION_TYPE, COUNT(*) AS count # Selects each unique TRANSACTION_TYPE # Counts how many times each type appears
FROM CDW_SAPP_CREDIT_CARD
GROUP BY TRANSACTION_TYPE # Groups the results by TRANSACTION_TYPE
ORDER BY count DESC; # Sorts them in descending order (most frequent first)
"""
credit_db = pd.read_sql(query, conn) # Executes the SQL query and loads the results into a pandas DataFrame called df
conn.close() # Closes the database connection

plt.figure(figsize=(10, 6)) # Sets the figure size of the plot to 10 inches wide and 6 inches tall.

# Plot the bar chart
plt.bar(credit_db['TRANSACTION_TYPE'], credit_db['count'], color='green') # Creates a vertical bar chart. # The x-axis shows each TRANSACTION_TYPE. # The y-axis shows the corresponding count. # Bars are colored green.
plt.title('Transaction Type Count') # Adds a title and axis labels to the chart for better readability.
plt.xlabel('Transaction Type')
plt.ylabel('Number of Transactions')
plt.xticks(rotation=45, ha='right')  # Rotates the x-axis labels by 45 degrees # Aligns them to the right (ha='right') to prevent overlapping if names are long.

plt.tight_layout() # Adjusts spacing to prevent elements from being cut off.
plt.show() # Displays the chart 



