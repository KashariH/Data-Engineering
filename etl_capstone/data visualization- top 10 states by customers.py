import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt # Bar plot with matplotlib directly for more control

# Connect to the database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="creditcard_capstone"
)

# Query the database
query = """
SELECT CUST_STATE, COUNT(*) AS count
FROM CDW_SAPP_CUSTOMER
GROUP BY CUST_STATE
ORDER BY count DESC
LIMIT 10;
"""

credit_db = pd.read_sql(query, conn) # Executes the SQL query and loads the results into a pandas DataFrame called credit_db
conn.close()

# Plotting
fig, ax = plt.subplots(figsize=(10, 6)) # Sets the figure size of the plot to 10 inches wide and 6 inches tall.
bars = ax.bar(credit_db['CUST_STATE'], credit_db['count'], color='pink') # Bars are colored pink 
plt.xticks(rotation=0)  # Rotating x-axis labels for better readability 

# Labeling
plt.xlabel('State') # Adding x-axis labels 
plt.ylabel('Number of Customers') # Adding y-axis labels
plt.title('Top 10 States by Customers') # Graph title 

plt.tight_layout()
plt.show()