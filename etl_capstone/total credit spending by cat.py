from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns

# Example data for credit_data
credit_data = pd.DataFrame({
    "Amount": [100, 50, 200, 75, 60, 120, 90, 40, 110, 80, 30, 55, 160, 70, 95, 20, 25]
})


credit_data["Category"] = [
    "Education", "Entertainment", "Grocery", "Entertainment", "Gas", "Education", "Entertainment",
    "Gas", "Entertainment", "Bills", "Gas", "Gas", "Grocery", "Bills", "Bills", "Test", "Test"
]

category_spending = credit_data.groupby("Category")["Amount"].sum().sort_values(ascending=False).reset_index() # Sorts the total spending by category in descending order and resets the index for better readability.

# Bar chart
plt.figure(figsize=(10, 6)) # Creates a new figure with a specified size of 10 inches wide and 6 inches tall.
sns.barplot(data=category_spending, x="Amount", y="Category", palette="viridis") # Creates a horizontal bar chart using seaborn's barplot function. The x-axis represents the total amount spent, and the y-axis represents the categories.
plt.title("Total Spending by Category") # Adds a title to the bar chart.
plt.xlabel("Total Amount Spent") # Adds a label to the x-axis.
plt.ylabel("Category") # Adds a label to the y-axis.
plt.show() 