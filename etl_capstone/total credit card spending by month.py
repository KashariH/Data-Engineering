import pandas as pd  # For data manipulation and analysis
import matplotlib.pyplot as plt  # For plotting the bar chart
import seaborn as sns  # For enhanced visualization with seaborn
import calendar  # To convert month numbers to month names

sns.set(style="whitegrid")  # Set the style of seaborn plots to "whitegrid" for better aesthetics

# Credit card transaction data
credit_data = pd.DataFrame({
    "Month": [2, 3, 7, 4, 10, 5, 5, 8, 3, 9, 8, 12, 4, 4, 5, 7, 9],
    "Amount": [
        78.9, 14.24, 56.7, 59.73, 3.59, 6.89, 43.39, 95.39, 93.26, 100.38,
        98.75, 42.71, 40.24, 17.81, 29.0, 70.63, 27.04
    ]
})

# Group by month and sum the amounts
monthly_spending = credit_data.groupby("Month")["Amount"].sum().reset_index()

# Map month numbers to month names
monthly_spending["Month"] = monthly_spending["Month"].apply(lambda x: calendar.month_name[x]) # Converts month numbers (1-12) to month names (January, February, etc.) using calendar.month_name.

# Sort months in calendar order
monthly_spending["Month"] = pd.Categorical(
    monthly_spending["Month"],
    categories=list(calendar.month_name)[1:],  # Exclude empty string at index 0
    ordered=True
)
monthly_spending = monthly_spending.sort_values("Month") # Sorts the DataFrame by month names in calendar order, ensuring that the months are displayed correctly in the chart.

# Line chart
plt.figure(figsize=(10, 6))
plt.plot(monthly_spending["Month"], monthly_spending["Amount"], marker='o') # Plots a line chart with month names on the x-axis and total amounts spent on the y-axis. The marker='o' adds circular markers at each data point for better visibility.
plt.title("Total Credit Card Spending by Month") # Adds a title to the line chart.
plt.xlabel("Month")
plt.ylabel("Total Amount Spent")
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.grid(True)
plt.tight_layout()
plt.show() 