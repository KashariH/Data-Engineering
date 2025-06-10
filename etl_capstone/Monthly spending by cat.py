from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns

# Example: Load your data into credit_data DataFrame (replace with your actual data source)
# Uncomment and update the following line with your actual data file path
# credit_data = pd.read_csv('your_credit_data.csv')

# For demonstration, here's a sample DataFrame (remove this and use your actual data)
credit_data = pd.DataFrame({
	'Month': ['Jan', 'Jan', 'Feb', 'Feb', 'Mar', 'Mar'],
	'Category': ['Food', 'Transport', 'Food', 'Transport', 'Food', 'Transport'],
	'Amount': [120, 50, 130, 60, 110, 55]
})

monthly_category = credit_data.groupby(["Month", "Category"])["Amount"].sum().unstack(fill_value=0) # Reshape the data to have months as rows and categories as columns, filling missing values with 0

# Heatmap
fig, ax = plt.subplots(figsize=(12, 8)) # Create a figure and axes for the heatmap
fig.patch.set_facecolor('red') # Set the background color of the figure to red 
ax.set_facecolor('red') # Set the background color of the axes to red
sns.heatmap(monthly_category, annot=True, fmt=".1f", cmap="YlGnBu", linewidths=0.5, ax=ax)
plt.title("Monthly Spending by Category (Heatmap)", color='white') # Set the title color to white
plt.xlabel("Category", color='white') # Set the x-axis label color to white
plt.ylabel("Month", color = 'white') # Set the y-axis label color to white

ax.tick_params(colors='white') # Change the color of the tick labels to white
plt.show()