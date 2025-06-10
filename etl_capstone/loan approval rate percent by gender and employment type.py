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