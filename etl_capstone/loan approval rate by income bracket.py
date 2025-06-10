import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
# Sample data (replace with your actual DataFrame)
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