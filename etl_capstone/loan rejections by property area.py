import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Input data
loan_status = ['N', 'Y', 'Y', 'Y', 'N', 'N', 'N', 'Y', 'Y', 'N', 'N', 'Y', 'Y', 'Y', 'N', 'N', 'Y', 'Y']
property_area = [
    'Semiurban', 'Urban', 'Urban', 'Urban', 'Semiurban', 'Urban', 'Urban', 'Rural', 'Urban',
    'Semiurban', 'Rural', 'Semiurban', 'Rural', 'Semiurban', 'Semiurban', 'Urban', 'Rural', 'Urban'
]

# Create DataFrame
df = pd.DataFrame({
    "Loan_Status": loan_status,
    "Property_Area": property_area[:len(loan_status)]
})

# Filter rejected loans
rejections = df[df["Loan_Status"] == "N"]

# Count and percentage
rejection_counts = rejections["Property_Area"].value_counts().reindex(['Urban', 'Semiurban', 'Rural'])
rejection_percentages = (rejection_counts / rejection_counts.sum()) * 100

# Combine into single DataFrame
plot_df = pd.DataFrame({
    'Property_Area': rejection_counts.index,
    'Rejections': rejection_counts.values,
    'Percentage': rejection_percentages.values
})

# Sort for better visual flow
plot_df = plot_df.sort_values('Rejections')

# Plot setup
sns.set_style("whitegrid")
palette = sns.color_palette("Set2")

fig, ax = plt.subplots(figsize=(9, 6))

# Horizontal barplot
bars = sns.barplot(
    data=plot_df,
    y='Property_Area',
    x='Rejections',
    palette=palette,
    ax=ax
)

# Annotate inside bars
for i, (count, pct) in enumerate(zip(plot_df['Rejections'], plot_df['Percentage'])):
    ax.text(count - 0.1, i, f"{count} ({pct:.1f}%)",
            color='black', fontsize=11, fontweight='bold',
            va='center', ha='right' if count > 1 else 'left')

# Titles and labels
ax.set_title("Loan Rejections by Property Area", fontsize=16, fontweight='bold', pad=20)
ax.set_xlabel("Number of Rejections", fontsize=12)
ax.set_ylabel("")

# Clean up
sns.despine(left=True, bottom=True)

plt.tight_layout()
plt.show()