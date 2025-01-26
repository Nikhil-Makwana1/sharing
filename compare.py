import pandas as pd

# Load the CSV files
file1 = "file1.csv"  # Replace with your file path
file2 = "file2.csv"

# Read the files into dataframes
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Ensure the columns are in the same order
if list(df1.columns) != list(df2.columns):
    print("Column mismatch between files. Please ensure both files have the same structure.")
    exit()

# Initialize a dictionary to store column-wise anomalies
anomaly_summary = {}

# Variables to calculate overall anomalies
total_anomalies = 0
total_values = 0

# Dataframe to store differences
differences = pd.DataFrame(columns=["Row", "Column", "File1 Value", "File2 Value"])

# Compare the data column by column
for column in df1.columns:
    # Identify rows with differences in the current column
    mismatched_rows = df1[column] != df2[column]
    
    # Get differences
    diff_rows = df1[mismatched_rows].index
    rows_to_add = []
    for row in diff_rows:
        rows_to_add.append({
            "Row": row + 1,  # Row index starts from 1 for user-friendly display
            "Column": column,
            "File1 Value": df1.at[row, column],
            "File2 Value": df2.at[row, column]
        })
    
    # Add new rows to the differences DataFrame
    differences = pd.concat([differences, pd.DataFrame(rows_to_add)], ignore_index=True)
    
    # Count anomalies for the current column
    column_total_values = len(df1[column])
    column_anomalies = mismatched_rows.sum()
    column_anomaly_percentage = (column_anomalies / column_total_values) * 100
    
    # Save the result for the column
    anomaly_summary[column] = {
        "Total Values": column_total_values,
        "Anomalies": column_anomalies,
        "Anomaly Percentage": column_anomaly_percentage,
    }
    
    # Update overall anomalies
    total_anomalies += column_anomalies
    total_values += column_total_values

# Calculate overall and average anomaly percentages
overall_anomaly_percentage = (total_anomalies / total_values) * 100
average_anomaly_percentage = sum(
    stats["Anomaly Percentage"] for stats in anomaly_summary.values()
) / len(anomaly_summary)

# Print column-wise anomaly summary
print("\nColumn-wise Anomaly Summary:")
for column, stats in anomaly_summary.items():
    print(f"Column: {column}")
    print(f"  Total Values: {stats['Total Values']}")
    print(f"  Anomalies: {stats['Anomalies']}")
    print(f"  Anomaly Percentage: {stats['Anomaly Percentage']:.2f}%\n")

# Print overall statistics
print("Overall Statistics:")
print(f"Total Values: {total_values}")
print(f"Total Anomalies: {total_anomalies}")
print(f"Overall Anomaly Percentage: {overall_anomaly_percentage:.2f}%")
print(f"Average Anomaly Percentage Across Columns: {average_anomaly_percentage:.2f}%\n")

# Print the differences
print("\nDifferences Found:")
print(differences)

# Optionally, save the anomaly summary and differences to CSV files
summary_df = pd.DataFrame(anomaly_summary).T
summary_df["Overall Anomaly Percentage"] = overall_anomaly_percentage
summary_df["Average Anomaly Percentage"] = average_anomaly_percentage
summary_df.to_csv("column_anomaly_summary.csv")

differences.to_csv("differences.csv", index=False)
