import pandas as pd

# Load the output CSV file
input_file = "AHD_Reporting_Form_Out.csv"  # Replace with your actual file path
df_output = pd.read_csv(input_file)

# Ensure that 'period' is treated as a string
df_output['period'] = df_output['period'].astype(str)

# Initialize an empty list to hold rows for the original structure
rows = []

# Define reverse mappings
month_num_map = {
    '01': 'January', '02': 'February', '03': 'March',
    '04': 'April', '05': 'May', '06': 'June',
    '07': 'July', '08': 'August', '09': 'September',
    '10': 'October', '11': 'November', '12': 'December'
}

# Initialize a row dictionary to group by Id
row_dict = {}

# Loop through each row in the output DataFrame
for index, row in df_output.iterrows():
    period = row['period']
    
    # Ensure period is a string
    if isinstance(period, int):
        period = str(period)
    
    # Extract the reporting year and month
    reporting_year = period[:4]
    reporting_month_num = period[4:6]
    reporting_month = month_num_map.get(reporting_month_num)

    # Check if reporting_month is valid
    if reporting_month is None:
        print(f"Skipping row {index} due to invalid reporting month: {reporting_month_num}")
        continue

    # Create a unique key for each group
    key = (row['orgUnit'], reporting_year, reporting_month)

    # Initialize the row_dict if not already present
    if key not in row_dict:
        row_dict[key] = {
            'Id': len(row_dict) + 1,
            'Start time': '',  # Fill with actual data if available
            'Completion time': '',  # Fill with actual data if available
            'Email': '',  # Fill with actual data if available
            'Name': '',  # Fill with actual data if available
            'Facility': row['orgUnit'],
            'Reporting_year': reporting_year,
            'Reporting_Quarter': '',  # Fill based on your requirement
            'Reporting month': reporting_month,
            'period': period,
            'Startdate': '',  # Fill with actual data if available
            'Enddate': ''  # Fill with actual data if available
        }
    
    # Set the value for the corresponding data element
    row_dict[key][row['dataElement']] = row['value']

# Convert the row_dict back to a list of DataFrames
for row in row_dict.values():
    rows.append(row)

# Create the original DataFrame from the list of rows
df_original = pd.DataFrame(rows)

# Save the reconstructed DataFrame to an Excel file
output_file = "Reconstructed_AHD_Reporting_Form.xlsx"  # Replace with your desired output path
df_original.to_excel(output_file, index=False)
print(f"Reconstructed data saved to {output_file}")
