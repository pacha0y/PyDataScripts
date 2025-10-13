import pandas as pd
import os

print(os.getcwd())

# Load the input sheet
base_path = r"C:/Users/PachawoBisani/Documents/Data/PyScripts/DHIS2 Imports/"
input_file = f"{base_path}AHD_Reporting_Form.xlsx"  # Replace with your actual file path
df_input = pd.read_excel(input_file, sheet_name='AHD_Reporting_Form')

# Initialize the output dataframe
columns = ['Id', 'Start time', 'Completion time', 'Email', 'Name', 'Facility', 'Reporting_year', 
           'Reporting_Quarter', 'Reporting month', 'period', 'Startdate', 'Enddate', 'data element', 'value']
df_output = pd.DataFrame(columns=columns)

# Define the reporting year and quarter mapping
quarter_map = {
    'January': 'Quarter1', 'February': 'Quarter1', 'March': 'Quarter1',
    'April': 'Quarter2', 'May': 'Quarter2', 'June': 'Quarter2',
    'July': 'Quarter3', 'August': 'Quarter3', 'September': 'Quarter3',
    'October': 'Quarter4', 'November': 'Quarter4', 'December': 'Quarter4'
}

# Define the numerical month mapping
# month_num_map = {
#     'January': '01', 'February': '02', 'March': '03',
#     'April': '04', 'May': '05', 'June': '06',
#     'July': '07', 'August': '08', 'September': '09',
#     'October': '10', 'November': '11', 'December': '12'
# }
month_num_map = {
    'Jan': '01', 'Feb': '02', 'Mar': '03',
    'Apr': '04', 'May': '05', 'Jun': '06',
    'Jul': '07', 'Aug': '08', 'Sep': '09',
    'Oct': '10', 'Nov': '11', 'Dec': '12'
}

data_elements_map = {
    "Total_number_of_ROC": "asQqkAud6WJ",
    "CD4<200": "zrbmZYhP2gO",
    "CD4 >= 200": "YvFoTQA3IPM",
    "Chest_Xlay_abnormal": "vRfx4AqZZ7m",
    "Chest_Xlay normal": "TfDcxklIjy2",
    "CrAg Neg": "bElFGfkznhB",
    "CrAg+": "uTNTZR2A0G1",
    "CSF_CrAg Neg": "fmhQvOd3A3W",
    "CSF_CrAg+": "jA4RNBaKtps",
    "FASH+": "dkuVrhhSnKY",
    "FASH Normal": "jXy16fgI4bL",
    "Gene_Xpert Neg": "wafYxp7w7oQ",
    "Gene_Xpert+": "rqjSefSPbv3",
    "LAM Neg": "zDQ1HGTExJi",
    "LAM+": "m1MIFkHtnsR",
    "Other_treatment": "I30zS1Nt83N",
    "Seriously_ill": "Ac6fGmcdnYh",
    "TX_CM": "ICQ4fViwhIO",
    "TX_Cryptococcemia": "OnEQH1fwPsY",
    "TX_KS": "whx2qNPkJKp",
    "TX_TB": "lGFG7F9AdD4",
    "WHO_stage_1": "Rh2ADm5gnQK",
    "WHO_stage_2": "anoegvTrvGZ",
    "WHO_stage_3": "mlOQKaWc9zI",
    "WHO_stage_4": "WI5PTSzQREZ",
}
# "byv6WAiQl3s",
category_option_combos = {
    "All_Categories(totals from page summeries)": "vNVfhS2oGT2",
    "ART_interrupters(for >2months)": "NweYqql83UD",
    "Children(0-4yrs)": "PgdmyzcFluv",
    "Inpatients(HIV_pos_admitted)": "bH3lIXZvZdj",
    "New_HIV_pos": "kUkskhxydV5",
    "Pre_and_adolescents(5-14)": "ap7UR1M1h8f",
    "Unsuppressed_ROC": "fovdRRTrZpI",
}

org_units = {
    "KCH_OPD1": "RY0I8Ha0azq",
    "Rainbow_MCH": "EQg6N2v2TXj",
    "UFC_Queens": "aimBmvkBWy3",
    "Tisungane_ZCH": "uJG4WFdUJSv",
    "Area_18": "h2ls2FUTYDc",
    "MPC_Bwaila": "XtF7Xzv3edv",
    "Chileka": "iiBtWzxLMUo",
    "Chitedze": "Xc3odqjv7h0",
    "LH_KCH": "kR3EKghUNys",
    "Lumbadzi": "UrNi47KXEKp",
    "Nathenje": "AgYrPo8MW5I",
    "Kawale": "d1WQftQ87x3",
    "Mitundu": "aVmCsQGN0r1",
    "Maula_prison": "sPwxCzvUtia",
}

# List of data elements to extract
data_elements = ['Total_number_of_ROC', 'Seriously_ill', 'WHO_stage_1', 'WHO_stage_2', 
                 'WHO_stage_3', 'WHO_stage_4', 'CD4 >= 200', 'CD4<200', 'CrAg Neg', 
                 'CrAg+', 'CSF_CrAg Neg', 'CSF_CrAg+', 'LAM Neg', 'LAM+', 'Gene_Xpert Neg', 
                 'Gene_Xpert+', 'Chest_Xlay normal', 'Chest_Xlay_abnormal', 'FASH Normal', 'FASH+', 
                 'TX_TB', 'TX_CM', 'TX_Cryptococcemia', 'TX_KS', 'Other_treatment']

df_input['CD4 >= 200'] = df_input['CD4_tests'] - df_input['CD4<200']
df_input['CrAg Neg'] = df_input['CrAg_tests'] - df_input['CrAg+']
df_input['CSF_CrAg Neg'] = df_input['CSF_CrAg'] - df_input['CSF_CrAg+']
df_input['LAM Neg'] = df_input['LAM_tests'] - df_input['LAM+']
df_input['Gene_Xpert Neg'] = df_input['Gene_Xpert_done'] - df_input['Gene_Xpert+']
df_input['Chest_Xlay normal'] = df_input['Chest_Xlay'] = df_input['Chest_Xlay_abnormal']
df_input['FASH Normal'] = df_input['FASH'] - df_input['FASH+']

# Initialize row counter for Id
row_id = 1

output_data = []

print(df_input.columns)
# Loop through each row in the input data
for index, row in df_input.iterrows():
    # Extract the reporting month and year
    reporting_year = row['Reporting_year']
    reporting_month = row['Reporting month']

    # Check if the reporting_month is NaN
    if pd.isna(reporting_month):
        print(f"Skipping row {index} due to missing Reporting month")
        continue  # Skip rows where reporting month is missing

    period = str(int(reporting_year)) + month_num_map[reporting_month]
    
    reporting_month = reporting_month.strip()  # Clean up the month name

    # Loop through each data element and create rows for the output
    for data_element in data_elements:

        element = data_elements_map.get(data_element, None)
        if not element:
            print(f"Skipping data element: {data_element}")
            continue  # Skip if reporting_month is not in quarter_map

        output_row = {
            'dataElement': element,
            'period': period,
            'orgUnit': org_units[row['Facility']],
            'categoryOptionCombo': category_option_combos[row['AHD_eligible_category']],
            'attributeoptioncombo': 'HllvX50cXC0',
            'value': row[data_element]
        }
        output_data.append(output_row)
        #df_output = df_output.append(output_row, ignore_index=True)
        row_id += 1  # Increment the row Id

df_output = pd.DataFrame(output_data)
# Save the output to an Excel file
output_file = f"{base_path}AHD_Reporting_Form_Out_20250602.csv"  # Replace with your desired output path
df_output.to_csv(output_file, index=False)
print(f"Data saved to {output_file}")
