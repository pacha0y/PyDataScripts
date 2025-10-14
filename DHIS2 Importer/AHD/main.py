from datetime import datetime
import os
import pandas as pd # type: ignore
from io import BytesIO
import requests # type: ignore
import configparser
from dotenv import load_dotenv # type: ignore
import numpy as np

# Load config and secrets
load_dotenv()
config = configparser.ConfigParser()
config.read('config.ini')

sheet_name = config['paths']['sheet_name']

# Load DHIS2 credentials
dhis2_url = config['dhis2']['base_url']
dhis2_user = config['dhis2']['username']
dhis2_pass = os.getenv("DHIS2_PASSWORD")
attribute_option_combo = config['dhis2']['org_unit_attribute_combo']

def load_excel_file():
    config = configparser.ConfigParser()
    config.read("config.ini")

    local_file = config["paths"]["excel_path"]
    print(f"→ Loading local Excel file: {local_file}")

    df = pd.read_excel(local_file, sheet_name=sheet_name, header=0, engine='openpyxl')
    df.columns = (
    df.columns
    .str.strip()
    .str.replace(r"\s+", "_", regex=True)
)
    print(f"✓ Loaded {len(df)} rows")
    return df

def load_csv_file():
    config = configparser.ConfigParser()
    config.read("config.ini")

    local_file = config["paths"]["csv_path"]
    print(f"→ Loading local CSV file: {local_file}")

    df = pd.read_csv(local_file)
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(r"\s+", "_", regex=True)
    )
    print(f"✓ Loaded {len(df)} rows")
    return df

def transform_data(df_input):
    # Mappings (you can also move these to separate files)
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

    category_option_combos = {
        "All_Categories(totals from page summeries)": "vNVfhS2oGT2",
        "ART_interrupters(for >2months)": "NweYqql83UD",
        "Children(0-4yrs)": "PgdmyzcFluv",
        "Inpatients(HIV_pos_admitted)": "bH3lIXZvZdj",
        "New_HIV_pos": "kUkskhxydV5",
        "Pre_and_adolescents(5-14)": "ap7UR1M1h8f",
        "Unsuppressed_ROC": "fovdRRTrZpI",
    }

    # Derived fields
    df_input['CD4 >= 200'] = df_input['CD4_tests'] - df_input['CD4<200']
    df_input['CrAg Neg'] = df_input['CrAg_tests'] - df_input['CrAg+']
    df_input['CSF_CrAg Neg'] = df_input['CSF_CrAg'] - df_input['CSF_CrAg+']
    df_input['LAM Neg'] = df_input['LAM_tests'] - df_input['LAM+']
    df_input['Gene_Xpert Neg'] = df_input['Gene_Xpert_done'] - df_input['Gene_Xpert+']
    df_input['Chest_Xlay normal'] = df_input['Chest_Xlay'] = df_input['Chest_Xlay_abnormal']
    df_input['FASH Normal'] = df_input['FASH'] - df_input['FASH+']
    # ... other derived fields

    data_elements = list(data_elements_map.keys())
    output_data = []

    for idx, row in df_input.iterrows():
        if pd.isna(row['Reporting_month']):
            continue
        month_str = row['Reporting_month'].strip()
        year = int(row['Reporting_year']) if pd.notna(row['Reporting_year']) else None
        try:
            month_num = datetime.strptime(month_str, '%b').month
        except ValueError:
            try:
                month_num = datetime.strptime(month_str, '%B').month
            except ValueError:
                month_num = None
        
        period = f"{year}{month_num:02d}" if year and month_num else None

        for de in data_elements:
            if de not in row or pd.isna(row[de]):
                continue

            output_data.append({
                "dataElement": data_elements_map[de],
                "period": period,
                "orgUnit": org_units.get(row['Facility'], ''),
                "categoryOptionCombo": category_option_combos.get(row['AHD_eligible_category'], ''),
                "attributeOptionCombo": attribute_option_combo,
                "value": row[de]
            })
    
    return output_data

def post_to_dhis2(data_values):
    url = f"{dhis2_url}/api/dataValueSets"
    payload = {
        "dataValues": data_values
    }

    response = requests.post(
        url,
        auth=(dhis2_user, dhis2_pass),
        json=payload,
        headers={"Content-Type": "application/json"}
    )

    if response.status_code != 200:
        print("DHIS2 Upload Failed:", response.text)
    else:
        print("✓ DHIS2 upload successful")

def is_positive_integer_value(v):
    return (
        isinstance(v, (int, float)) and
        v == int(v) and
        v > 0
    )

if __name__ == "__main__":
    print("→ Loading data into Dataframe from Excel...")
    df = load_csv_file()

    print("→ Transforming data...")
    dhis2_data = transform_data(df)

    dhis2_data = [
        {**d, "value": int(d["value"])}  # cast to int
        for d in dhis2_data
        if is_positive_integer_value(d.get("value"))
    ]
    
    for d in dhis2_data:
        if not (type(d.get("value")) is int and d["value"] > 0):
            print(f"Excluded: {d.get('value')} (type: {type(d.get('value'))})")

    print(f"→ Posting {len(dhis2_data)} values to DHIS2...")
    post_to_dhis2(dhis2_data)
