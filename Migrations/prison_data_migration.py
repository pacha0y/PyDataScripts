import mysql.connector
import pandas as pd
import uuid
import requests
import re
from pandas.api import types as pd_types

def connect_to_db(host, user, password, dbname):
    try:
        connect = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=dbname
        )
        return connect
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    
def get_data_from_source_db(connect, query):
    try:
        df = pd.read_sql(query, con=connect)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None
    
def import_data_to_target_db(connect, df, table_name):

    print(f"Importing data to {table_name}...")
    if df is None or df.empty:
        print("No rows to insert.")
        return

    # Work on a copy to avoid mutating original
    df = df.copy()

    # Convert pandas datetime types to native Python datetimes
    for col in df.columns:
        if pd_types.is_datetime64_any_dtype(df[col]) or isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)

    # Replace NaN/NaT with None for SQL NULLs
    df = df.where(pd.notnull(df), None)

    # Validate and quote column names to avoid SQL injection / syntax errors
    cols = list(df.columns)
    for c in cols:
        if not re.match(r'^[A-Za-z0-9_]+$', str(c)):
            raise ValueError(f"Unsafe column name: {c}")
    col_sql = ', '.join([f"`{c}`" for c in cols])

    placeholders = ', '.join(['%s'] * len(cols))
    sql = f"INSERT INTO `{table_name}` ({col_sql}) VALUES ({placeholders})"

    values = list(df.itertuples(index=False, name=None))

    cursor = connect.cursor()
    try:
        cursor.executemany(sql, values)
        connect.commit()
    except Exception as err:
        connect.rollback()
        print(f"Error inserting data: {err}")
        raise
    finally:
        cursor.close()

def generate_unique_id():
    return str(uuid.uuid4())

def get_last_id(connect, table_name, id_column):
    try:
        cursor = connect.cursor()
        cursor.execute(f"SELECT MAX({id_column}) FROM {table_name}")
        last_id = cursor.fetchone()[0]
        cursor.close()
        return last_id if last_id is not None else 0
    except mysql.connector.Error as err:
        print(f"Error fetching last ID: {err}")
        return 0


source_db = connect_to_db('localhost', 'mysql_user', 'mysql_pass', 'lh_db')
target_db = connect_to_db('localhost', 'mysql_user', 'mysql_pass', 'emr_db')

patient_query = "SELECT * FROM prisoners"
patients_df = get_data_from_source_db(source_db, patient_query)
art_at_init_query = "SELECT * FROM art_history_at_entry"
art_at_init_df = get_data_from_source_db(source_db, art_at_init_query)

# Prepare patient data
last_id = get_last_id(target_db, 'person', 'person_id')
patients_df['uuid'] = patients_df.apply(lambda x: generate_unique_id(), axis=1)
patients_df['person_id'] = range(last_id + 1, last_id + 1 + len(patients_df))
patients_df['creator'] = 1

patients_df['date_created'] = pd.to_datetime(patients_df['created_at']).dt.to_pydatetime()
# patients_df['date_created'] = pd.to_datetime(patients_df['date_created'], errors='coerce').apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
patients_df['dob'] = pd.to_datetime(patients_df['dob'], errors='coerce').apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
patients_df["gender"] = patients_df["gender"].str.strip().str.capitalize().replace({'Male': 'M', 'Female': 'F'})

ids_df = patients_df[['id','person_id']]

# Migrate person data
person_df = patients_df[['person_id', 'gender', 'dob', 'date_created','creator', 'uuid']]
person_df = person_df.rename(columns={'dob': 'birthdate'})
person_df['birthdate_estimated'] = 0


import_data_to_target_db(target_db, person_df, 'person')

# Migrate person name data
person_name_df = patients_df[['person_id', 'fname', 'lname','alias', 'date_created', 'creator', 'uuid']]
person_name_df = person_name_df.rename(columns={'fname': 'given_name', 'lname': 'family_name', 'alias': 'middle_name'})
person_name_df['preferred'] = 0

import_data_to_target_db(target_db, person_name_df, 'person_name')

# Migrate patient data
patient_df = patients_df[['person_id', 'date_created', 'creator']]
patient_df = patient_df.rename(columns={'person_id': 'patient_id'})

import_data_to_target_db(target_db, patient_df, 'patient')

patient_program_df = patients_df[['person_id', 'date_created', 'entry_date', 'creator']]
patient_program_df = patient_program_df.rename(columns={'person_id': 'patient_id', 'entry_date': 'date_enrolled'})
patient_program_df['program_id'] = 8
patient_program_df['uuid'] = [generate_unique_id() for _ in range(len(patient_program_df))]

import_data_to_target_db(target_db, patient_program_df, 'patient_program')

# Migrate person attribute data
person_attribute_df = patients_df[['person_id', 'education_level', 'religion', 'denomination','next_of_kin_name', 'next_of_kin_contact',
                                   'nationality', 'entry_date', 'prisoners_no', 'gender', 'cell', 'status', 'date_created', 'creator']]
attribute_columns = ['education_level', 'religion', 'denomination', 'nationality', 'next_of_kin_name', 'next_of_kin_contact',
                     'prisoners_no', 'entry_date', 'gender', 'cell', 'status']

# Melt the DataFrame
person_attribute_df = person_attribute_df.melt(
    id_vars=['person_id', 'date_created', 'creator'], 
    value_vars=attribute_columns,
    var_name='person_attribute_type_id',
    value_name='value'
)

# Map attribute names to IDs
attribute_type_map = {
    'education_level': 28,
    'religion': 29,
    # 'denomination': 3,
    'nationality': 3,
    'next_of_kin_name': 24,
    'next_of_kin_contact': 39,
    'prisoners_no': 40,
    'entry_date': 41,
    'gender': 43,
    'cell': 44,
    'status': 42,
    'HIV_status': 48,
    'on_ART': 49,
    'Hx_of_TB': 46,
    'Hx_of_STI': 47,
    'DM':50
}

person_attribute_df['person_attribute_type_id'] = person_attribute_df['person_attribute_type_id'].map(attribute_type_map)
person_attribute_df = person_attribute_df.dropna(subset=['value', 'person_attribute_type_id'])
person_attribute_df['uuid'] = [generate_unique_id() for _ in range(len(person_attribute_df))]

import_data_to_target_db(target_db, person_attribute_df, 'person_attribute')

# Migrate person address data
person_address_df = patients_df[['person_id', 'home_district', 'home_ta', 'home_village','residential_district', 'residential_ta',
       'residential_village', 'date_created', 'creator', 'uuid']]
person_address_df = person_address_df.rename(columns={'home_district': 'address2',
                                                    'home_ta': 'county_district', 'home_village': 'neighborhood_cell', 'residential_district': 'state_province',
                                                    'residential_ta': 'township_division', 'residential_village': 'city_village'})
import_data_to_target_db(target_db, person_address_df, 'person_address')

# Migrate person identity data
person_identity_df = patients_df[['person_id', 'prisoners_no','national_id', 'date_created', 'creator', 'uuid']]
person_identity_df = person_identity_df.rename(columns={'person_id': 'patient_id'})
person_identity_df['npid'] = [
    f"P1107{num:07d}" for num in range(last_id, last_id + len(person_identity_df))
]
person_identity_df['location_id'] = 1067

person_identity_df = person_identity_df.melt(
    id_vars=['patient_id', 'date_created', 'creator', 'location_id'],
    value_vars=['prisoners_no', 'national_id', 'npid'],
    var_name='identifier_type',
    value_name='identifier'
)

person_identity_df['identifier_type'] = person_identity_df['identifier_type'].map({
    'prisoners_no': 30,
    'national_id': 28,
    'npid': 3
})

person_identity_df['uuid'] = [generate_unique_id() for _ in range(len(person_identity_df))]

import_data_to_target_db(target_db, person_identity_df, 'patient_identifier')

## ART Status at initiation
art_at_init_df['creator'] = 1
art_at_init_df['HIV_status'] = art_at_init_df['HIV_status'].replace({'Prev Positive': 'KP', 'Prev Negative': 'KN', 'Never Tested': 'UK'})
art_at_init_df['prisoners_no'] = art_at_init_df['prisoners_no'].astype(int)
ids_df = ids_df.copy()
ids_df['id'] = ids_df['id'].astype(int)

art_at_init_df = art_at_init_df.merge(ids_df, left_on='prisoners_no', right_on='id', how='inner')

med_his_df = art_at_init_df[['person_id', 'HIV_status', 'on_ART', 'Hx_of_TB', 'Hx_of_STI', 'DM', 'creator', 'created_at']]
med_his_df = med_his_df.rename(columns={'created_at': 'date_created'})
med_his_df = med_his_df.melt(
    id_vars=['person_id', 'creator', 'date_created'],
    value_vars=['HIV_status', 'on_ART', 'Hx_of_TB', 'Hx_of_STI', 'DM',],
    var_name='person_attribute_type_id',
    value_name='value'
)

med_his_df['person_attribute_type_id'] = med_his_df['person_attribute_type_id'].map(attribute_type_map)
med_his_df = med_his_df.dropna(subset=['value', 'person_attribute_type_id'])
med_his_df['uuid'] = [generate_unique_id() for _ in range(len(med_his_df))]

import_data_to_target_db(target_db, med_his_df, 'person_attribute')

source_db.close()
target_db.close()

