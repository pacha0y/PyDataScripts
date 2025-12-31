import mysql.connector
import pandas as pd
import numpy as np

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
    
def df_to_mysql_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures ALL NaN values become Python None (SQL NULL).
    """
    df = df.astype(object)
    df = df.replace({np.nan: None})
    return df

def load_and_clean_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)

    df.columns = (
        df.columns
          .str.strip()
          .str.replace(" ", "")
    )

    date_cols = [
        "ApplicationDate",
        "DateEmailReceived",
        "ApprovedDate"
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    bool_map = {
        "Yes": 1, "No": 2, "Unknown": 3,
        "Y": 1, "N": 2, "U": 3,
        True: 1, False: 0
    }

    bool_cols = ["Approved", "SampleSent", "ResultReceived"]

    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map(bool_map)
            df[col] = df[col].astype(object).where(df[col].notna(), None)

    return df

def prepare_update_rows(df: pd.DataFrame) -> list:
    """
    Converts DataFrame rows into a list of tuples for batch update.
    """
    rows = []

    for _, row in df.iterrows():
        rows.append((
            row.get("ApplicationDate"),
            row.get("DateEmailReceived"),
            row.get("Approved"),
            row.get("ApprovedDate"),
            row.get("SampleSent"),
            row.get("ResultReceived"),
            row.get("ApplicationID")
        ))

    return rows


def update_applications(conn, rows: list) -> int:
    """
    Updates application records using ApplicationID as primary key.
    Returns number of affected rows.
    """
    update_sql = """
    UPDATE application
    SET
        ApplicationDate = COALESCE(%s, ApplicationDate),
        DateEmailReceived = COALESCE(%s, DateEmailReceived),
        Approved = COALESCE(%s, Approved),
        ApprovedDate = COALESCE(%s, ApprovedDate),
        SampleSent = COALESCE(%s, SampleSent),
        ResultReceived = COALESCE(%s, ResultReceived)
    WHERE ApplicationID = %s
    """

    print(assert_no_nan(rows))

    cursor = conn.cursor()
    cursor.executemany(update_sql, rows)
    conn.commit()

    affected = cursor.rowcount
    cursor.close()

    return affected

import math

def assert_no_nan(rows):
    for i, row in enumerate(rows):
        for j, val in enumerate(row):
            if isinstance(val, float) and math.isnan(val):
                raise ValueError(f"NaN at row {i}, column {j}")


def run_update_pipeline(
    excel_path,
    db_host,
    db_user,
    db_password,
    db_name
):
    df = load_and_clean_csv(excel_path)

    # MUST be the final transformation
    df = df_to_mysql_safe(df)

    rows = prepare_update_rows(df)

    conn = connect_to_db(
        db_host, db_user, db_password, db_name
    )

    try:
        updated = update_applications(conn, rows)
        print(f"Successfully updated {updated} records.")
    finally:
        conn.close()


if __name__ == "__main__":
    run_update_pipeline(
        excel_path="applications.csv",
        db_host="localhost",
        db_user="root",
        db_password="root",
        db_name="hiv_dr_db"
    )
