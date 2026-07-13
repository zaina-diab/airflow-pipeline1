import os
import re
from datetime import datetime

import boto3
import pandas as pd
import pyarrow
import requests
import yaml
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest


# =====================================================
# CONFIG
# =====================================================

CONFIG_PATH = "/opt/airflow/config.yaml"

FOLLOWUP_FILE = "/opt/airflow/data/Eman Follow up (1).xlsx"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


config = load_config()


# =====================================================
# GOOGLE CONFIG
# =====================================================

SPREADSHEET_ID = config["google"]["spreadsheet_id"]

GOOGLE_SERVICE_ACCOUNT = config["google"]["service_account"]

SHEETS_TO_SKIP = set(
    config["google"].get("sheets_to_skip", [])
)


GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly"
]


# =====================================================
# AWS CONFIG
# =====================================================

S3_BUCKET = config["s3"]["bucket"]

S3_REGION = config["aws"]["region"]

AWS_ACCESS_KEY_ID = config["aws"]["access_key_id"]

AWS_SECRET_ACCESS_KEY = config["aws"]["secret_access_key"]


S3_FILE_PATH = config["s3"]["full_load_path"]


POSTGRES_CONNECTION = (
    "postgresql+psycopg2://airflow:airflow@postgres/airflow"
)
# =====================================================
# GOOGLE AUTH
# =====================================================

def get_google_credentials():

    credentials = Credentials.from_service_account_info(
        GOOGLE_SERVICE_ACCOUNT,
        scopes=GOOGLE_SCOPES
    )

    credentials.refresh(
        GoogleAuthRequest()
    )

    return credentials



# =====================================================
# FETCH GOOGLE SHEETS
# =====================================================

def fetch_sheets():

    creds = get_google_credentials()

    headers = {
        "Authorization": f"Bearer {creds.token}"
    }


    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/"
        f"{SPREADSHEET_ID}"
    )


    response = requests.get(
        url,
        headers=headers,
        params={
            "includeGridData": "true",
            "fields":
            "sheets(properties(title),data(rowData(values(formattedValue))))"
        },
        timeout=60
    )


    response.raise_for_status()


    sheets = {}


    for sheet in response.json().get("sheets", []):

        sheet_name = sheet["properties"]["title"]


        if sheet_name in SHEETS_TO_SKIP:
            print(
                f"Skipping sheet: {sheet_name}"
            )
            continue



        rows_raw = (
            sheet
            .get("data", [{}])[0]
            .get("rowData", [])
        )


        rows = []

        for row in rows_raw:

            values = [
                cell.get("formattedValue", "")
                for cell in row.get("values", [])
            ]

            rows.append(values)



        # remove header
        if len(rows) <= 1:
            continue


        sheets[sheet_name] = rows[1:]


    print(
        f"Loaded sheets: {len(sheets)}"
    )


    return sheets



# =====================================================
# ARABIC NORMALIZATION
# =====================================================

def normalize_arabic(text):

    if pd.isna(text):
        return text


    text = str(text)


    # remove tashkeel
    text = re.sub(
        r"[\u064B-\u0652]",
        "",
        text
    )


    # unify alef
    text = re.sub(
        r"[أإآ]",
        "ا",
        text
    )


    # taa marbuta
    text = text.replace(
        "ة",
        "ه"
    )


    # yaa
    text = text.replace(
        "ى",
        "ي"
    )


    return text.strip()



# =====================================================
# MOBILE CLEANING
# =====================================================

def clean_mobile(mobile):

    if pd.isna(mobile):
        return None


    mobile = str(mobile)


    # take first number if separated
    mobile = mobile.split("/")[0]


    # remove symbols
    mobile = re.sub(
        r"\D",
        "",
        mobile
    )


    # Jordan number without country code
    if (
        len(mobile) == 9
        and mobile.startswith(
            ("77", "78", "79")
        )
    ):
        return "962" + mobile



    # already has country code
    if (
        len(mobile) == 12
        and mobile.startswith("962")
    ):
        return mobile



    return None



# =====================================================
# EXCEL UPLOAD DATES
# =====================================================

def fetch_upload_dates():


    followup = pd.read_excel(
        FOLLOWUP_FILE
    )


    # clean column names
    followup.columns = (
        followup.columns
        .astype(str)
        .str.replace("\ufeff", "")
        .str.replace("\t", "")
        .str.replace("\n", "")
        .str.strip()
    )


    print(
        "Excel columns:",
        followup.columns.tolist()
    )


    upload_dates = {}


    for _, row in followup.iterrows():


        data_name = row.get(
            "اسم الداتا"
        )


        upload_date = pd.to_datetime(
         row.get("تاريخ التفريغ"),
        dayfirst=True,
        errors="coerce"
        )


        if pd.notna(data_name):

            upload_dates[
                normalize_arabic(data_name)
            ] = upload_date



    print(
        f"Loaded {len(upload_dates)} upload dates"
    )


    return upload_dates



def lookup_upload_date(sheet_name, upload_dates):


    normalized_sheet = normalize_arabic(
        sheet_name
    )


    for key, value in upload_dates.items():

        if key in normalized_sheet:
            return value


    return None

# =====================================================
# BUILD DATAFRAME
# =====================================================

def build_dataframe(sheets_data):

    dfs = []

    upload_dates = fetch_upload_dates()


    columns_mapping = [
        "name",
        "mobile",
        "grade",
        "location",
        "data_source",
        "data_source_2",
        "data_source_1"
    ]


    for sheet_name, rows in sheets_data.items():


        df = pd.DataFrame(rows)


        # make sure indexes are numbers
        df.columns = range(
            df.shape[1]
        )


        for index, column_name in enumerate(columns_mapping):

            if index in df.columns:
                df[column_name] = df[index]
            else:
                df[column_name] = None



        df["sheet_name"] = sheet_name


        df["upload_date"] = lookup_upload_date(
            sheet_name,
            upload_dates
        )


        dfs.append(
            df[
                [
                    "name",
                    "mobile",
                    "grade",
                    "location",
                    "data_source",
                    "data_source_2",
                    "data_source_1",
                    "sheet_name",
                    "upload_date"
                ]
            ]
        )



    if not dfs:
        raise Exception(
            "No data found from Google Sheets"
        )


    final_df = pd.concat(
        dfs,
        ignore_index=True
    )


    return final_df




# =====================================================
# TRANSFORMATION
# =====================================================

def transform_dataframe(df):


    print(
        "Before cleaning:",
        len(df)
    )


    # -------------------------
    # Remove unknown grades
    # -------------------------

    df = df[
        df["grade"] != "غير معرف"
    ]



    # -------------------------
    # Grade mapping
    # -------------------------

    df["grade"] = df["grade"].replace(
        {
            "عاشر": "2010"
        }
    )



    # -------------------------
    # Mobile cleaning
    # -------------------------

    df["mobile"] = df["mobile"].apply(
        clean_mobile
    )


    df = df[
        df["mobile"].notna()
    ]



    # -------------------------
    # Arabic normalization
    # -------------------------

    arabic_columns = [
        "name",
        "location",
        "sheet_name",
        "data_source",
        "data_source_1",
        "data_source_2",
        "grade"
    ]


    for column in arabic_columns:

        if column in df.columns:

            df[column] = df[column].apply(
                normalize_arabic
            )



    # -------------------------
    # Remove duplicates
    # -------------------------

    df = df.drop_duplicates()



    # -------------------------
    # Full load ID
    # -------------------------

    df.insert(
        0,
        "id",
        range(
            1,
            len(df)+1
        )
    )



    # -------------------------
    # Timestamp
    
    # -------------------------
    df["upload_date"] = pd.to_datetime(
    df["upload_date"],
    errors="coerce"
)

    df["timestamp"] = (
        datetime.utcnow()
        .isoformat()
    )



    print(
        "After cleaning:",
        len(df)
    )


    return df




# =====================================================
# SAVE PARQUET
# =====================================================

def save_locally(df):


    output_dir = (
        "/opt/airflow/output"
    )


    os.makedirs(
        output_dir,
        exist_ok=True
    )


    file_path = (
        f"{output_dir}/full_load.parquet"
    )


    df.to_parquet(
        file_path,
        engine="pyarrow",
        index=False
    )


    print(
        f"Saved parquet: {file_path}"
    )


    return file_path




# =====================================================
# UPLOAD TO S3
# =====================================================

def upload_to_s3(file_path):


    s3_client = boto3.client(

        "s3",

        region_name=S3_REGION,

        aws_access_key_id=
        AWS_ACCESS_KEY_ID,

        aws_secret_access_key=
        AWS_SECRET_ACCESS_KEY
    )



    s3_client.upload_file(

        file_path,

        S3_BUCKET,

        S3_FILE_PATH
    )


    print(
        "Uploaded:"
        f"s3://{S3_BUCKET}/{S3_FILE_PATH}"
    )




def load_to_postgres(df):

    engine = create_engine(
        POSTGRES_CONNECTION
    )

    df.to_sql(
        name="customers_full_load",
        con=engine,
        if_exists="replace",
        index=False
    )

    print(
        "Loaded data into PostgreSQL"
    )
# =====================================================
# MAIN PIPELINE
# =====================================================

def run_full_pipeline():


    print(
        "Starting Full Excel Load..."
    )


    sheets = fetch_sheets()


    df = build_dataframe(
        sheets
    )


    df = transform_dataframe(
        df
    )



    # Required schema order

    df = df[
        [
            "id",
            "name",
            "mobile",
            "grade",
            "location",
            "data_source",
            "data_source_2",
            "data_source_1",
            "sheet_name",
            "upload_date",
            "timestamp"
        ]
    ]



    file_path = save_locally(
        df
    )


    upload_to_s3(
        file_path
    )

    load_to_postgres(
    df
    )

    print(
        "FULL LOAD FINISHED SUCCESSFULLY"
    )


    return df