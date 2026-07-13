from airflow.models import Variable

from datetime import datetime

from sqlalchemy import create_engine, text

import pandas as pd
import boto3
import json
import re
import io
import requests
import yaml

from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest


# =====================================================
# CONFIG
# =====================================================

CONFIG_PATH = "/opt/airflow/config.yaml"

WATERMARK_VARIABLE_KEY = "sheets_row_watermarks"

GLUE_CRAWLER_NAME = "joac-event-crawler"


def load_config():

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)



config = load_config()



# =====================================================
# AWS
# =====================================================

S3_BUCKET = config["s3"]["bucket"]

S3_REGION = config["aws"]["region"]

AWS_ACCESS_KEY_ID = config["aws"]["access_key_id"]

AWS_SECRET_ACCESS_KEY = config["aws"]["secret_access_key"]

S3_INCREMENTAL_BASE = config["s3"]["incremental_base"]



# =====================================================
# POSTGRES
# =====================================================

POSTGRES_CONNECTION = (
    "postgresql+psycopg2://airflow:airflow@postgres/airflow"
)



# =====================================================
# GOOGLE
# =====================================================

SPREADSHEET_ID = config["google"]["spreadsheet_id"]

GOOGLE_SERVICE_ACCOUNT = config["google"]["service_account"]


SHEETS_TO_SKIP = set(
    config["google"].get(
        "sheets_to_skip",
        []
    )
)


GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly"
]



CANONICAL_COLUMNS = [

    "name",
    "mobile",
    "grade",
    "location",
    "data_source",
    "data_source_2",
    "data_source_1"

]



# =====================================================
# CLEAN MOBILE
# =====================================================

def clean_mobile(mobile):

    if pd.isna(mobile) or mobile is None:
        return None


    mobile = str(mobile).strip()


    digits = re.sub(
        r"\D",
        "",
        mobile
    )


    if (
        len(digits) == 9
        and digits.startswith(
            ("77","78","79")
        )
    ):
        return "962" + digits



    if (
        len(digits) == 12
        and digits.startswith("962")
    ):
        return digits



    return None



# =====================================================
# CLEAN GRADE
# =====================================================

def clean_grade(grade):

    if pd.isna(grade):
        return None


    grade = str(grade).strip()


    if grade == "غير معرف":
        return None


    if grade == "عاشر":
        return "2010"


    return grade



# =====================================================
# GOOGLE FETCH
# =====================================================

def fetch_google_sheets():

    credentials = Credentials.from_service_account_info(

        GOOGLE_SERVICE_ACCOUNT,

        scopes=GOOGLE_SCOPES

    )


    credentials.refresh(
        GoogleAuthRequest()
    )


    headers = {

        "Authorization":
        f"Bearer {credentials.token}"

    }



    response = requests.get(

        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}",

        headers=headers,

        params={

            "includeGridData":"true",

            "fields":
            "sheets(properties(title),data(rowData(values(formattedValue))))"

        },

        timeout=60

    )


    response.raise_for_status()


    return response.json()["sheets"]




# =====================================================
# GET NEXT ID
# =====================================================

def get_next_id():


    engine = create_engine(
        POSTGRES_CONNECTION
    )


    query = """

    SELECT COALESCE(MAX(id),0)+1

    FROM customers_full_load

    """


    with engine.connect() as conn:

        result = conn.execute(
            text(query)
        )

        next_id = result.scalar()


    return next_id




# =====================================================
# LOAD TO POSTGRES + S3
# =====================================================

def load_incremental_to_postgres(df):


    engine = create_engine(
        POSTGRES_CONNECTION
    )


    # ===============================
    # CREATE IDS
    # ===============================


    next_id = get_next_id()


    df.insert(

        0,

        "id",

        range(

            next_id,

            next_id + len(df)

        )

    )



    # ===============================
    # ORDER COLUMNS
    # ===============================


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



    # ===============================
    # POSTGRES APPEND
    # ===============================


    df.to_sql(

        name="customers_full_load",

        con=engine,

        if_exists="append",

        index=False

    )


    print(
        "Incremental data loaded into PostgreSQL"
    )



    # ===============================
    # SAVE PARQUET
    # ===============================


    now = datetime.utcnow()



    key = (

        f"{S3_INCREMENTAL_BASE}/"

        f"{now.strftime('%Y/%m/%d')}/"

        f"event_{now.strftime('%Y%m%d_%H%M%S')}.parquet"

    )



    buffer = io.BytesIO()



    df.to_parquet(

        buffer,

        engine="pyarrow",

        index=False

    )



    buffer.seek(0)



    s3 = boto3.client(

        "s3",

        region_name=S3_REGION,

        aws_access_key_id=AWS_ACCESS_KEY_ID,

        aws_secret_access_key=AWS_SECRET_ACCESS_KEY

    )



    s3.put_object(

        Bucket=S3_BUCKET,

        Key=key,

        Body=buffer.getvalue()

    )



    print(
        f"Uploaded s3://{S3_BUCKET}/{key}"
    )




# =====================================================
# MAIN EVENT LOAD
# =====================================================

def fetch_and_load_new_rows(**kwargs):


    sheets = fetch_google_sheets()



    try:

        watermarks = json.loads(

            Variable.get(

                WATERMARK_VARIABLE_KEY,

                default_var="{}"

            )

        )


    except:

        watermarks = {}



    updated_watermarks = dict(
        watermarks
    )


    new_rows = []



    for sheet in sheets:


        sheet_name = sheet["properties"]["title"]



        if sheet_name in SHEETS_TO_SKIP:
            continue



        rows = (

            sheet

            .get("data",[{}])[0]

            .get("rowData",[])

        )



        all_rows = [

            [

                cell.get(
                    "formattedValue",
                    ""
                )

                for cell in row.get(
                    "values",
                    []
                )

            ]

            for row in rows

        ]



        data_rows = all_rows[1:]



        current_count = len(data_rows)



        old_count = int(

            watermarks.get(
                sheet_name,
                0
            )

        )



        if current_count <= old_count:

            continue



        latest_rows = data_rows[old_count:]



        for row in latest_rows:


            record = {}



            for index,column in enumerate(
                CANONICAL_COLUMNS
            ):


                record[column] = (

                    row[index]

                    if index < len(row)

                    else None

                )



            record["sheet_name"] = sheet_name



            record["mobile"] = clean_mobile(
                record["mobile"]
            )


            if not record["mobile"]:
                continue



            record["grade"] = clean_grade(
                record["grade"]
            )


            if not record["grade"]:
                continue



            record["upload_date"] = None


            record["timestamp"] = datetime.utcnow().isoformat()



            new_rows.append(record)



        updated_watermarks[sheet_name] = current_count




    if not new_rows:

        print(
            "No new valid rows"
        )

        Variable.set(

            WATERMARK_VARIABLE_KEY,

            json.dumps(updated_watermarks)

        )

        return



    df = pd.DataFrame(new_rows)



    # remove duplicates by mobile

    df.drop_duplicates(

        subset=["mobile"],

        keep="last",

        inplace=True

    )



    load_incremental_to_postgres(df)



    # ===============================
    # START GLUE CRAWLER
    # ===============================


    glue = boto3.client(

        "glue",

        region_name=S3_REGION,

        aws_access_key_id=AWS_ACCESS_KEY_ID,

        aws_secret_access_key=AWS_SECRET_ACCESS_KEY

    )


    try:

        glue.start_crawler(
            Name=GLUE_CRAWLER_NAME
        )


        print(
            "Glue crawler started"
        )


    except Exception as e:

        print(
            "Glue crawler error:",
            e
        )



    Variable.set(

        WATERMARK_VARIABLE_KEY,

        json.dumps(updated_watermarks)

    )


    print(
        "Event Load Finished Successfully"
    )