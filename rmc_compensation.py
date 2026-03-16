import requests
import pandas as pd
import numpy as np
import io
import os
import logging

from datetime import datetime, timedelta
from dotenv import load_dotenv


# ------------------------------------------------
# ENV
# ------------------------------------------------
load_dotenv()

POST_URL = os.getenv("POST_URL")
API_PUSH = os.getenv("API_PUSH")


# ------------------------------------------------
# PATH
# ------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

VEHICLE_PATH = os.path.join(ROOT_DIR, "vehicle.json")


# ------------------------------------------------
# LOGGING
# ------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ------------------------------------------------
# FETCH REPORT
# ------------------------------------------------
def fetch_rmc_report(date: str = None) -> pd.DataFrame:

    if date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        target_date = date

    date_start = f"{target_date} 00:00:00"
    date_end = f"{target_date} 23:59:59"

    logging.info(f"Fetch report for {target_date}")

    payload = {
        "date_start": date_start,
        "date_end": date_end,
        "type": "vehicle",
        "plants_list": ["all"],
        "company_id": 1231,
        "site_id": "",
        "type_file": "excel"
    }

    headers = {"Content-Type": "application/json"}

    with requests.Session() as s:

        resp = s.post(
            POST_URL,
            json=payload,
            headers=headers,
            timeout=180
        )

        resp.raise_for_status()

        result = resp.json()

        file_url = result.get("result")

        if not file_url:
            raise Exception("No result file URL")

        logging.info(f"Download excel {file_url}")

        r2 = s.get(file_url, timeout=180)
        r2.raise_for_status()

        df = pd.read_excel(
            io.BytesIO(r2.content),
            skiprows=3
        )

    logging.info(f"Rows fetched {len(df)}")

    return df


# ------------------------------------------------
# TRANSFORM
# ------------------------------------------------
def transform_data(df: pd.DataFrame) -> pd.DataFrame:

    cols = [
        "หมายเลข DP",
        "รหัสรถ",
        "ประเภทรถ",
        "ชื่อแพลนต์",
        "เวลาถึงไซต์งาน",
        "เวลาออกจากไซต์งาน",
        "เวลาออกตั๋ว"
    ]

    df = df[cols]

    df["เวลาถึงไซต์งาน"] = pd.to_datetime(df["เวลาถึงไซต์งาน"], errors="coerce")
    df["เวลาออกจากไซต์งาน"] = pd.to_datetime(df["เวลาออกจากไซต์งาน"], errors="coerce")

    df["site_minutes"] = (
        (df["เวลาออกจากไซต์งาน"] - df["เวลาถึงไซต์งาน"])
        .dt.total_seconds() / 60
    )

    df["site_minutes"] = df["site_minutes"].round(0)

    # -----------------------
    # tier
    # -----------------------

    df["tier"] = np.select(
        [
            df["site_minutes"].isna(),
            df["site_minutes"] < 91,
            df["site_minutes"].between(91, 119),
            df["site_minutes"].between(120, 150),
            df["site_minutes"] > 150
        ],
        [
            "no_tier",
            "tier_0",
            "tier_1",
            "tier_2",
            "tier_3"
        ],
        default="tier_3"
    )

    # -----------------------
    # compensation
    # -----------------------

    df["compensate"] = np.select(
        [
            (df["tier"] == "tier_1") & (df["ประเภทรถ"] == "รถโม่ใหญ่ 10 ล้อ"),
            (df["tier"] == "tier_2") & (df["ประเภทรถ"] == "รถโม่ใหญ่ 10 ล้อ"),
            (df["tier"] == "tier_3") & (df["ประเภทรถ"] == "รถโม่ใหญ่ 10 ล้อ"),

            (df["tier"] == "tier_1") & (df["ประเภทรถ"] == "รถโม่เล็ก 4 ล้อ"),
            (df["tier"] == "tier_2") & (df["ประเภทรถ"] == "รถโม่เล็ก 4 ล้อ"),
            (df["tier"] == "tier_3") & (df["ประเภทรถ"] == "รถโม่เล็ก 4 ล้อ")
        ],
        [
            1, 2, 3,
            0.5, 1, 1.5
        ],
        default=0
    )

    # -----------------------
    # load vehicle
    # -----------------------

    vehicle = pd.read_json(VEHICLE_PATH)
    vehicle_df = pd.json_normalize(vehicle["data"])

    df["รหัสรถ"] = df["รหัสรถ"].astype(str)
    vehicle_df["code"] = vehicle_df["code"].astype(str)

    df = df.merge(
        vehicle_df,
        how="left",
        left_on="รหัสรถ",
        right_on="code"
    )

    # -----------------------
    # truck type
    # -----------------------

    df["truck_type"] = df["ประเภทรถ"].map({
        "รถโม่ใหญ่ 10 ล้อ": "ML",
        "รถโม่เล็ก 4 ล้อ": "MS"
    })

    # -----------------------
    # rename
    # -----------------------

    df = df.rename(columns={
        "หมายเลข DP": "TicketNo",
        "รหัสรถ": "TruckNo",
        "plate_no": "TruckPlateNo",
        "plate_no_only": "TruckPlateNo_clean",
        "ชื่อแพลนต์": "PlantName",
        "เวลาถึงไซต์งาน": "SiteMoveInAt",
        "เวลาออกจากไซต์งาน": "SiteMoveOutAt",
        "site_minutes": "minutes_diff",
        "เวลาออกตั๋ว": "TicketCreateAt"
    })

    df["date_ticket"] = pd.to_datetime(df["TicketCreateAt"]).dt.date

    df["is_complete_trip"] = np.where(
        df["SiteMoveInAt"].notna() &
        df["SiteMoveOutAt"].notna(),
        "Y",
        "N"
    )

    return df


# ------------------------------------------------
# CLEAN DATA
# ------------------------------------------------
def clean_data(df: pd.DataFrame) -> pd.DataFrame:

    df = df.replace([np.inf, -np.inf], np.nan)

    required_cols = [
        "TicketNo",
        "TruckPlateNo",
        "TruckPlateNo_clean",
        "PlantName",
        "truck_type",
        "date_ticket"
    ]

    df = df.dropna(subset=required_cols)

    for col in required_cols:
        df = df[df[col] != ""]

    logging.info(f"Rows after clean {len(df)}")

    return df


# ------------------------------------------------
# PUSH API
# ------------------------------------------------
def push_api(df: pd.DataFrame):

    df = df.copy()

    # datetime convert
    datetime_cols = [
        "SiteMoveInAt",
        "SiteMoveOutAt",
        "TicketCreateAt"
    ]

    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = df[col].apply(
            lambda x: x.isoformat() if pd.notnull(x) else None
        )

    df["date_ticket"] = pd.to_datetime(df["date_ticket"], errors="coerce")
    df["date_ticket"] = df["date_ticket"].apply(
        lambda x: x.isoformat() if pd.notnull(x) else None
    )

    data = df.to_dict(orient="records")

    logging.info(f"Send records {len(data)}")

    response = requests.post(
        API_PUSH,
        json=data,
        headers={"Content-Type": "application/json"},
        timeout=120
    )

    logging.info(f"API status {response.status_code}")
    logging.info(response.text)


# ------------------------------------------------
# MAIN
# ------------------------------------------------
def main():

    try:

        df_raw = fetch_rmc_report()

        df = transform_data(df_raw)

        df = clean_data(df)

        push_api(df)

        logging.info("Job completed")

    except Exception as e:

        logging.error("Job failed")
        logging.error(str(e))


# ------------------------------------------------
# RUN
# ------------------------------------------------
if __name__ == "__main__":

    main()