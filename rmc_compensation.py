import requests
import pandas as pd
import numpy as np
import io
import os
import logging
import os
import json
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


VEHICLE_PATH = os.path.join(BASE_DIR, "vehicle.json")

with open(VEHICLE_PATH, "r") as f:
    vehicle_data = json.load(f)

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

    vehicle_list = [
        55046,55047,55075,55085,55086,55091,55092,55093,55094,55095,55096,55097,
        55533,55534,55535,55686,55687,55688,55689,55690,55938,55939,55945,55947,
        55948,57178,57179,57180,57181,57182,57184,57185,57343,57344,58266,58267,
        40730,40733,46350,46363,46368,46371,46372,46409,46420,46424,46445,46458,
        46460,46892,46893,46894,46895,47066,47067,47068,47069,47114,47115,47116,
        47117,47119,47120,48650,48651,48652,48653,49427,49428,49429,49430,49431,
        49432,52603,52604,52774,52775,52829,53657,53658,53659,53660,53663,53664,
        53665,53666,53671,53672,53673,53674,53675,53715,53716,53717,53718,53719,
        53745,53746,53747,53748,53749,53750,53751,53752,53753,53754,53755,53756,
        53757,53758,53759,53821,53822,53823,53824,53825,53826,53827,53828,53830,
        54291,54292,54293,54294,54295,54377,54576,54577,54578,54579,54580,55004,
        55005,55006,55016,55017
    ]

    payload = {
        "date_start": date_start,
        "date_end": date_end,
        "type": "vehicle",
        "vehicle_list": vehicle_list,
        "plants_list": ["all"],
        "company_id": 1231,
        "vehicle_visibility": ",".join(map(str, vehicle_list)),
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
def transform_data(df):

    cols_to_select = [
        "หมายเลข DP",
        "รหัสรถ",
        "รหัสคนขับ",
        "คนขับรถ",
        "ประเภทรถ",
        "รหัสแพลนต์",
        "ชื่อแพลนต์",
        "รหัสไซต์งาน",
        "ชื่อไซต์งาน",
        "เวลาถึงไซต์งาน",
        "เวลาออกจากไซต์งาน",
        "รหัสยกเลิกตั๋ว",
        "สถานะ",
        "สถานะตั๋ว",
        "เวลาออกตั๋ว"
    ]

    df = df[cols_to_select]

    df["เวลาถึงไซต์งาน"] = pd.to_datetime(df["เวลาถึงไซต์งาน"], errors="coerce")
    df["เวลาออกจากไซต์งาน"] = pd.to_datetime(df["เวลาออกจากไซต์งาน"], errors="coerce")

    df["site_minutes"] = (
        (df["เวลาออกจากไซต์งาน"] - df["เวลาถึงไซต์งาน"])
        .dt.total_seconds() / 60
    )

    df["site_minutes"] = df["site_minutes"].round(0)

    df["tier"] = np.select(
        [
            df["site_minutes"].isna(),
            df["site_minutes"] < 91,
            df["site_minutes"].between(91,119),
            df["site_minutes"].between(120,150),
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

    df["compensate"] = np.select(
        [
            (df["tier"]=="tier_1") & (df["ประเภทรถ"]=="รถโม่ใหญ่ 10 ล้อ"),
            (df["tier"]=="tier_2") & (df["ประเภทรถ"]=="รถโม่ใหญ่ 10 ล้อ"),
            (df["tier"]=="tier_3") & (df["ประเภทรถ"]=="รถโม่ใหญ่ 10 ล้อ"),
            (df["tier"]=="tier_1") & (df["ประเภทรถ"]=="รถโม่เล็ก 4 ล้อ"),
            (df["tier"]=="tier_2") & (df["ประเภทรถ"]=="รถโม่เล็ก 4 ล้อ"),
            (df["tier"]=="tier_3") & (df["ประเภทรถ"]=="รถโม่เล็ก 4 ล้อ")
        ],
        [1,2,3,0.5,1,1.5],
        default=0
    )

    vehicle = pd.read_json(VEHICLE_PATH)
    vehicle_df = pd.json_normalize(vehicle["data"])

    df["รหัสรถ"] = df["รหัสรถ"].astype(str)
    vehicle_df["code"] = vehicle_df["code"].astype(str)

    df_merge = df.merge(
        vehicle_df,
        how="left",
        left_on="รหัสรถ",
        right_on="code"
    )

    df_merge["truck_type"] = df_merge["ประเภทรถ"].map({
        "รถโม่ใหญ่ 10 ล้อ": "ML",
        "รถโม่เล็ก 4 ล้อ": "MS"
    })

    final = df_merge[[
        "หมายเลข DP","รหัสรถ","plate_no_only","plate_no","ชื่อแพลนต์",
        "เวลาถึงไซต์งาน","เวลาออกจากไซต์งาน","site_minutes",
        "tier","truck_type","compensate","เวลาออกตั๋ว"
    ]]

    final = final.rename(columns={
        "หมายเลข DP":"TicketNo",
        "รหัสรถ":"TruckNo",
        "plate_no":"TruckPlateNo",
        "plate_no_only":"TruckPlateNo_clean",
        "ชื่อแพลนต์":"PlantName",
        "เวลาถึงไซต์งาน":"SiteMoveInAt",
        "เวลาออกจากไซต์งาน":"SiteMoveOutAt",
        "site_minutes":"minutes_diff",
        "เวลาออกตั๋ว":"TicketCreateAt"
    })

    final["date_ticket"] = pd.to_datetime(final["TicketCreateAt"]).dt.date

    final["is_complete_trip"] = np.where(
        final["SiteMoveInAt"].notna() &
        final["SiteMoveOutAt"].notna(),
        "Y",
        "N"
    )

    return final


# ------------------------------------------------
# PUSH API
# ------------------------------------------------
def push_api(df):

    df = df.replace([np.inf,-np.inf],np.nan)

    required_cols = [
        "TicketNo","TruckPlateNo","TruckPlateNo_clean",
        "PlantName","truck_type","date_ticket"
    ]

    df = df.dropna(subset=required_cols)

    for col in required_cols:
        df = df[df[col]!=""]

    logging.info(f"rows sending {len(df)}")

    datetime_cols = ["SiteMoveInAt","SiteMoveOutAt","TicketCreateAt"]

    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col],errors="coerce")
        df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

    df["date_ticket"] = pd.to_datetime(df["date_ticket"],errors="coerce")
    df["date_ticket"] = df["date_ticket"].apply(
        lambda x: x.date().isoformat() if pd.notnull(x) else None
    )

    df["minutes_diff"] = pd.to_numeric(df["minutes_diff"],errors="coerce").fillna(0)
    df["compensate"] = pd.to_numeric(df["compensate"],errors="coerce").fillna(0)
    df = df.astype(object).where(pd.notnull(df), None)
    data = df.to_dict(orient="records")

    response = requests.post(
        API_PUSH,
        json=data,
        headers={"Content-Type":"application/json"},
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

        logging.info("transform data")

        df = transform_data(df_raw)

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