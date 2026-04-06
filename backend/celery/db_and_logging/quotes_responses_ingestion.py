import os
import json
import re
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
import pandas as pd


load_dotenv()

DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_SSL = os.environ.get("DB_SSL", "false").lower() == "true"
DB_SSL_MODE = os.environ.get("DB_SSL_MODE", "require")

HARDCODED_RUN_ID = "11111111-1111-1111-1111-111111111111"

BASE_DIR = "policy_bazaar_responses_validation"
DEFAULT_DIR = "policy_bazaar_data_new"
MEDIAN_DIR = "policy_bazaar_data_median"


def get_connection():
    conn_params = {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD
    }

    if DB_SSL:
        conn_params["sslmode"] = DB_SSL_MODE

    return psycopg2.connect(**conn_params)


def insert_quotes_response(
    cursor,
    api_name,
    api_url,
    idv_selected,
    idv_type,
    full_json
):
    cursor.execute("""
        INSERT INTO quotes_responses (
            run_id,
            api_name,
            api_url,
            idv_type,
            idv_selected,
            response_json
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        HARDCODED_RUN_ID,
        api_name,
        api_url,
        idv_type,
        idv_selected,
        Json(full_json)
    ))


def process_folder(reg_folder):
    folder_path = os.path.join(BASE_DIR, reg_folder)

    print(f"\n🔄 Processing vehicle: {reg_folder}")

    conn = get_connection()
    cursor = conn.cursor()
    ncb_inserted = False
    

    try:
        for filename in os.listdir(folder_path):
            if not filename.endswith(".json"):
                continue

            file_path = os.path.join(folder_path, filename)

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            url = data.get("url", "")

            # 🔹 NCB API
            if "carapi/Quote/NcbData" in url and not ncb_inserted:
                api_name = "carapi/Quote/NcbData"

                insert_quotes_response(
                    cursor,
                    api_name,
                    url,
                    None,   # idv_selected not applicable
                    None,   # idv_type not applicable
                    data
                )
                ncb_inserted = True
                print(f"✅ Inserted NcbData from {filename}")
                continue

            # 🔹 Quotes API
            
            elif "carapi/Quote/Quotes" in url:
                api_name = "carapi/Quote/Quotes"

                bucket_list = data.get("data", {}).get("data", {}).get("bucketList", [])

                grouped_plans = []

                for bucket in bucket_list:
                    for plan in bucket.get("plans", []):
                        grouped_plans.append({
                            "planId": plan.get("planId"),
                            "idv": plan.get("idv"),
                            "planName": plan.get("planName")
                        })

                grouped_response = {
                    "original_response": data,
                    "plans_summary": grouped_plans
                }

                insert_quotes_response(
                    cursor,
                    api_name,
                    url,
                    None,   # idv_selected NULL
                    None,   # idv_type NULL
                    grouped_response
                )

                
                print(f"✅ Inserted Quotes (grouped) from {filename}")
                continue
            else:
                continue


        conn.commit()
        print("✔ Completed:", reg_folder)

    except Exception as e:
        conn.rollback()
        print("❌ Error:", e)

    finally:
        cursor.close()
        conn.close()


def main():
    if not os.path.exists(BASE_DIR):
        print(f"❌ {BASE_DIR} not found")
        return

    print("=" * 60)
    print("🚀 Ingesting NcbData & Quotes APIs into quotes_responses")
    print("=" * 60)
    # Load plan IDs once (performance optimization)
 

    # for reg_folder in os.listdir(BASE_DIR):
    #     reg_path = os.path.join(BASE_DIR, reg_folder)

    #     if os.path.isdir(reg_path):
    #         process_folder(reg_folder, default_plan_ids, median_plan_ids)
    target_vehicle = "MH04KW1827"

    reg_path = os.path.join(BASE_DIR, target_vehicle)

    if os.path.isdir(reg_path):
        process_folder(target_vehicle)
    else:
        print(f"❌ {target_vehicle} folder not found")
if __name__ == "__main__":
    main()