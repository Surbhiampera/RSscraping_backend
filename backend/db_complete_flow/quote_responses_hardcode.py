import os
import json
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

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


def insert_quotes_response(cursor, api_name, api_url, idv_selected, idv_type, full_json):
    try:
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
            int(idv_selected),
            Json(full_json)
        ))
        print(f"✅ Inserted row: {api_name}, {idv_type}, IDV={int(idv_selected)}")
    except Exception as e:
        print(f"❌ Failed to insert {api_name} {idv_type}: {e}")
        raise


def process_folder(reg_folder):
    folder_path = os.path.join(BASE_DIR, reg_folder) if reg_folder else BASE_DIR
    print(f"\n🔄 Processing vehicle: {reg_folder}")

    if not os.path.exists(folder_path):
        print(f"❌ Folder not found: {folder_path}")
        return

    conn = get_connection()
    cursor = conn.cursor()
    

    try:
        for filename in os.listdir(folder_path):
            if not filename.endswith(".json"):
                continue

            file_path = os.path.join(folder_path, filename)
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            url = data.get("url", "")
            
            # -------------------
            # CarDetails API
            # -------------------
            if "carapi/Quote/CarDetails" in url:
                api_name = "carapi/Quote/CarDetails"

                insert_quotes_response(cursor, api_name, url, 213840, "default", data)
                insert_quotes_response(cursor, api_name, url, 420032, "median", data)

                print(f"✅ Inserted CarDetails data from {filename}")
                continue

            # -------------------
            # NCB API
            # -------------------
            if "carapi/Quote/NcbData" in url:
                api_name = "carapi/Quote/NcbData"

                insert_quotes_response(cursor, api_name, url, 213840, "default", data)
                insert_quotes_response(cursor, api_name, url, 420032, "median", data)
                
                print(f"✅ Inserted NCB data from {filename}")
                continue

            # -------------------
            # Quotes API
            # -------------------

            elif "carapi/Quote/Quotes" in url:
                api_name = "carapi/Quote/Quotes"

                bucket_list = data.get("data", {}).get("data", {}).get("bucketList", [])

                # ----- Extract combo IDs from planDetails -----
                combo_ids = []
                for bucket in bucket_list:
                    for plan in bucket.get("plans", []):
                        for plan_detail in plan.get("planDetails", []):
                            combo_id = plan_detail.get("addonComboId")
                            if combo_id is not None:
                                combo_ids.append(combo_id)
                print(f"   🔹 Combo IDs in {filename}: {combo_ids}")
                # ---------------------------------------------

                grouped_plans = []
                for bucket in bucket_list:
                    for plan in bucket.get("plans", []):
                        for plan_detail in plan.get("planDetails", []):
                            grouped_plans.append({
                                "planId": plan_detail.get("planId"),
                                "idv": plan.get("idv"),
                                "planName": plan_detail.get("planName"),
                                "addonComboId": plan_detail.get("addonComboId")
                            })

                grouped_response = {
                    "original_response": data,
                    "plans_summary": grouped_plans
                }

                # Insert both default & median
                insert_quotes_response(cursor, api_name, url, 213840, "default", grouped_response)
                insert_quotes_response(cursor, api_name, url, 420032, "median", grouped_response)
                print(f"✅ Inserted Quotes data from {filename}")
                continue
        conn.commit()
        print("✔ Completed processing folder:", reg_folder)

    except Exception as e:
        conn.rollback()
        print("❌ Error during processing:", e)

    finally:
        cursor.close()
        conn.close()

def main():
    print("=" * 60)
    print("🚀 Ingesting NCB & Quotes APIs into quotes_responses")
    print("=" * 60)

    items = os.listdir(BASE_DIR)

    # Case 1: JSON files directly inside BASE_DIR
    if any(file.endswith(".json") for file in items):
        process_folder("")

    # Case 2: vehicle folders inside BASE_DIR
    else:
        for reg_folder in items:
            folder_path = os.path.join(BASE_DIR, reg_folder)
            if os.path.isdir(folder_path):
                process_folder(reg_folder)


if __name__ == "__main__":
    main()