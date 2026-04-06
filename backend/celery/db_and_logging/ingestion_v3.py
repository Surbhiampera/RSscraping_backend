#car_info and quotes_details ingestion script
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


# ✅ Hardcoded run_id
HARDCODED_RUN_ID = "11111111-1111-1111-1111-111111111111"


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


def extract_ids_from_filename(filename):
    match = re.match(r"plan_(\d+)_(\d+)\.json", filename)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def insert_quote(cursor, insurer_name, plan_id, addon_combo_id, data, idv_selected, idv_type):
    cursor.execute("""
        INSERT INTO quotes_details (
            run_id,
            insurer_name,
            plan_id,
            plan_json,
            addon_combo_id,
            idv_selected,
            idv_type
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        HARDCODED_RUN_ID,
        insurer_name,
        plan_id,
        Json(data),
        addon_combo_id,
        idv_selected,
        idv_type
    ))


def insert_car_info(cursor, folder_name, data):
    url = data.get("url", "")

    # Only process CarDetails API
    if "carapi/Quote/CarDetails" not in url:
        return

    print("🚗 CarDetails API found")

    api_outer = data.get("data", {})
    api_data = api_outer.get("data", {})

    if not api_data:
        print("No inner data found")
        return

    registration_no = api_data.get("registrationNumber")
    variant = api_data.get("variant")
    fuel_type = api_data.get("fuelType")
    registration_date = api_data.get("registrationDate")

    ga_obj = api_data.get("gaEventObj", {})

    make_name = ga_obj.get("makeName")
    model_name = ga_obj.get("modelName")
    city_tier = ga_obj.get("cityTier")
    car_age = ga_obj.get("carAge")

    vehicle_variant = variant
    raw_cc = api_data.get("cubicCapacity")

    cubic_capacity = None

    if raw_cc:
        raw_str = str(raw_cc).lower().strip()
        num_match = re.search(r"\d+(\.\d+)?", raw_str)

        if num_match:
            value = float(num_match.group())
            if "l" in raw_str and "cc" not in raw_str:
                cubic_capacity = int(value * 1000)
            else:
                cubic_capacity = int(value)

    state_code = registration_no[:2] if registration_no else None

    print("INSERTING:", registration_no)

    # cursor.execute("""
    #     INSERT INTO car_info (
    #         run_id,
    #         registration_number,
    #         make_name,
    #         model_name,
    #         vehicle_variant,
    #         fuel_type,
    #         cubic_capacity,
    #         state_code,
    #         city_tier,
    #         car_age,
    #         registration_date
    #     )
    #     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    # """, (
    #     HARDCODED_RUN_ID,
    #     registration_no,
    #     make_name,
    #     model_name,
    #     vehicle_variant,
    #     fuel_type,
    #     cubic_capacity,
    #     state_code,
    #     city_tier,
    #     car_age,
    #     registration_date
    # ))
    cursor.execute("""
        INSERT INTO car_info (
            run_id,
            registration_number,
            make_name,
            model_name,
            vehicle_variant,
            fuel_type,
            cubic_capacity,
            state_code,
            city_tier,
            car_age,
            registration_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (run_id, registration_number)
        DO NOTHING
    """, (
        HARDCODED_RUN_ID,
        registration_no,
        make_name,
        model_name,
        vehicle_variant,
        fuel_type,
        cubic_capacity,
        state_code,
        city_tier,
        car_age,
        registration_date
    ))

def process_validation_folder(folder_path, excel_base_dir, mode):
    print(f"\n🔄 Processing folder: {folder_path}")
    folder_name = os.path.basename(folder_path)

    excel_path = f"{excel_base_dir}/{folder_name}_policybazaar_data.xlsx"

    if not os.path.exists(excel_path):
        print(f"❌ Excel file not found: {excel_path}")
        return

    df = pd.read_excel(excel_path)

    raw_idv = str(df["Selected IDV"].iloc[0])
    idv_selected = int(re.search(r"\d+", raw_idv).group())

    if mode == "median":
        idv_type = "median"
    else:
        type_match = re.search(r"\((.*?)\)", raw_idv)
        idv_type = type_match.group(1) if type_match else "clarity"

    print("Using IDV Selected:", idv_selected)
    print("Using IDV Type:", idv_type)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        for filename in os.listdir(folder_path):
            if not filename.endswith(".json"):
                continue

            if filename == "_execution_metadata.json":
                continue

            plan_id, addon_combo_id = extract_ids_from_filename(filename)

            if not plan_id:
                print(f"⚠ Invalid filename: {filename}")
                continue

            file_path = os.path.join(folder_path, filename)

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # insurer_name = (
            #     data.get("api_response", {})
            #         .get("data", {})
            #         .get("insurer")
            # )
            raw_insurer = (
                data.get("api_response", {})
                    .get("data", {})
                    .get("insurer")
            )

            if raw_insurer:
                insurer_name = re.sub(r"General Insurance.*", "", raw_insurer).strip()
            else:
                insurer_name = None

            insert_quote(
                cursor,
                insurer_name,
                plan_id,
                addon_combo_id,
                data,
                idv_selected,
                idv_type
            )
            print(f"✅ Inserted: {filename}")

        conn.commit()
        print("✔ Folder completed")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")

    finally:
        cursor.close()
        conn.close()


def process_car_details_folder(base_dir):
    print(f"\n🚗 Processing CarDetails from: {base_dir}")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        for reg_folder in os.listdir(base_dir):
            reg_path = os.path.join(base_dir, reg_folder)

            if not os.path.isdir(reg_path):
                continue

            print(f"➡ Processing vehicle: {reg_folder}")

            for filename in os.listdir(reg_path):
                if not filename.endswith(".json"):
                    continue

                file_path = os.path.join(reg_path, filename)

                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                insert_car_info(cursor, reg_folder, data)
        # ✅ Process only MH04kw1827
        # reg_folder = "MH04kw1827"
        # reg_path = os.path.join(base_dir, reg_folder)

        # if not os.path.exists(reg_path):
        #     print("Folder not found")
        #     return

        # print(f"➡ Processing vehicle: {reg_folder}")

        # for filename in os.listdir(reg_path):
        #     if not filename.endswith(".json"):
        #         continue

        #     file_path = os.path.join(reg_path, filename)

        #     with open(file_path, "r", encoding="utf-8") as f:
        #         data = json.load(f)

        #     insert_car_info(cursor, reg_folder, data)
        conn.commit()
        print("✔ CarDetails processing completed")

    except Exception as e:
        conn.rollback()
        print("❌ Error in CarDetails:", e)

    finally:
        cursor.close()
        conn.close()


def main():
    jobs = [
        {
            "base_dir": "plans_json_validation",
            "excel_dir": "policy_bazaar_data_new",
            "mode": "auto"
        },
        {
            "base_dir": "plans_median",
            "excel_dir": "policy_bazaar_data_median",
            "mode": "median"
        }
    ]

    for job in jobs:
        base_dir = job["base_dir"]

        if not os.path.exists(base_dir):
            print(f"❌ {base_dir} directory not found.")
            continue

        print(f"\n📁 Reading from: {os.path.abspath(base_dir)}")

        for folder in os.listdir(base_dir):
            folder_path = os.path.join(base_dir, folder)

            if os.path.isdir(folder_path):
                process_validation_folder(
                    folder_path,
                    job["excel_dir"],
                    job["mode"]
                )

        # ✅ ADDITIONAL: Process CarDetails separately
        car_details_dir = "policy_bazaar_responses_validation"

        if os.path.exists(car_details_dir):
            process_car_details_folder(car_details_dir)
        else:
            print("❌ policy_bazaar_responses_validation folder not found")


if __name__ == "__main__":
    main()
