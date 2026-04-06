# db_live_sync.py

import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "policy_bazaar_scripts"))

from pb_logger import ScrapeLogger  # type: ignore
from db_v2 import (  # type: ignore
    insert_quotes_plandetails,
    insert_quotes_response,
    insert_run_log,
    create_scrape_run,
    insert_akamai_event,
)


def extract_api_name(url):
    return url.split('/')[-1] if url else "unknown"


class LiveDBSync:

    def __init__(self, run_id: str, conn):
        self.run_id = run_id
        self.last_log_id = None  # store last inserted step log id
        self.conn = conn

    # -------------------------
    # STEP PUSH
    # -------------------------
    def push_latest_step(self, log: ScrapeLogger):
        if not log.steps:
            return

        step = log.steps[-1]

        log_id = insert_run_log(
            conn=self.conn,
            run_id=self.run_id,
            step_number=step.get("step_number"),
            step_key=step.get("step_key"),
            status=step.get("status"),
            start_ts=step.get("start_ts"),
            end_ts=step.get("end_ts"),
            duration_ms=step.get("duration_ms"),
            data=step.get("data"),
        )

        self.last_log_id = log_id

    # -------------------------
    # AKAMAI PUSH
    # -------------------------
    def push_latest_akamai(self, log: ScrapeLogger):
        if not log.akamai_events:
            return

        event = log.akamai_events[-1]
        insert_akamai_event(
            conn=self.conn,
            run_id=self.run_id,
            log_id=self.last_log_id,
            step_after=event.get("step_after"),
            step_key_after=event.get("step_key_after"),
            event_timestamp=event.get("timestamp"),
            data=event.get("data"),
        )

    # -------------------------
    # LIVE API RESPONSE PUSH
    # -------------------------
    async def live_handle_response(self, response, idv_type, idv_value):
        try:
            url = response.url.lower()

            # 1️⃣ Only Policybazaar car APIs
            if "policybazaar.com/carapi" not in url:
                return

            # 2️⃣ Must be JSON response
            content_type = response.headers.get("content-type") or ""
            if "application/json" not in content_type:
                return

            # 3️⃣ Skip unwanted endpoints
            skip_keywords = [
                "api_local_ip",
                "v2_device_add",
                "customerVisitTracking",
                "InsertFunnelTracking",
            ]

            for keyword in skip_keywords:
                if keyword.lower() in url.lower():
                    return

            # 4️⃣ Parse JSON safely
            try:
                data = await response.json()
            except Exception:
                # This is the cause of: Expecting value: line 1 column 1
                return

            # 5️⃣ Skip status 201
            if response.status == 201:
                return

            # 6️⃣ Skip empty data responses
            if isinstance(data, dict):
                if data.get("data", "___MISSING___") is None:
                    return

            # 7️⃣ Extract clean API name
            api_name = extract_api_name(url)

           
            # Extra safety: skip random token APIs
            if len(api_name) > 80:
                return
            
            if "quote/plandetails" in url:
                request =  response.request
                post_data_json = request.post_data_json
                plan_id = post_data_json.get("planId") if post_data_json else None
                addon_combo_id = post_data_json.get("addonComboId") if post_data_json else None

                data["plan_id_data"] = {
                    "planId": plan_id,
                    "addonComboId": addon_combo_id
                }
                insurer_name = data.get("data", {}).get("insurer", "unknown")
                
                # 8️⃣ Insert into DB with extra plan_id_data
                insert_quotes_plandetails(
                    conn=self.conn,
                    run_id=self.run_id,
                    insurer_name=insurer_name,
                    plan_id=plan_id,    
                    plan_json=data,
                    addon_combo_id=addon_combo_id,
                    idv_selected=idv_value,
                    idv_type=idv_type
                )
            else :
                # 8️⃣ Insert into DB
                insert_quotes_response(
                    conn=self.conn,
                    run_id=self.run_id,
                    api_name=api_name,
                    api_url=url,
                    response_json=data,
                    idv_type=idv_type,
                    idv_selected=idv_value,
                )

            print(f"📡 LIVE API PUSHED: {api_name}")

        except Exception as e:
            print("Live API error:", str(e))

    # -------------------------
    # FINALIZE RUN
    # -------------------------
    def finalize_run(self, status: str, total_duration_ms: int = None, notes: str = None):
        """Finalize scrape run record in database"""
        create_scrape_run(
            conn=self.conn,
            run_id=self.run_id,
            status=status,
            ended_at=datetime.now(),
            total_duration_ms=total_duration_ms,
            notes=notes,
        )

        # Auto-trigger pipeline for SUCCESS runs
        if status == "SUCCESS":
            self._auto_run_pipeline()

    def _auto_run_pipeline(self):
        """Automatically run the quotes pipeline to populate final_flat_output."""
        try:
            backend_root = Path(__file__).resolve().parent.parent.parent
            db_flow_path = str(backend_root / "db_complete_flow")
            if db_flow_path not in sys.path:
                sys.path.insert(0, db_flow_path)

            import run_pipeline_v2
            print(f"\n🔄 Auto-running pipeline for run_id={self.run_id}")
            success = run_pipeline_v2.run_pipeline(self.run_id)
            if success:
                print(f"✅ Auto-pipeline completed for run_id={self.run_id}")
            else:
                print(f"⚠️  Auto-pipeline skipped (no data) for run_id={self.run_id}")
        except Exception as e:
            print(f"⚠️  Auto-pipeline failed for run_id={self.run_id}: {e}")
  