# pb_logger.py
import json
import logging
import time
import atexit
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional


class ScrapeLogger:
    def __init__(self, base_dir: Optional[str], run_id: str, car_number: str):
        self.base_dir = base_dir
        self.run_id = run_id
        self.car_number = car_number
 
        self.start_time = time.time()

        self.log_dir = None
        if base_dir:
            self.log_dir = Path(base_dir) / "logs"
            self.log_dir.mkdir(parents=True, exist_ok=True)

        self.step_start_times = {}
        # ===============================
        # Structured Run Builder
        # ===============================
        self.run_start_ts = datetime.now(timezone.utc).isoformat()
        self.run_status = "RUNNING"

        self.steps = []
        self.current_step_data = {}

        self.akamai_events = []


        self._setup_loggers()

        # Ensure files are closed even if process crashes
        atexit.register(self._close)

        self.info("RUN_START", f"Scrape started for car={car_number}")

    def _setup_loggers(self):
        # Human readable logger
        self.logger = logging.getLogger(f"pb_run_{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        # Prevent duplicate handlers
        if self.logger.handlers:
            return

        if self.log_dir:
            log_file = self.log_dir / "run.log"
            fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s %(message)s",
                datefmt="%H:%M:%S"
            )
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

            # JSON logger (machine readable)
            self.json_file = open(self.log_dir / "run.jsonl", "a", encoding="utf-8")
        else:
            # Fallback to console logging when file output is disabled
            ch = logging.StreamHandler()
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s %(message)s",
                datefmt="%H:%M:%S"
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
            self.json_file = None

    # ---------------- BASIC LOGGING ----------------
    def info(self, code: str, msg: str, **extra):

        # Special handling for AKAMAI
        if code == "AKAMAI":
            akamai_ts = datetime.now(timezone.utc).isoformat()

            # Find last completed step
            if self.steps:
                last_step = sorted(self.steps, key=lambda x: x["end_ts"])[-1]
                step_after = last_step["step_number"]
                step_key_after = last_step["step_key"]
            else:
                step_after = 0
                step_key_after = "RUN_START"

            self.akamai_events.append({
                "step_after": step_after,
                "step_key_after": step_key_after,
                "timestamp": akamai_ts,
                "data": extra
            })

        self._write("INFO", code, msg, extra)

  

    def warn(self, code: str, msg: str, **extra):
        self._write("WARN", code, msg, extra)

    def error(self, code: str, msg: str, **extra):
        self._write("ERROR", code, msg, extra)

    # ---------------- STEP LOGGING ----------------
    #     self.current_step_data[step] = {
    #         "step_number": step_number,
    #         "step_key": step_key,
    #         "status": "RUNNING",
    #         "start_ts": datetime.now(timezone.utc).isoformat(),
    #         "data": {}
    #     }
    # Extract section info if provided
    def step_start(self, step: str, msg: str, **extra):
        self.step_start_times[step] = time.time()

        # Extract step number and key
        parts = step.split("_", 2)
        step_number = int(parts[1]) if len(parts) > 2 else 0
        step_key = parts[2] if len(parts) > 2 else step

        # section_name = extra.pop("section_name", None)
        # section_selector = extra.pop("section_selector", None)

        self.current_step_data[step] = {
            "step_number": step_number,
            "step_key": step_key,
            "status": "RUNNING",
            "start_ts": datetime.now(timezone.utc).isoformat(),
            # "section": {
            #     "name": section_name,
            #     "selector": section_selector
            # } if section_name else None,
            "data": {}
        }

        self.info(step, f"START: {msg}", **extra)
        
    def step_success(self, step: str, **extra):
        duration = self._duration(step)

        step_data = self.current_step_data.pop(step, {})
        step_data["end_ts"] = datetime.now(timezone.utc).isoformat()
        step_data["duration_ms"] = duration
        step_data["status"] = "SUCCESS"

        # Store extra fields inside data
        step_data.setdefault("data", {}).update(extra)

        self.steps.append(step_data)

        self.info(step, "SUCCESS", duration_ms=duration, **extra)

    
    def step_fail(self, step: str, error: Exception = None, **extra):
        duration = self._duration(step)
        msg = f"FAILED: {str(error)}" if error else "FAILED"

        step_data = self.current_step_data.pop(step, {})
        step_data["end_ts"] = datetime.now(timezone.utc).isoformat()
        step_data["duration_ms"] = duration
        step_data["status"] = "FAILED"
        step_data.setdefault("data", {}).update(extra)

        self.steps.append(step_data)

        self.error(step, msg, duration_ms=duration, **extra)
   

    
    # 🔁 ALIASES (to match scraper usage)
    def step_error(self, step: str, error: str = None, **extra):
        self.step_fail(step, Exception(error) if error else None, **extra)

    # ---------------- SPECIAL EVENTS ----------------
    def captcha_detected(self, provider: str):
        self.warn("CAPTCHA", "Captcha detected", provider=provider)

    def redirect(self, from_url: str, to_url: str):
        self.info("REDIRECT", "Page redirected", from_url=from_url, to_url=to_url)

    def navigation(self, url: str):
        self.info("NAVIGATE", "Navigating", url=url)

    # ---------------- RUN END ----------------
    def run_success(self):
        total = int((time.time() - self.start_time) * 1000)
        self.run_status = "SUCCESS"

        run_end_ts = datetime.now(timezone.utc).isoformat()

        final_output = {
            "run_id": self.run_id,
            "car": self.car_number,
            "run_summary": {
                "start_ts": self.run_start_ts,
                "end_ts": run_end_ts,
                "total_duration_ms": total,
                "status": self.run_status
            },
            "steps": sorted(self.steps, key=lambda x: x["step_number"]),
            "akamai_events": self.akamai_events
        }

        if self.log_dir:
            structured_file = self.log_dir / "structured_run.json"
            with open(structured_file, "w", encoding="utf-8") as f:
                json.dump(final_output, f, indent=2)

        self.info("RUN_END", "Scrape completed successfully", total_ms=total)
        self._close()

    def run_fail(self, error: Exception):
        total = int((time.time() - self.start_time) * 1000)
        self.run_status = "FAILED"

        run_end_ts = datetime.now(timezone.utc).isoformat()

        final_output = {
            "run_id": self.run_id,
            "car": self.car_number,
            "run_summary": {
                "start_ts": self.run_start_ts,
                "end_ts": run_end_ts,
                "total_duration_ms": total,
                "status": self.run_status
            },
            "steps": sorted(self.steps, key=lambda x: x["step_number"]),
            "akamai_events": self.akamai_events
        }

        self.error("RUN_END", f"Scrape failed: {error}", total_ms=total)
        self._close()

    # 🔁 ALIAS (to match scraper usage)
    def run_error(self, error: str = None, **extra):
        self.run_fail(error)

    # ---------------- INTERNAL ----------------
    def _duration(self, step: str):
        start = self.step_start_times.pop(step, None)
        return int((time.time() - start) * 1000) if start else None

    def _write(self, level, code, msg, extra):
        text = f"{code} | {msg}"
        getattr(self.logger, level.lower())(text)

        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "code": code,
            "message": msg,
            "run_id": self.run_id,
            "car": self.car_number,
            **extra
        }
        if self.json_file:
            self.json_file.write(json.dumps(record) + "\n")
            self.json_file.flush()

    def _close(self):
        try:
            if not self.json_file.closed:
                self.json_file.close()
        except Exception:
            pass
