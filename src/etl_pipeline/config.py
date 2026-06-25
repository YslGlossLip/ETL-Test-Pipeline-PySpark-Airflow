from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUALITY_DIR = DATA_DIR / "quality"


IMPRESSIONS_FILE_NAME = "interview.X.csv"
EVENTS_FILE_NAME = "interview.y.csv"

IMPRESSIONS_INPUT_PATH = INPUT_DIR / IMPRESSIONS_FILE_NAME
EVENTS_INPUT_PATH = INPUT_DIR / EVENTS_FILE_NAME


REQUIRED_IMPRESSION_COLUMNS = [
    "reg_time",
    "uid",
    "fc_imp_chk",
    "fc_time_chk",
    "utmtr",
    "mm_dma",
    "osName",
    "model",
    "hardware",
    "site_id"
    ]

REQUIRED_EVENT_COLUMNS = [
    "uid",
    "tag"
    ]

MART_DEFINITIONS = {
    "mart_by_mm_dma": "mm_dma",
    "mart_by_site_id": "site_id",
    "mart_by_hardware": "hardware"
    }