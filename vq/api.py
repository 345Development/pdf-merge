import os
from dataclasses import dataclass
from typing import Any, Dict

from utils import version


@dataclass
class ApiSettings:
    url: str
    headers: Dict[str, Any]


def get_user_agent_string() -> str:
    return f"pdfmerge/{version.get_build_date()}-{version.get_git_short_hash()}"


def get_api_key_details() -> ApiSettings:
    if not (key := os.getenv("VQ_KEY")):
        raise ValueError("No VQ_KEY available (check secrets?)")
    if not (url := os.getenv("VQ_URL")):
        raise ValueError("No VQ_URL available (check yaml?)")

    return ApiSettings(
        url=url,
        headers={"X-API-KEY": key, "User-Agent": get_user_agent_string()},
    )
