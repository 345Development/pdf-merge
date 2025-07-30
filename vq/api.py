import os
from dataclasses import dataclass
from typing import Any, Dict

import utils


@dataclass
class ApiSettings:
    url: str
    headers: Dict[str, Any]


def get_api_details() -> ApiSettings:
    if not (key := os.getenv("VQ_KEY")):
        raise ValueError("No VQ_KEY available (check secrets?)")
    if not (url := os.getenv("VQ_URL")):
        raise ValueError("No VQ_URL available (check yaml?)")

    return ApiSettings(
        url=url,
        headers={
            "X-API-KEY": key,
            "User-Agent": f"product-capture/{utils.get_build_date()}-{utils.get_git_short_hash()}",
        },
    )
