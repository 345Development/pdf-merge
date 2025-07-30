import subprocess
import os


def get_git_short_hash() -> str:
    if (commit := os.getenv("GIT_COMMIT")) is not None:
        return commit
    else:
        try:
            return (
                subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
                .decode("ascii")
                .strip()
            )
        except:
            return "unknown"


def get_build_date() -> str:
    return os.getenv("BUILD_DATE", "")
