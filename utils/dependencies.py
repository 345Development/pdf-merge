from enum import Enum, auto
import utils.logging as log
from threading import Event


class Reason(Enum):
    SYS_INTERRUPT = auto()
    JOB_CANCELLED = auto()


class GracefulShutdownHandler:
    def __init__(self):
        self.event = Event()
        self.reason = None
        self.message = ""

    @property
    def interrupted(self):
        return self.event.is_set()

    def shutdown(self, reason: Reason, message: str = ""):
        if self.reason is not None:
            log.error(
                f"tried to shutdown job already shut down new {reason=} old {self.reason=}"
            )
            return

        log.log(f"shutting down {reason=} - {message}")
        self.event.set()
        self.reason = reason
        self.message = message

    def reset(self):
        self.event.clear()
        self.reason = None
        self.message = ""

    def wait(self, timeout: float):
        self.event.wait(timeout)
