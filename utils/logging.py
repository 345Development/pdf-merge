import functools
import sys
import threading
import time
from typing import Callable, Iterable, List, Optional, TypeVar
from typing_extensions import ParamSpec
from tqdm import tqdm


# TODO: rewrite this to not use a file as something is going wrong that messes up jobs shutting down
# class Prefix:
#     PREFIX_FILENAME = Path("temp-jobname.txt")
#     _prefix_cache: Optional[str] = None


class _Prefix:
    _prefix_stack: List[str] = [""]


class WithLogPrefix:
    def __init__(self, prefix: str):
        _Prefix._prefix_stack.append(prefix)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _Prefix._prefix_stack.pop()
        return False


def log(message: str, **kwargs):
    print(f"{_Prefix._prefix_stack[-1]}{message}", **kwargs)


P = ParamSpec("P")
T = TypeVar("T")


def log_errors(func: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(func)
    def with_error_log(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            tb = e.__traceback__

            # skip this file (so the log doesn't just read line xx of logging.py)
            while tb and tb.tb_frame.f_code.co_filename == __file__:
                tb = tb.tb_next

            e = e.with_traceback(tb)

            error(exception=e)
            raise e

    return with_error_log


def error(
    message: Optional[str] = None, exception: Optional[Exception] = None, **kwargs
):
    if exception is not None:
        if exception.__traceback__:
            filelines = []
            tb = exception.__traceback__
            while tb is not None:
                filename = tb.tb_frame.f_code.co_filename
                lineno = tb.tb_lineno
                fileline = f"{filename} line {lineno}"
                filelines.append(fileline)
                tb = tb.tb_next

            error = ", ".join(filelines) + f": {repr(exception)}"
        else:
            error = repr(exception)

        if message is not None:
            message = f"{message}{error}"
        else:
            message = error

    if message is None:
        message = ""

    log(message, file=sys.stderr, **kwargs)


# def progress_bar(iterable: Iterable, total: Optional[int], desc: Optional[str]):
#     yield from CloudProgress(iterable=iterable, total=total, desc=desc)


T = TypeVar("T")


class progress_bar(Iterable[T]):
    def __init__(
        self,
        iterable: Optional[Iterable[T]] = None,
        total: Optional[int] = None,
        desc: str = "",
    ):
        if total is None and iterable is not None:
            try:
                total = len(iterable)  # type: ignore
            except (TypeError, AttributeError):
                total = None

        self.iterable = iterable
        self.total = total
        self.desc = desc

        self.n = 0
        self.start_time = time.time()

        self.time_interval = 30
        if total is None:
            self.n_interval = None
        else:
            self.n_interval = max(total // 10, 1)

        self.last_print_time = None
        self.last_print_n = None

        self.lock = threading.Lock()

    def should_print(self):
        if self.last_print_time is None or self.last_print_n is None:
            return True

        if self.n == self.total:
            return True

        should_print = (time.time() - self.last_print_time) > self.time_interval

        if self.n_interval:
            should_print |= (self.n - self.last_print_n) > self.n_interval

        return should_print

    @property
    def total_time(self):
        return time.time() - self.start_time

    @property
    def est_time_left(self):
        assert self.total is not None
        return (self.total - self.n) * (self.total_time / self.n)

    def print_progress(self):
        if self.n == 0:
            if self.total is not None:
                log(f"{self.desc} starting ({self.total} total)")
            else:
                log(f"{self.desc} starting (unknown total)")
        elif self.n == self.total:
            log(f"{self.desc} finished (total {tqdm.format_interval(self.total_time)})")
        else:
            if self.total is not None:
                log(
                    f"{self.desc}: {self.n}/{self.total}"
                    f" - {100*self.n/self.total:.1f}%"
                    f" - time {tqdm.format_interval(self.total_time)}"
                    f" - est. remaining {tqdm.format_interval(self.est_time_left)}"
                )
            else:
                log(
                    f"{self.desc}: {self.n}/?"
                    f" - time: {tqdm.format_interval(self.total_time)}"
                    f" - speed: {self.n / self.total_time} itr/s"
                )

        self.last_print_n = self.n
        self.last_print_time = time.time()

    def refresh(self):
        if self.should_print():
            self.print_progress()

    def update(self, n: int = 1):
        with self.lock:
            self.n += n
        self.refresh()

    def __iter__(self):
        if self.iterable is None:
            raise ValueError(
                "Cannot iterate when progress bar not created with iterable"
            )

        self.start_time = time.time()
        self.print_progress()

        for obj in self.iterable:
            yield obj
            self.update()

        if self.total != self.n:
            self.total = self.n
            self.print_progress()
