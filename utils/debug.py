import sys
import threading
import traceback
import psutil


def log_threads():
    thread_names = {t.ident: t.name for t in threading.enumerate()}
    for thread_id, frame in sys._current_frames().items():
        print("Thread %s:" % thread_names.get(thread_id, thread_id))
        traceback.print_stack(frame)


def log_processes():
    pids = psutil.pids()
    for pid in pids:
        print(psutil.Process(pid))


def log_open_resources():
    # Open files
    print("Open file descriptors:")
    for proc in psutil.Process().open_files():
        print(f" - {proc.path} (fd={proc.fd})")

    # Network connections
    print("Open network connections:")
    for conn in psutil.Process().net_connections():
        print(f" - {conn.fd} {conn.status} {conn.laddr} -> {conn.raddr}")


import atexit


def get_atexit_functions():
    funs = []

    class Capture:
        def __eq__(self, other):
            funs.append(other)
            return False

    c = Capture()
    atexit.unregister(c)  # type: ignore
    return funs


def qual_fn(obj):
    return f"{obj.__module__}.{obj.__qualname__}"


def log_atexit():
    print(f"Atexit handlers: {[qual_fn(fn) for fn in get_atexit_functions()]}")


def print_debug_info():
    log_threads()
    log_processes()
    log_open_resources()
    log_atexit()
