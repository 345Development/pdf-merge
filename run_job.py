from datetime import datetime
from functools import partial
import os
from pathlib import Path
import signal
import tempfile
import time
from typing import Callable, List
from uuid import UUID

from utils.dependencies import Reason
import utils.logging as log
from utils import GracefulShutdownHandler
import utils
from vq.files import VQFilesManager
from vq.jobs_manager import Job, JobsSystemManager, WorkerRegistration
import vq.api
import exceptiongroup
from pypdf import PdfWriter


def run_job(
    vqf_manager: VQFilesManager, job: Job, shutdown_handler: GracefulShutdownHandler
):
    with tempfile.TemporaryDirectory() as job_dir:
        try:
            job_dir = Path(job_dir)
            downloaded_files = vqf_manager.download_files(
                job.files_to_merge, job_dir, shutdown_handler=shutdown_handler
            )

            if not downloaded_files:
                # interrupted
                return

            merger = PdfWriter()
            for pdf in downloaded_files:
                merger.append(pdf)

            output_path = (
                job_dir / "merged.pdf"
            )  # TODO: for olly - what output filename? make input?
            merger.write(output_path)
            merger.close()

            vqf_manager.upload_files(job.destination_folder_uuid, [output_path])

        except Exception as e:
            # TODO: for olly - do you want it to output a file containing the error message if there's an error?
            # if no, can just delete this entire exception block, and it'll still get logged)
            import exceptiongroup

            log.log("Attempting to write error log to vq files folder")

            error_path = job_dir / Path(
                f"error_log_{job.task_uuid}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
            )
            exception_text = exceptiongroup.format_exception(e)
            with open(error_path, "wt") as error_file:
                error_file.writelines(exception_text)
            vqf_manager.upload_files(job.destination_folder_uuid, [error_path])

            raise e

    log.log("cleaned up")


def run_with_jobs_system(
    api_settings: vq.api.ApiSettings,
    vqf_manager: VQFilesManager,
    shutdown_handler: GracefulShutdownHandler,
):
    continuous = os.getenv("CONTINUOUS", "false").lower() == "true"
    sleep_time = int(os.getenv("SLEEP_TIME", "60"))

    error_count = 0

    worker_details = WorkerRegistration(
        service_name="pdf-merge",
        major_version=0,
        minor_version=1,
        patch_version=0,
    )

    with JobsSystemManager(
        api_settings=api_settings,
        worker_details=worker_details,
        heartbeat_interval=10,
        claim_duration=600,
        shutdown_handler=shutdown_handler,
    ) as jsm:
        while True:
            if shutdown_handler.interrupted:
                if not continuous or shutdown_handler.reason != Reason.JOB_CANCELLED:
                    return

                # if just one job is cancelled then clear status, but keep object (for sigint etc)
                shutdown_handler.reset()

            try:
                with jsm.get_job() as job:
                    try:
                        error_count = 0

                        if job is None:
                            if continuous:
                                log.log("no jobs found, waiting... (continuous mode)")
                                time.sleep(sleep_time)
                                continue
                            else:
                                log.log("no jobs found, shutting down")
                                return

                        with log.WithLogPrefix(f"345pdf: {str(job.task_uuid)[:8]} - "):
                            run_job(
                                vqf_manager=vqf_manager,
                                job=job,
                                shutdown_handler=shutdown_handler,
                            )

                    except Exception as e:
                        log.log(f"exception processing job {job}")
                        log.error(exception=e)

                        exceptiongroup.print_exception(e)

                        if not continuous:
                            return

                        if error_count >= 5:
                            log.error("had successive errors so shutting down")
                            return

                        error_count += 1
            except Exception as e:
                log.log("exception getting job from jobs system")
                log.error(exception=e)
                exceptiongroup.print_exception(e)

                if error_count >= 5:
                    log.error("had successive errors so shutting down")
                    return

                error_count += 1

            if not continuous:
                break

            log.log("checking for new job...")
    log.log("exiting")


def run_cloud():
    shutdown_handler = GracefulShutdownHandler()

    def interrupt(*args):
        signum = args[0]
        name = signal.Signals(signum).name
        shutdown_handler.shutdown(
            reason=Reason.SYS_INTERRUPT,
            message=f"interrupted by host with {name} ({signum})",
        )

    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)

    with log.WithLogPrefix("345pdf: "):
        log.log(f"version info: {utils.get_build_date()}-{utils.get_git_short_hash()}")
        log.log("getting VQ details")

        api_settings = vq.api.get_api_details()

        org = os.getenv("ORGANISATION_UUID")
        if org is None:
            raise ValueError(
                "ORGANISATION_UUID env variable must be set for read/write to VQ Files"
            )

        vqf_manager = VQFilesManager(
            api_settings=api_settings, organisation_uuid=UUID(org)
        )

        run_with_jobs_system(
            api_settings=api_settings,
            vqf_manager=vqf_manager,
            shutdown_handler=shutdown_handler,
        )
        log.log("about to exit")
    log.log("exiting....")


def main():
    run_cloud()

    # force exit - ensures jobs exit completely (might not be necessary)
    def timeout_handler(signum, frame):
        print("Timeout reached - forcing shutdown with os._exit(1)")
        os._exit(1)

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(10)


if __name__ == "__main__":
    main()
