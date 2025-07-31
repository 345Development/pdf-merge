import os
from pathlib import Path
import signal
import tempfile

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
        job_dir = Path(job_dir)
        downloaded_files = vqf_manager.download_files(
            job.files_to_merge,
            job_dir,
            shutdown_handler=shutdown_handler,
            organisation_uuid=job.organisation_uuid,
        )

        if not downloaded_files:
            # interrupted
            return

        merger = PdfWriter()
        for pdf in downloaded_files:
            merger.append(pdf)

        output_path = job_dir / job.output_name
        if output_path.suffix.lower() != ".pdf":
            output_path = output_path.with_suffix(".pdf")

        merger.write(output_path)
        merger.close()

        vqf_manager.upload_files(
            job.destination_folder_uuid,
            [output_path],
            organisation_uuid=job.organisation_uuid,
        )

    log.log("cleaned up")


def run_with_jobs_system(
    api_settings: vq.api.ApiSettings,
    shutdown_handler: GracefulShutdownHandler,
):
    continuous = os.getenv("CONTINUOUS", "false").lower() == "true"

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
                        if job is None:
                            log.log("no jobs found, shutting down")
                            return

                        vqf_manager = VQFilesManager(
                            vq_url=api_settings.url, token=job.task_token
                        )

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

                        raise e

            except Exception as e:
                log.log("exception getting job from jobs system")
                log.error(exception=e)
                exceptiongroup.print_exception(e)

                return

            # continuous mode will do jobs until there are no jobs left
            # otherwise shuts down after 1 job
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

        api_settings = vq.api.get_api_key_details()

        run_with_jobs_system(
            api_settings=api_settings,
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
    os.environ["VQ_URL"] = "https://api.345.global"
    os.environ["VQ_KEY"] = "ze9BOS091EGUffol"
    main()
