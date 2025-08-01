from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import threading
import time
from typing import Generator, List, Optional
from pydantic import BaseModel
from uuid import UUID

import humps
import requests

from utils.dependencies import GracefulShutdownHandler, Reason
from utils.k8s import update_pod_deletion_cost
from vq.api import ApiSettings
import utils.logging as log


@dataclass
class Job:
    task_uuid: UUID
    files_to_merge: List[UUID]
    destination_folder_uuid: UUID
    output_name: str
    organisation_uuid: UUID
    task_token: str

    @staticmethod
    def from_claim(claim: ClaimResponse) -> Job:
        config = claim.task_configuration

        files_to_merge = [UUID(file) for file in config["filesToMerge"]]
        destination_folder = UUID(config["destinationFolder"])
        output_name = config["outputName"]
        org_uuid = config["organisationUuid"]

        uuid = claim.task_uuid

        return Job(
            task_uuid=uuid,
            files_to_merge=files_to_merge,
            destination_folder_uuid=destination_folder,
            output_name=output_name,
            organisation_uuid=org_uuid,
            task_token=claim.task_token,
        )


class CamelModel(BaseModel):
    class Config:
        alias_generator = humps.camelize
        populate_by_name = True


class BaseModelFromDB(CamelModel):
    uuid: UUID
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime]


class Service(BaseModelFromDB):
    name: str
    major_version: int
    minor_version: int
    patch_version: int


class WorkerRegistration(CamelModel):
    service_name: str
    channel: str = "generic"
    friendly_name: Optional[str] = None
    major_version: int
    minor_version: int
    patch_version: int


class Worker(BaseModelFromDB):
    service: Service
    channel: str
    friendly_name: Optional[str] = None
    last_poll: datetime
    active: bool


class ClaimResponse(CamelModel):
    claim_uuid: UUID
    claim_expires: datetime
    task_uuid: UUID
    task_token: str
    task_configuration: dict
    task_retries: int
    task_retry_count: int


class JobsSystemHeartbeat:
    api_settings: ApiSettings
    worker_uuid: UUID
    task_uuid: UUID
    claim_uuid: UUID
    shutdown_handler: GracefulShutdownHandler
    interval: float
    extension_duration: float
    running: bool
    _stop: bool
    _thread: Optional[threading.Thread]

    def __init__(
        self,
        api_settings: ApiSettings,
        worker_uuid: UUID,
        task_uuid: UUID,
        claim_uuid: UUID,
        shutdown_handler: GracefulShutdownHandler,
        interval: float = 10,
        extension_duration: float = 600,
    ):
        self.api_settings = api_settings

        self.worker_uuid = worker_uuid
        self.task_uuid = task_uuid
        self.claim_uuid = claim_uuid

        self.interval = interval
        self.extension_duration = extension_duration

        self.shutdown_handler = shutdown_handler

        self.running = False
        self._stop = False
        self._thread = None

    def _heartbeat(self):
        heartbeat_url = (
            f"{self.api_settings.url}/api/v1/jobs/tasks/{self.task_uuid}/poll"
            f"?workerUuid={self.worker_uuid}"
            f"&claimUuid={self.claim_uuid}"
            f"&extension={self.extension_duration}"
        )

        response = requests.post(heartbeat_url, headers=self.api_settings.headers)
        response.raise_for_status()

        status = response.json()["status"]
        if status == "cancelled":
            self.shutdown_handler.shutdown(
                reason=Reason.JOB_CANCELLED, message="cancelled by jobs system"
            )
            self.stop()
        elif status != "in progress":
            log.error(f"warning - unexpected status from heartbeat {status}")

    def _loop(self):
        error_count = 0
        while True:
            time.sleep(self.interval)
            if self._stop:
                self.running = False
                return
            else:
                try:
                    self._heartbeat()
                    error_count = 0
                except Exception as e:
                    log.error("a heartbeat failed")
                    log.error(exception=e)
                    error_count += 1
                    if error_count >= 5:
                        log.error("heartbeats failing successively, stopping loop")
                        self.running = False
                        raise e

    def start(self):
        self._thread = threading.Thread(target=self._loop)
        self._thread.start()
        self.running = True

    def stop(self):
        self._stop = True

    def wait_to_finish(self):
        if not self._stop:
            raise ValueError("Must stop heartbeat before waiting to finish")

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval * 10)
            if self._thread.is_alive():
                raise TimeoutError("Heartbeat thread stuck alive")


class JobsSystemManager:
    def __init__(
        self,
        api_settings: ApiSettings,
        shutdown_handler: GracefulShutdownHandler,
        worker_details: WorkerRegistration,
        heartbeat_interval=10,
        claim_duration=600,
    ):
        self.api_settings = api_settings
        self.heartbeat_interval = heartbeat_interval
        self.claim_duration = claim_duration
        self.shutdown_handler = shutdown_handler
        self.worker_details = worker_details

        self.heartbeat: Optional[JobsSystemHeartbeat] = None

    def __enter__(self):
        self.worker = self.__register_worker()
        update_pod_deletion_cost(-1000)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.heartbeat is not None and self.heartbeat.running:
            self.heartbeat.stop()

        self.__deactivate_worker()
        return False

    def __deactivate_worker(self):
        log.log(f"deactivating worker {self.worker.uuid}")
        worker_url = (
            f"{self.api_settings.url}/api/v1/jobs/{self.worker.uuid}/deactivate"
        )
        response = requests.post(
            worker_url, headers=self.api_settings.headers, timeout=60
        )
        response.raise_for_status()

    def __register_worker(self) -> Worker:
        worker_url = f"{self.api_settings.url}/api/v1/jobs/register"

        response = requests.post(
            url=worker_url,
            headers=self.api_settings.headers,
            json=self.worker_details.model_dump(),
        )
        response.raise_for_status()

        response_json = response.json()

        worker = Worker(**response_json)

        log.log(f"worker registered uuid={worker.uuid}")

        return worker

    @contextmanager
    def get_job(self) -> Generator[Optional[Job], None, None]:
        if self.heartbeat is not None:
            raise ValueError("Tried to get more than one job in one worker")

        # get job from job system
        job_url = (
            f"{self.api_settings.url}/api/v1/jobs/{self.worker.uuid}/poll"
            f"?claimDuration={self.claim_duration}"
        )

        response = requests.post(url=job_url, headers=self.api_settings.headers)
        response.raise_for_status()

        if response.status_code == 204:
            log.log("job poll returned no job")
            yield None
            return

        update_pod_deletion_cost(1000)
        response_json = response.json()
        claim_response = ClaimResponse(**response_json)

        log.log(f"got job with response {claim_response}")

        task_uuid = claim_response.task_uuid
        claim_uuid = claim_response.claim_uuid

        job = Job.from_claim(claim_response)

        self.heartbeat = JobsSystemHeartbeat(
            api_settings=self.api_settings,
            worker_uuid=self.worker.uuid,
            task_uuid=task_uuid,
            claim_uuid=claim_uuid,
            interval=self.heartbeat_interval,
            extension_duration=self.claim_duration,
            shutdown_handler=self.shutdown_handler,
        )
        self.heartbeat.start()

        error = None

        try:
            yield job
        except Exception as e:
            if e.__traceback__:
                filename = e.__traceback__.tb_frame.f_code.co_filename
                lineno = e.__traceback__.tb_lineno
                error = f"{filename} line {lineno}: {repr(e)}"
            else:
                error = repr(e)
        finally:
            if self.shutdown_handler.interrupted:
                if self.shutdown_handler.reason != Reason.JOB_CANCELLED:
                    self.__job_failed(
                        task_uuid, claim_uuid, error=self.shutdown_handler.message
                    )
                else:
                    pass  # don't need to notify jobs system (in fact, must not)
            elif error is not None:
                self.__job_failed(task_uuid, claim_uuid, error=error)
            else:
                self.__job_complete(task_uuid, claim_uuid)

            self.heartbeat.stop()
            update_pod_deletion_cost(-1000)
            try:
                self.heartbeat.wait_to_finish()
            except Exception as e:
                log.error("heartbeat thread did not stop")
                log.error(exception=e)
            self.heartbeat = None

    def __job_complete(self, task_uuid: UUID, claim_uuid: UUID):
        complete_url = (
            f"{self.api_settings.url}/api/v1/jobs/{self.worker.uuid}"
            f"/complete/{task_uuid}?claimUuid={claim_uuid}"
        )
        response = requests.post(complete_url, headers=self.api_settings.headers)
        response.raise_for_status()

    def __job_failed(self, task_uuid: UUID, claim_uuid: UUID, error=""):
        log.error(f"ERROR - returning task - {error}")
        return_url = (
            f"{self.api_settings.url}/api/v1/jobs/{self.worker.uuid}"
            f"/return/{task_uuid}?claimUuid={claim_uuid}"
        )
        response = requests.post(return_url, headers=self.api_settings.headers)
        response.raise_for_status()
