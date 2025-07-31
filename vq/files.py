import concurrent.futures
import shutil
from pathlib import Path
from typing import List, Optional
from uuid import UUID

import requests

import utils.logging as log
import vq.api
from utils import GracefulShutdownHandler


class DownloadManager:
    def __init__(self, total: Optional[int] = None):
        self.futures_list = []
        self.pbar = log.progress_bar(desc="Downloading files", total=total)
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def download(self, url: str, destination: Path):
        self.futures_list.append(
            self.executor.submit(
                DownloadManager.__download_url_to_file, self, url, destination
            )
        )
        self.pbar.refresh()

    def __download_url_to_file(self, url: str, destination: Path):
        response = requests.get(url, stream=True)
        with open(destination, "wb") as out_file:
            shutil.copyfileobj(response.raw, out_file)
        self.pbar.update()

    def close(self):
        concurrent.futures.wait(self.futures_list)
        self.executor.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        # False means if an exception occurred do not suppress
        return False


class VQFilesManager:
    def __init__(self, vq_url: str, token: str):
        self.vq_url = vq_url
        headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": vq.api.get_user_agent_string(),
        }
        self.headers = headers

    def download_files(
        self,
        files: List[UUID],
        download_path: Path,
        shutdown_handler: GracefulShutdownHandler,
        organisation_uuid: UUID,
    ):
        downloaded_files: List[Path] = []
        with DownloadManager(total=len(files)) as download_manager:
            for file_uuid in files:
                if shutdown_handler.interrupted:
                    return

                response = requests.get(
                    url=f"{self.vq_url}/api/v1/fileReferences/{file_uuid}",
                    params={"organisation": organisation_uuid},
                    headers=self.headers,
                )

                if response.status_code == 404:
                    raise FileNotFoundError(
                        f"The file uuid {file_uuid} cannot be found"
                    )
                response.raise_for_status()
                file = response.json()

                file_info = file["file"]
                file_url = (
                    file_info["baseUrl"]
                    + "/"
                    + file_info["folder"]
                    + "/"
                    + file_info["fileHash"]
                    + "."
                    + file_info["extension"]
                )
                destination: Path = download_path / file["name"]
                download_manager.download(file_url, destination)

                downloaded_files.append(destination)

        return downloaded_files

    def upload_files(
        self, folder_uuid: UUID, files: List[Path], organisation_uuid: UUID
    ):
        upload_url = (
            f"{self.vq_url}/api/v1/fileReferences/file"
            "?overwrite=overwrite"
            "&runTriggers=true"
            f"&organisation={organisation_uuid}"
        )

        for file_path in log.progress_bar(files, desc="Uploading"):
            with open(file_path, "rb") as file_data:
                files_body = {
                    "files_in": (file_path.name, file_data),
                }
                data_body = {
                    "folder_uuid": folder_uuid,
                }

                log.log(f"Uploading {file_path.name}")

                response = requests.post(
                    url=upload_url,
                    headers=self.headers,
                    files=files_body,
                    data=data_body,
                )
                response.raise_for_status()
